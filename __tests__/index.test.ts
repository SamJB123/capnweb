// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { expect, it, describe, inject } from "vitest"
import { cborCodec, CborCodec, RpcSession, type RpcSessionOptions, RpcTransport, RpcTarget,
         RpcStub, newWebSocketRpcSession, newMessagePortRpcSession,
         newHttpBatchRpcSession} from "../src/index.js"
import { Devaluator, Evaluator } from "../src/serialize.js"
import { RpcPayload, PayloadStubHook } from "../src/core.js"
import { Counter, TestTarget } from "./test-util.js"
import { Encoder, Decoder, FLOAT32_OPTIONS } from "cbor-x"

// Test cases for CBOR serialization roundtrip
let SERIALIZE_TEST_CASES: unknown[] = [
  123,
  null,
  "foo",
  true,

  {foo: 123},
  {foo: {bar: 123, baz: 456}, qux: 789},

  [123],
  [[123, 456]],
  {foo: [123]},
  {foo: [123], bar: [456, 789]},

  123n,
  new Date(1234),
  new TextEncoder().encode("hello!"),
  undefined,
  new Error("the message"),
  new TypeError("the message"),
  new RangeError("the message"),

  Infinity,
  -Infinity,
  NaN,
];

class NotSerializable {
  i: number;
  constructor(i: number) {
    this.i = i;
  }
  toString() {
    return `NotSerializable(${this.i})`;
  }
}

// Helper to serialize using Devaluator -> CBOR
function serialize(value: unknown): Uint8Array {
  return cborCodec.encode(Devaluator.devaluate(value));
}

// Helper to deserialize using CBOR -> Evaluator
class NullImporter {
  importStub(): never { throw new Error("Not supported"); }
  importPromise(): never { throw new Error("Not supported"); }
  getExport(): undefined { return undefined; }
}
function deserialize(data: Uint8Array): unknown {
  const payload = new Evaluator(new NullImporter()).evaluate(cborCodec.decode(data));
  payload.dispose();
  return payload.value;
}

describe("simple serialization", () => {
  it("can roundtrip values through CBOR", () => {
    for (let value of SERIALIZE_TEST_CASES) {
      const encoded = serialize(value);
      const decoded = deserialize(encoded);
      // Handle NaN specially since NaN !== NaN
      if (typeof value === 'number' && isNaN(value)) {
        expect(isNaN(decoded as number)).toBe(true);
      } else {
        expect(decoded).toStrictEqual(value);
      }
    }
  })

  it("throws an error if the value can't be serialized", () => {
    expect(() => serialize(new NotSerializable(123))).toThrowError(
      new TypeError("Cannot serialize value: NotSerializable(123)")
    );

    expect(() => serialize(Object.create(null))).toThrowError(
      new TypeError("Cannot serialize value: (couldn't stringify value)")
    );
  })

  it("throws an error for circular references", () => {
    let obj: any = {};
    obj.self = obj;
    expect(() => serialize(obj)).toThrowError(
      "Serialization exceeded maximum allowed depth. (Does the message contain cycles?)"
    );
  })

  it("can serialize complex nested structures", () => {
    let complex = {
      level1: {
        level2: {
          level3: {
            array: [1, 2, { nested: "deep" }],
            date: new Date(5678),
            nullVal: null,
            undefinedVal: undefined
          }
        }
      },
      top_array: [[1, 2], [3, 4]]
    };
    let encoded = serialize(complex);
    expect(deserialize(encoded)).toStrictEqual(complex);
  })

  it("throws errors for malformed deserialization data", () => {
    // CBOR decoder throws for invalid data
    expect(() => cborCodec.decode(new Uint8Array([0xFF, 0xFF]))).toThrowError();
    // Unknown Cap'n Web type code
    expect(() => deserialize(cborCodec.encode(["unknown_type", "param"]))).toThrowError();
  })
});

// =======================================================================================

class TestTransport implements RpcTransport {
  constructor(public name: string, private partner?: TestTransport) {
    if (partner) {
      partner.partner = this;
    }
  }

  private queue: Uint8Array[] = [];
  private waiter?: () => void;
  private aborter?: (err: any) => void;
  public log = false;

  async send(message: Uint8Array): Promise<void> {
    if (this.log) console.log(`${this.name}: ${JSON.stringify(cborCodec.decode(message))}`);
    this.partner!.queue.push(message);
    if (this.partner!.waiter) {
      this.partner!.waiter();
      this.partner!.waiter = undefined;
      this.partner!.aborter = undefined;
    }
  }

  async receive(): Promise<Uint8Array> {
    if (this.queue.length == 0) {
      await new Promise<void>((resolve, reject) => {
        this.waiter = resolve;
        this.aborter = reject;
      });
    }

    return this.queue.shift()!;
  }

  forceReceiveError(error: any) {
    this.aborter!(error);
  }
}

// Spin the microtask queue a bit to give messages time to be delivered and handled.
async function pumpMicrotasks() {
  for (let i = 0; i < 16; i++) {
    await Promise.resolve();
  }
}

class TestHarness<T extends RpcTarget> {
  clientTransport: TestTransport;
  serverTransport: TestTransport;
  client: RpcSession<T>;
  server: RpcSession;

  stub: RpcStub<T>;

  constructor(target: T, serverOptions?: RpcSessionOptions) {
    this.clientTransport = new TestTransport("client");
    this.serverTransport = new TestTransport("server", this.clientTransport);

    this.client = new RpcSession<T>(this.clientTransport);

    // TODO: If I remove `<undefined>` here, I get a TypeScript error about the instantiation being
    //   excessively deep and possibly infinite. Why? `<undefined>` is supposed to be the default.
    this.server = new RpcSession<undefined>(this.serverTransport, target, serverOptions);

    this.stub = this.client.getRemoteMain();
  }

  // Enable logging of all messages sent. Useful for debugging.
  enableLogging() {
    this.clientTransport.log = true;
    this.serverTransport.log = true;
  }

  checkAllDisposed() {
    expect(this.client.getStats(), "client").toStrictEqual({imports: 1, exports: 1});
    expect(this.server.getStats(), "server").toStrictEqual({imports: 1, exports: 1});
  }

  async [Symbol.asyncDispose]() {
    try {
      // HACK: Spin the microtask loop for a bit to make sure dispose messages have been sent
      //   and received.
      await pumpMicrotasks();

      // Check at the end of every test that everything was disposed.
      this.checkAllDisposed();
    } catch (err) {
      // Don't throw from disposer as it may suppress the real error that caused the disposal in
      // the first place.

      // I couldn't find a better way to make vitest log a failure without throwing...
      let message: string;
      if (err instanceof Error) {
        message = err.stack || err.message;
      } else {
        message = `${err}`;
      }
      expect.soft(true, message).toBe(false);
    }
  }
}

describe("local stub", () => {
  it("supports wrapping an RpcTarget", async () => {
    let stub = new RpcStub(new TestTarget());
    expect(await stub.square(3)).toBe(9);
  });

  it("supports wrapping a function", async () => {
    // TODO: If we don't explicitly declare the type of `i` then the type system complains about
    //   too-deep recursion here. Why?
    let stub = new RpcStub((i :number) => i + 5);
    expect(await stub(3)).toBe(8);
  });

  it("supports wrapping an async function", async () => {
    let stub = new RpcStub(async (i :number) => { return i + 5; });
    expect(await stub(3)).toBe(8);
  });

  it("supports wrapping an arbitrary object", async () => {
    let stub = new RpcStub({abc: "hello"});
    expect(await stub.abc).toBe("hello");
  });

  it("supports wrapping an object with nested stubs", async () => {
    let innerTarget = new TestTarget();
    let innerStub = new RpcStub(innerTarget);
    let outerObject = { inner: innerStub, value: 42 };
    let outerStub = new RpcStub(outerObject);

    expect(await outerStub.value).toBe(42);
    expect(await outerStub.inner.square(4)).toBe(16);
  });

  it("supports wrapping an object with nested RpcTargets", async () => {
    let innerTarget = new TestTarget();
    let outerObject = { inner: innerTarget, value: 42 };
    let outerStub = new RpcStub(outerObject);

    expect(await outerStub.value).toBe(42);
    expect(await outerStub.inner.square(4)).toBe(16);
  });

  it("supports wrapping an object with nested functions", async () => {
    let outerObject = { square: (x: number) => x * x, value: 42 };
    let outerStub = new RpcStub(outerObject);

    expect(await outerStub.value).toBe(42);
    expect(await outerStub.square(4)).toBe(16);
  });

  it("supports wrapping an object with nested async functions", async () => {
    async function asyncSqare(x: number) {
      await Promise.resolve();
      return x * x;
    }

    let outerObject = { square: asyncSqare, value: 42 };
    let outerStub = new RpcStub(outerObject);

    expect(await outerStub.value).toBe(42);
    expect(await outerStub.square(4)).toBe(16);
  });

  it("supports wrapping an RpcTarget with nested stubs", async () => {
    class TargetWithStubs extends RpcTarget {
      getValue() { return 42; }

      get innerStub() {
        return new RpcStub(new TestTarget());
      }
    }

    let outerStub = new RpcStub(new TargetWithStubs());
    expect(await outerStub.getValue()).toBe(42);
    expect(await outerStub.innerStub.square(3)).toBe(9);
  });

  it("supports wrapping an RpcTarget with nested RpcTargets", async () => {
    class TargetWithTargets extends RpcTarget {
      getValue() { return 42; }

      get innerTarget() {
        return new TestTarget();
      }
    }

    let outerStub = new RpcStub(new TargetWithTargets());
    expect(await outerStub.getValue()).toBe(42);
    expect(await outerStub.innerTarget.square(3)).toBe(9);
  });

  it("returns undefined when accessing nonexistent properties", async () => {
    let objectStub = new RpcStub({foo: "bar"});
    let arrayStub = new RpcStub([1, 2, 3]);
    let targetStub = new RpcStub(new TestTarget());

    expect(await (objectStub as any).nonexistent).toBe(undefined);
    expect(await (arrayStub as any).nonexistent).toBe(undefined);
    expect(await (targetStub as any).nonexistent).toBe(undefined);

    // Accessing a property of undefined should throw TypeError (but the error message differs
    // across runtimes).
    await expect(() => (objectStub as any).nonexistent.foo).rejects.toThrow(TypeError);
    await expect(() => (arrayStub as any).nonexistent.foo).rejects.toThrow(TypeError);
    await expect(() => (targetStub as any).nonexistent.foo).rejects.toThrow(TypeError);
  });

  it("exposes only prototype properties for RpcTarget, not instance properties", async () => {
    class TargetWithProps extends RpcTarget {
      instanceProp = "instance";
      dynamicProp: string;

      constructor() {
        super();
        this.dynamicProp = "dynamic";
      }

      get prototypeProp() { return "prototype"; }
      prototypeMethod() { return "method"; }
    }

    let target = new TargetWithProps();
    let stub = new RpcStub(target);

    expect(await stub.prototypeProp).toBe("prototype");
    expect(await stub.prototypeMethod()).toBe("method");
    await expect(() => (stub as any).instanceProp).rejects.toThrow(new TypeError(
        "Attempted to access property 'instanceProp', which is an instance property of the " +
        "RpcTarget. To avoid leaking private internals, instance properties cannot be accessed " +
        "over RPC. If you want to make this property available over RPC, define it as a method " +
        "or getter on the class, instead of an instance property."));
    await expect(() => (stub as any).dynamicProp).rejects.toThrow(new TypeError(
        "Attempted to access property 'dynamicProp', which is an instance property of the " +
        "RpcTarget. To avoid leaking private internals, instance properties cannot be accessed " +
        "over RPC. If you want to make this property available over RPC, define it as a method " +
        "or getter on the class, instead of an instance property."));
  });

  it("does not expose private methods starting with #", async () => {
    class TargetWithPrivate extends RpcTarget {
      #privateMethod() { return "private"; }
      publicMethod() { return "public"; }
    }

    let stub = new RpcStub(new TargetWithPrivate());
    expect(await stub.publicMethod()).toBe("public");
    expect(await (stub as any)["#privateMethod"]).toBe(undefined);
  });

  it("supports map() on nulls", async () => {
    let counter = new RpcStub(new Counter(0));

    let stub = new RpcStub(new TestTarget());

    {
      using promise = stub.returnNull();
      expect(await promise.map(_ => counter.increment(123))).toBe(null);
    }

    {
      using promise = stub.returnUndefined();
      expect(await promise.map(_ => counter.increment(456))).toBe(undefined);
    }

    {
      using promise = stub.returnNumber(2);
      expect(await promise.map(i => counter.increment(i))).toBe(2);
    }

    {
      using promise = stub.returnNumber(4);
      expect(await promise.map(i => counter.increment(i))).toBe(6);
    }
  });

  it("supports map() on arrays", async () => {
    let outerCounter = new RpcStub(new Counter(0));
    let stub = new RpcStub(new TestTarget());

    using fib = stub.generateFibonacci(6);
    using counters = await fib.map(i => {
      let counter = stub.makeCounter(i);
      let val = counter.increment(3);
      outerCounter.increment();
      return {counter, val};
    });

    expect(counters.map(x => x.val)).toStrictEqual([3, 4, 4, 5, 6, 8]);

    expect(await Promise.all(counters.map(x => x.counter.value)))
        .toStrictEqual([3, 4, 4, 5, 6, 8]);

    expect(await outerCounter.value).toBe(6);
  });

  it("supports nested map()", async () => {
    let stub = new RpcStub(new TestTarget());

    let fib = stub.generateFibonacci(7);
    let result = await fib.map(i => {
      return stub.generateFibonacci(i).map(j => {
        return stub.generateFibonacci(j);
      });
    });

    expect(result).toStrictEqual([
      [],
      [[]],
      [[]],
      [[], [0]],
      [[], [0], [0]],
      [[], [0], [0], [0, 1], [0, 1, 1]],
      [[], [0], [0], [0, 1], [0, 1, 1], [0, 1, 1, 2, 3], [0, 1, 1, 2, 3, 5, 8, 13],
          [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144]],
    ]);
  });

  it("overrides toString() to at least specify the type", async () => {
    let stub = new RpcStub(new TestTarget());
    expect(stub.toString()).toBe("[object RpcStub]");
    let promise = stub.square(3);
    expect(promise.toString()).toBe("[object RpcPromise]");
  });
});

describe("stub disposal", () => {
  it("disposes nested stubs and RpcTargets when wrapping an object", () => {
    class DisposableTarget extends RpcTarget {
      constructor(private disposeFlag: { value: boolean }) {
        super();
      }
      [Symbol.dispose]() { this.disposeFlag.value = true; }
    }

    let innerFlag = { value: false };
    let anotherFlag = { value: false };
    let innerStub = new RpcStub(new DisposableTarget(innerFlag));

    let outerObject = {
      stub: innerStub,
      target: new DisposableTarget(anotherFlag),
      value: 42
    };
    let outerStub = new RpcStub(outerObject);

    outerStub[Symbol.dispose]();

    expect(innerFlag.value).toBe(true);
    expect(anotherFlag.value).toBe(true);
  });

  it("only calls RpcTarget disposer when wrapping an RpcTarget with nested stubs", () => {
    let targetDisposed = false;
    let innerTargetDisposed = false;

    class InnerTarget extends RpcTarget {
      [Symbol.dispose]() { innerTargetDisposed = true; }
    }

    class TargetWithStubs extends RpcTarget {
      inner = new RpcStub(new InnerTarget());

      get innerStub() {
        return this.inner;
      }

      [Symbol.dispose]() { targetDisposed = true; }
    }

    let outerStub = new RpcStub(new TargetWithStubs());
    outerStub[Symbol.dispose]();

    expect(targetDisposed).toBe(true);
    expect(innerTargetDisposed).toBe(false); // nested stubs in RpcTarget are not auto-disposed
  });

  it("only disposes RpcTarget when all dups are disposed", () => {
    let disposed = false;
    class DisposableTarget extends RpcTarget {
      [Symbol.dispose]() { disposed = true; }
    }

    let original = new RpcStub(new DisposableTarget());
    let dup1 = original.dup();
    let dup2 = original.dup();

    original[Symbol.dispose]();
    expect(disposed).toBe(false);

    dup1[Symbol.dispose]();
    expect(disposed).toBe(false);

    dup2[Symbol.dispose]();
    expect(disposed).toBe(true);
  });

  it("makes disposal idempotent - duplicate dispose calls don't affect refcount", () => {
    let disposed = false;
    class DisposableTarget extends RpcTarget {
      [Symbol.dispose]() { disposed = true; }
    }

    let original = new RpcStub(new DisposableTarget());
    let dup1 = original.dup();

    // Dispose the duplicate twice
    dup1[Symbol.dispose]();
    dup1[Symbol.dispose]();
    expect(disposed).toBe(false);

    // Only when original is also disposed should the target be disposed
    original[Symbol.dispose]();
    expect(disposed).toBe(true);
  });
});

describe("basic rpc", () => {
  it("supports calls", async () => {
    await using harness = new TestHarness(new TestTarget());
    expect(await harness.stub.square(3)).toBe(9);
  });

  it("supports throwing errors", async () => {
    await using harness = new TestHarness(new TestTarget());
    let stub = harness.stub;
    await expect(() => stub.throwError()).rejects.toThrow(new RangeError("test error"));
  });

  it("supports .then(), .catch(), and .finally() on RPC promises", async () => {
    await using harness = new TestHarness(new TestTarget());
    let stub = harness.stub;

    // Test .then() with successful call
    {
      let result = await stub.square(3).then(value => {
        expect(value).toBe(9);
        return value * 2;
      });
      expect(result).toBe(18);
    }

    // Test .catch() with error
    {
      let result = await stub.throwError()
        .catch(err => {
          expect(err).toBeInstanceOf(RangeError);
          expect((err as Error).message).toBe("test error");
          return "caught";
        });
      expect(result).toBe("caught");
    }

    // Test .finally() with successful call
    {
      let finallyCalled = false;
      await stub.square(4)
        .finally(() => {
          finallyCalled = true;
        });
      expect(finallyCalled).toBe(true);
    }

    // Test .finally() with an error
    {
      let finallyCalled = false;
      let promise = stub.throwError()
        .finally(() => {
          finallyCalled = true;
        });
      await expect(() => promise).rejects.toThrow(new RangeError("test error"));
      expect(finallyCalled).toBe(true);
    }
  });

  it("throws error when trying to send non-serializable argument", async () => {
    await using harness = new TestHarness(new TestTarget());
    let stub = harness.stub;

    expect(() => stub.square(new NotSerializable(123) as any)).toThrow(
      new TypeError("Cannot serialize value: NotSerializable(123)")
    );
  });

  it("throws error when trying to return non-serializable result", async () => {
    class BadTarget extends RpcTarget {
      returnNonSerializable() {
        return new NotSerializable(456);
      }
    }

    await using harness = new TestHarness(new BadTarget());
    let stub = harness.stub as any;

    await expect(() => stub.returnNonSerializable()).rejects.toThrow(
      new TypeError("Cannot serialize value: NotSerializable(456)")
    );
  });

  /**
   * Verifies that Object.prototype properties (toString, hasOwnProperty, etc.) and other
   * dangerous properties (__proto__, constructor) cannot be accessed on RpcTarget objects
   * through the RPC property access mechanism. This prevents prototype pollution and
   * information disclosure vulnerabilities.
   */
  it("does not expose common Object properties on RpcTarget", async () => {
    const target = new TestTarget();
    const payload = RpcPayload.fromAppReturn(target);
    const hook = new PayloadStubHook(payload);

    const dangerousProperties = [
      "toString",
      "hasOwnProperty",
      "valueOf",
      "toLocaleString",
      "isPrototypeOf",
      "propertyIsEnumerable",
      "__proto__",
      "constructor",
    ];

    for (const prop of dangerousProperties) {
      const resultHook = hook.get([prop]);
      const resultPayload = await resultHook.pull();
      expect(resultPayload.value, `property "${prop}" should not be exposed`).toBe(undefined);
      resultPayload.dispose();
      resultHook.dispose();
    }

    hook.dispose();
  });

  /**
   * Verifies that Object.prototype properties, Array.prototype properties, and Function.prototype
   * properties are not exposed on plain objects passed through RPC. Also verifies that dangerous
   * properties in incoming data are stripped during deserialization to prevent prototype pollution.
   */
  it("does not expose common Object properties on plain objects", async () => {
    const obj = {
      foo: 123,
      arr: [1, 2, 3],
      func(x: any) { return `${x}`; },
    };
    const payload = RpcPayload.fromAppReturn(obj);
    const hook = new PayloadStubHook(payload);

    // Regular properties should work
    let resultHook = hook.get(["foo"]);
    let resultPayload = await resultHook.pull();
    expect(resultPayload.value).toBe(123);
    resultPayload.dispose();
    resultHook.dispose();

    // Object.prototype properties should not be exposed
    for (const prop of ["toString", "hasOwnProperty", "__proto__", "constructor"]) {
      resultHook = hook.get([prop]);
      resultPayload = await resultHook.pull();
      expect(resultPayload.value, `property "${prop}" should not be exposed`).toBe(undefined);
      resultPayload.dispose();
      resultHook.dispose();
    }

    // Array.prototype properties should not be exposed on nested arrays
    resultHook = hook.get(["arr", "map"]);
    resultPayload = await resultHook.pull();
    expect(resultPayload.value, `Array.prototype.map should not be exposed`).toBe(undefined);
    resultPayload.dispose();
    resultHook.dispose();

    // Function.prototype properties should not be exposed on nested functions
    resultHook = hook.get(["func", "call"]);
    resultPayload = await resultHook.pull();
    expect(resultPayload.value, `Function.prototype.call should not be exposed`).toBe(undefined);
    resultPayload.dispose();
    resultHook.dispose();

    hook.dispose();
  });

  /**
   * Verifies that dangerous properties are stripped from incoming deserialized data during
   * the evaluation phase. This prevents prototype pollution attacks where a malicious peer
   * sends objects with __proto__, constructor, toString, or toJSON properties.
   */
  it("strips dangerous properties from deserialized objects", () => {
    // Simulate receiving data from a malicious peer with dangerous properties
    const maliciousData = {
      safe: "value",
      nested: { also: "safe" },
      toString: "malicious",
      hasOwnProperty: "malicious",
      __proto__: { bad: true },
      constructor: "malicious",
      toJSON: "malicious",
    };

    // NullImporter since we're not testing stub import functionality
    class NullImporter {
      importStub() { throw new Error("not implemented"); }
      importPromise() { throw new Error("not implemented"); }
      getExport() { return undefined; }
    }

    const evaluator = new Evaluator(new NullImporter());
    const payload = evaluator.evaluate(maliciousData);
    const result = payload.value as Record<string, unknown>;

    // Safe properties should be preserved
    expect(result.safe).toBe("value");
    expect((result.nested as any).also).toBe("safe");

    // Dangerous properties should be stripped (using hasOwn to check own properties, not prototype)
    expect(Object.hasOwn(result, "toString")).toBe(false);
    expect(Object.hasOwn(result, "hasOwnProperty")).toBe(false);
    expect(Object.hasOwn(result, "__proto__")).toBe(false);
    expect(Object.hasOwn(result, "constructor")).toBe(false);
    expect(Object.hasOwn(result, "toJSON")).toBe(false);

    payload.dispose();
  });

  it("supports passing async functinos", async () => {
    await using harness = new TestHarness(new TestTarget());

    async function square(i: number) {
      await Promise.resolve();
      return i * i;
    }

    expect(await harness.stub.callFunction(square, 3)).toStrictEqual({result: 9});
  });
});

describe("capability-passing", () => {
  it("supports returning an RpcTarget", async () => {
    await using harness = new TestHarness(new TestTarget());
    let stub = harness.stub;
    using counter = await stub.makeCounter(4);
    expect(await counter.increment()).toBe(5);
    expect(await counter.increment(4)).toBe(9);
  });

  it("supports passing a stub back over the connection", async () => {
    await using harness = new TestHarness(new TestTarget());
    let stub = harness.stub;

    using counter = await stub.makeCounter(4);
    expect(await stub.incrementCounter(counter)).toBe(5);
    expect(await stub.incrementCounter(counter, 4)).toBe(9);
  });

  it("supports three-party capability passing", async () => {
    // Create two parallel connections: Alice and Bob
    class AliceTarget extends RpcTarget {
      getCounter() {
        return new Counter(10);
      }
    }

    class BobTarget extends RpcTarget {
      // Bob actually uses the counter, causing calls to proxy through Bob to Alice
      incrementCounter(counter: RpcStub<Counter>, amount: number) {
        return counter.increment(amount);
      }
    }

    await using aliceHarness = new TestHarness(new AliceTarget());
    await using bobHarness = new TestHarness(new BobTarget());

    let aliceStub = aliceHarness.stub;
    let bobStub = bobHarness.stub;

    // Get counter from Alice.
    using counter = await aliceStub.getCounter();

    // Bob increments the counter - this call proxies from Bob through the client to Alice
    let result = await bobStub.incrementCounter(counter, 3);
    expect(result).toBe(13);
  });

  it("supports proxying", async () => {
    // Create two connections in series: us -> Bob -> Alice
    class AliceTarget extends RpcTarget {
      getCounter(i: number) {
        return new Counter(i);
      }

      incrementCounter(counter: RpcStub<Counter>, amount: number) {
        return counter.increment(amount);
      }
    }

    class BobTarget extends RpcTarget {
      constructor(private alice: RpcStub<AliceTarget>) {
        super();
      }

      async getCounter(i: number) {
        return await this.alice.getCounter(i);
      }

      getCounterPromise(i: number) {
        return this.alice.getCounter(i);
      }

      incrementCounter(counter: RpcStub<Counter>, amount: number) {
        return this.alice.incrementCounter(counter, amount);
      }
    }

    await using aliceHarness = new TestHarness(new AliceTarget());
    await using bobHarness = new TestHarness(new BobTarget(aliceHarness.stub));

    let bobStub = bobHarness.stub;

    // Return capability through proxy.
    {
      using result = await bobStub.getCounter(4);
      expect(await result.increment(2)).toBe(6)
    }

    // Return capability through proxy, pipeline.
    {
      using result = bobStub.getCounter(4);
      expect(await result.increment(2)).toBe(6)
    }

    // Return promise through proxy.
    {
      using result = bobStub.getCounterPromise(4);
      expect(await result.increment(2)).toBe(6)
    }

    // Send capability through proxy.
    {
      let counter = new Counter(10);

      let result = await bobStub.incrementCounter(counter, 3);

      expect(result).toBe(13);
      expect(counter.increment(1)).toBe(14);
    }
  });
});

describe("promise pipelining", () => {
  it("supports passing a promise in arguments", async () => {
    await using harness = new TestHarness(new TestTarget());
    let stub = harness.stub;
    using promise = stub.square(2);
    expect(await stub.square(promise)).toBe(16);
  });

  it("supports calling a promise", async () => {
    await using harness = new TestHarness(new TestTarget());
    let stub = harness.stub;
    using counter = stub.makeCounter(4);
    let promise1 = counter.increment();
    let promise2 = counter.increment(4);
    expect(await promise1).toBe(5);
    expect(await promise2).toBe(9);
  });

  it("supports returning a promise", async () => {
    await using harness = new TestHarness(new TestTarget());
    let stub = harness.stub;
    expect(await stub.callSquare(stub, 3)).toStrictEqual({result: 9});
  });

  it("propagates errors to pipelined calls", async () => {
    class ErrorTarget extends RpcTarget {
      throwError(): TestTarget {
        throw new Error("pipelined error");
      }
    }

    await using harness = new TestHarness(new ErrorTarget());
    let stub = harness.stub;

    // Pipeline a call on a promise that will reject
    using errorPromise = stub.throwError();
    using pipelinedCall = errorPromise.square(5);

    await expect(() => pipelinedCall).rejects.toThrow("pipelined error");
  });

  it("propagates errors to argument-pipelined calls", async () => {
    class ErrorTarget extends RpcTarget {
      throwError(): never {
        throw new Error("pipelined error");
      }

      processValue(value: any) {
        return value * 2;
      }
    }

    await using harness = new TestHarness(new ErrorTarget());
    let stub = harness.stub;

    // Pipeline a call on a promise that will reject
    using errorPromise = stub.throwError();
    using pipelinedCall = stub.processValue(errorPromise);

    await expect(() => pipelinedCall).rejects.toThrow("pipelined error");
  });

  it("doesn't create spurious unhandled rejections", async () => {
    class ErrorTarget extends RpcTarget {
      throwError(): never {
        throw new Error("test error");
      }

      processValue(value: any) {
        return value * 2;
      }
    }

    await using harness = new TestHarness(new ErrorTarget());
    let stub = harness.stub;

    let promise = stub.throwError();
    let promise2 = stub.processValue(promise);

    // Intentionally don't await the promises until the next tick. This means we don't pull them,
    // which means nothing awaits the final result on the server end, which means the errors
    // could be considered "unhandled rejections". We do not want the server end to actually see
    // them as such, though, since it's entirely the client's fault that it hasn't waited on them
    // yet! This tests that the system silences such unhandled rejection notices. Note that
    // vitest automatically treats unhandled rejections as failures.
    await new Promise(resolve => setTimeout(resolve, 0));

    await expect(() => promise).rejects.toThrow("test error");
    await expect(() => promise2).rejects.toThrow("test error");
  });
});

describe("map() over RPC", () => {
  it("supports map() on nulls", async () => {
    let counter = new RpcStub(new Counter(0));

    await using harness = new TestHarness(new TestTarget());
    let stub = harness.stub;

    {
      using promise = stub.returnNull();
      expect(await promise.map(_ => counter.increment(123))).toBe(null);
    }

    {
      using promise = stub.returnUndefined();
      expect(await promise.map(_ => counter.increment(456))).toBe(undefined);
    }

    {
      using promise = stub.returnNumber(2);
      expect(await promise.map(i => counter.increment(i))).toBe(2);
    }

    {
      using promise = stub.returnNumber(4);
      expect(await promise.map(i => counter.increment(i))).toBe(6);
    }
  });

  it("supports map() on arrays", async () => {
    let outerCounter = new RpcStub(new Counter(0));

    await using harness = new TestHarness(new TestTarget());
    let stub = harness.stub;

    using fib = stub.generateFibonacci(6);
    using counters = await fib.map(i => {
      let counter = stub.makeCounter(i);
      let val = counter.increment(3);
      outerCounter.increment();
      return {counter, val};
    });

    expect(counters.map(x => x.val)).toStrictEqual([3, 4, 4, 5, 6, 8]);

    expect(await Promise.all(counters.map(x => x.counter.value)))
        .toStrictEqual([3, 4, 4, 5, 6, 8]);

    expect(await outerCounter.value).toBe(6);
  });

  it("supports nested map()", async () => {
    await using harness = new TestHarness(new TestTarget());
    let stub = harness.stub;

    using fib = stub.generateFibonacci(7);
    using result = await fib.map(i => {
      return stub.generateFibonacci(i).map(j => {
        return stub.generateFibonacci(j);
      });
    });

    expect(result).toStrictEqual([
      [],
      [[]],
      [[]],
      [[], [0]],
      [[], [0], [0]],
      [[], [0], [0], [0, 1], [0, 1, 1]],
      [[], [0], [0], [0, 1], [0, 1, 1], [0, 1, 1, 2, 3], [0, 1, 1, 2, 3, 5, 8, 13],
          [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144]],
    ]);
  });
});

describe("stub disposal over RPC", () => {
  it("disposes remote RpcTarget when stub is disposed", async () => {
    let targetDisposedCount = 0;
    class DisposableTarget extends RpcTarget {
      getValue() { return 42; }
      [Symbol.dispose]() { ++targetDisposedCount; }
    }

    class MainTarget extends RpcTarget {
      getDisposableTarget() {
        return new DisposableTarget();
      }
    }

    await using harness = new TestHarness(new MainTarget());
    let mainStub = harness.stub as any;

    {
      using disposableStub = await mainStub.getDisposableTarget();
      expect(await disposableStub.getValue()).toBe(42);
      expect(targetDisposedCount).toBe(0);
    } // disposer runs here

    // Wait a bit for the disposal message to be processed
    await pumpMicrotasks();

    expect(targetDisposedCount).toBe(1);
  });

  it("disposes a returned RpcTarget for every time it appears in a result", async () => {
    let targetDisposedCount = 0;
    class DisposableTarget extends RpcTarget {
      getValue() { return 42; }
      [Symbol.dispose]() { ++targetDisposedCount; }
    }

    class MainTarget extends RpcTarget {
      getDisposableTarget() {
        let result = new DisposableTarget();
        return [result, result, result];
      }
    }

    await using harness = new TestHarness(new MainTarget());
    let mainStub = harness.stub as any;

    {
      using disposableStub = await mainStub.getDisposableTarget();
      expect(await disposableStub[0].getValue()).toBe(42);
      expect(await disposableStub[1].getValue()).toBe(42);
      expect(await disposableStub[2].getValue()).toBe(42);

      // The current implementation will actually call the disposer twice as soon as the pipeline
      // is done, but the last call won't happen until the stubs are disposed.
      expect(targetDisposedCount).toBeLessThan(3);
    } // final disposer runs here

    // Wait a bit for the disposal message to be processed
    await pumpMicrotasks();

    // Disposer is called three times.
    expect(targetDisposedCount).toBe(3);
  });

  it("disposes RpcTarget that was passed in params", async () => {
    let targetDisposedCount = 0;
    class DisposableTarget extends RpcTarget {
      getValue() { return 42; }
      [Symbol.dispose]() { ++targetDisposedCount; }
    }

    class MainTarget extends RpcTarget {
      useDisposableTarget(stub: RpcStub<DisposableTarget>) {
        return stub.getValue();
      }
    }

    await using harness = new TestHarness(new MainTarget());
    let mainStub = harness.stub as any;

    {
      let result = await mainStub.useDisposableTarget(new DisposableTarget());
      expect(result).toBe(42);
    }

    // Wait a bit for the disposal message to be processed
    await pumpMicrotasks();

    expect(targetDisposedCount).toBe(1);
  });

  it("dupes RpcTarget that was passed in params if it has a dup() method", async () => {
    let dupCount = 0;
    let disposeCount = 0;
    class DisposableTarget extends RpcTarget {
      getValue() { return 42; }

      dup() {
        ++dupCount;
        return new DisposableTarget();
      }

      disposed = false;
      [Symbol.dispose]() {
        if (this.disposed) throw new Error("double disposed");
        this.disposed = true;
        ++disposeCount;
      }
    }

    class MainTarget extends RpcTarget {
      useDisposableTarget(stub: RpcStub<DisposableTarget>) {
        return stub.getValue();
      }
    }

    await using harness = new TestHarness(new MainTarget());
    let mainStub = harness.stub as any;

    let disposableTarget = new DisposableTarget();

    {
      let result = await mainStub.useDisposableTarget(disposableTarget);
      expect(dupCount).toBe(1);
      expect(result).toBe(42);
    }

    {
      let result = await mainStub.useDisposableTarget(disposableTarget);
      expect(dupCount).toBe(2);
      expect(result).toBe(42);
    }

    // Wait a bit for the disposal message to be processed
    await pumpMicrotasks();

    expect(dupCount).toBe(2);
    expect(disposeCount).toBe(2);
    expect(disposableTarget.disposed).toBe(false);
  });

  it("only disposes remote target when all RPC dups are disposed", async () => {
    let targetDisposed = false;
    class DisposableTarget extends RpcTarget {
      getValue() { return 42; }
      [Symbol.dispose]() { targetDisposed = true; }
    }

    class MainTarget extends RpcTarget {
      getDisposableTarget() {
        return new DisposableTarget();
      }
    }

    await using harness = new TestHarness(new MainTarget());
    let mainStub = harness.stub as any;

    let disposableStub = await mainStub.getDisposableTarget();
    let dup1 = disposableStub.dup();
    let dup2 = disposableStub.dup();

    disposableStub[Symbol.dispose]();
    await pumpMicrotasks();
    expect(targetDisposed).toBe(false);

    dup1[Symbol.dispose]();
    await pumpMicrotasks();
    expect(targetDisposed).toBe(false);

    dup2[Symbol.dispose]();
    await pumpMicrotasks();
    expect(targetDisposed).toBe(true);
  });

  it("makes RPC disposal idempotent", async () => {
    let targetDisposed = false;
    class DisposableTarget extends RpcTarget {
      getValue() { return 42; }
      [Symbol.dispose]() { targetDisposed = true; }
    }

    class MainTarget extends RpcTarget {
      getDisposableTarget() {
        return new DisposableTarget();
      }
    }

    await using harness = new TestHarness(new MainTarget());
    let mainStub = harness.stub as any;

    let disposableStub = await mainStub.getDisposableTarget();
    let dup1 = disposableStub.dup();

    // Dispose the duplicate twice
    dup1[Symbol.dispose]();
    dup1[Symbol.dispose]();
    await pumpMicrotasks();
    expect(targetDisposed).toBe(false);

    // Only when original is also disposed should the target be disposed
    disposableStub[Symbol.dispose]();
    await pumpMicrotasks();
    expect(targetDisposed).toBe(true);
  });

  it("disposes targets automatically on disconnect", async () => {
    let targetDisposed = false;
    class DisposableTarget extends RpcTarget {
      getValue() { return 42; }
      hangingCall(): Promise<number> {
        // This call will hang and be interrupted by disconnect
        return new Promise(() => {}); // Never resolves
      }
      [Symbol.dispose]() { targetDisposed = true; }
    }

    // Intentionally don't use `using` here because we expect the stats to be wrong after a
    // disconnect.
    let harness = new TestHarness(new DisposableTarget());
    let stub = harness.stub;
    expect(await stub.getValue()).toBe(42);

    // Start a hanging call
    let hangingPromise = stub.hangingCall();

    // Simulate disconnect by making the transport fail
    harness.clientTransport.forceReceiveError(new Error("test error"));

    // The hanging call should be rejected
    await expect(() => hangingPromise).rejects.toThrow(new Error("test error"));

    // Further calls should also fail immediately
    await expect(() => stub.getValue()).rejects.toThrow(new Error("test error"));

    // Targets should be disposed
    expect(targetDisposed).toBe(true);
  });

  it("shuts down the connection if the main capability is disposed", async () => {
    // Intentionally don't use `using` here because we expect the stats to be wrong after a
    // disconnect.
    let harness = new TestHarness(new TestTarget());
    let stub = harness.stub;

    let counter = await stub.makeCounter(0);

    stub[Symbol.dispose]();

    await expect(() => counter.increment(1)).rejects.toThrow(
      new Error("RPC session was shut down by disposing the main stub")
    );
  });
});

describe("e-order", () => {
  it("maintains e-order for concurrent calls on single stub", async () => {
    let callOrder: number[] = [];
    class OrderTarget extends RpcTarget {
      recordCall(id: number) {
        callOrder.push(id);
        return id;
      }
    }

    await using harness = new TestHarness(new OrderTarget());
    let stub = harness.stub as any;

    // Make multiple concurrent calls
    let promises = [
      stub.recordCall(1),
      stub.recordCall(2),
      stub.recordCall(3),
      stub.recordCall(4)
    ];

    await Promise.all(promises);

    // Calls should arrive in the order they were made
    expect(callOrder).toEqual([1, 2, 3, 4]);
  });

  it("maintains e-order for promise-pipelined calls", async () => {
    let callOrder: number[] = [];
    class OrderTarget extends RpcTarget {
      getObject() {
        return {
          method1: (id: number) => { callOrder.push(id); return id; },
          method2: (id: number) => { callOrder.push(id); return id; }
        };
      }
    }

    await using harness = new TestHarness(new OrderTarget());
    let stub = harness.stub as any;

    // Get a promise for an object
    using objectPromise = stub.getObject();

    // Make pipelined calls on different methods of the same promise
    let promises = [
      objectPromise.method1(1),
      objectPromise.method2(2),
      objectPromise.method1(3),
      objectPromise.method2(4)
    ];

    await Promise.all(promises);

    // Calls should arrive in the order they were made, even across different methods
    expect(callOrder).toEqual([1, 2, 3, 4]);
  });
});

describe("error serialization", () => {
  it("hides the stack by default", async () => {
    await using harness = new TestHarness(new TestTarget(), {
      onSendError: (error) => {
        // default behavior
      }
    });
    let stub = harness.stub;

    let result = await stub.throwError()
      .catch(err => {
        expect(err).toBeInstanceOf(RangeError);
        expect((err as Error).message).toBe("test error");

        // By default, the stack isn't sent. A stack may be added client-side, though. So we
        // verify that it doesn't contain the function name `throwErrorImpl` nor the file name
        // `test-util.ts`, which should only appear on the server.
        expect((err as Error).stack).not.toContain("throwErrorImpl");
        expect((err as Error).stack).not.toContain("test-util.ts");

        return "caught";
      });
    expect(result).toBe("caught");
  });

  it("reveals the stack if the callback returns the error", async () => {
    await using harness = new TestHarness(new TestTarget(), {
      onSendError: (error) => {
        return error;
      }
    });
    let stub = harness.stub;

    let result = await stub.throwError()
      .catch(err => {
        expect(err).toBeInstanceOf(RangeError);
        expect((err as Error).message).toBe("test error");

        // Now the error function and source file should be in the stack.
        expect((err as Error).stack).toContain("throwErrorImpl");
        expect((err as Error).stack).toContain("test-util.ts");

        return "caught";
      });
    expect(result).toBe("caught");
  });

  it("allows errors to be rewritten", async () => {
    await using harness = new TestHarness(new TestTarget(), {
      onSendError: (error) => {
        let rewritten = new TypeError("rewritten error");
        rewritten.stack = "test stack";
        return rewritten;
      }
    });
    let stub = harness.stub;

    let result = await stub.throwError()
      .catch(err => {
        expect(err).toBeInstanceOf(TypeError);
        expect((err as Error).message).toBe("rewritten error");
        expect((err as Error).stack).toBe("test stack");
        return "caught";
      });
    expect(result).toBe("caught");
  });
});

describe("onRpcBroken", () => {
  it("signals when the connection is lost", async () => {
    class TestBroken extends RpcTarget {
      getValue() { return 42; }
      makeCounter() { return new Counter(0); }
      hangingCall(): Promise<Counter> {
        // This call will hang and be interrupted by disconnect
        return new Promise(() => {}); // Never resolves
      }
      throwError(): Promise<Counter> { throw new Error("test error"); }
    }

    // Intentionally don't use `using` here because we expect the stats to be wrong after a
    // disconnect.
    let harness = new TestHarness(new TestBroken());
    let stub = harness.stub;
    expect(await stub.getValue()).toBe(42);

    let errors: {which: string, error: any}[] = [];
    stub.onRpcBroken(error => { errors.push({which: "stub", error}); });

    let counter1Promise = stub.makeCounter();
    counter1Promise.onRpcBroken(error => { errors.push({which: "counter1Promise", error}); });

    let counter2 = await stub.makeCounter();
    counter2.onRpcBroken(error => { errors.push({which: "counter2", error}); });

    let counter1 = await counter1Promise;
    counter1.onRpcBroken(error => { errors.push({which: "counter1", error}); });

    let hangingPromise = stub.hangingCall();
    hangingPromise.onRpcBroken(error => { errors.push({which: "hangingCall", error}); });

    let throwingPromise = stub.throwError();
    throwingPromise.onRpcBroken(error => { errors.push({which: "throwError", error}); });

    // The method that threw should report brokenness immediately.
    await throwingPromise.catch(err => {});
    expect(errors).toStrictEqual([
      {which: "throwError", error: new Error("test error")},
    ]);

    // onRpcBroken() when already broken just reports the error immediately.
    throwingPromise.onRpcBroken(error => { errors.push({which: "throwError2", error}); });
    expect(errors).toStrictEqual([
      {which: "throwError", error: new Error("test error")},
      {which: "throwError2", error: new Error("test error")},
    ]);

    // Simulate disconnect by making the transport fail
    harness.clientTransport.forceReceiveError(new Error("test disconnect"));
    await hangingPromise.catch(err => {});

    // Now all the other errors were reported, in the order in which the callbacks were
    // registered.
    expect(errors).toStrictEqual([
      {which: "throwError", error: new Error("test error")},
      {which: "throwError2", error: new Error("test error")},
      {which: "stub", error: new Error("test disconnect")},
      {which: "counter1Promise", error: new Error("test disconnect")},
      {which: "counter2", error: new Error("test disconnect")},
      {which: "counter1", error: new Error("test disconnect")},
      {which: "hangingCall", error: new Error("test disconnect")},
    ]);
  });
});

// =======================================================================================

describe("HTTP requests", () => {
  it("can perform a batch HTTP request", async () => {
    let cap = newHttpBatchRpcSession<TestTarget>(`http://${inject("testServerHost")}`);

    let promise1 = cap.square(6);

    let counter = cap.makeCounter(2);
    let promise2 = counter.increment(3);
    let promise3 = cap.incrementCounter(counter, 4);

    expect(await Promise.all([promise1, promise2, promise3]))
        .toStrictEqual([36, 5, 9]);
  });
});

describe("WebSockets", () => {
  it("can open a WebSocket connection", async () => {
    let url = `ws://${inject("testServerHost")}`;

    let cap = newWebSocketRpcSession<TestTarget>(url);

    expect(await cap.square(5)).toBe(25);

    {
      let counter = cap.makeCounter(2);
      expect(await counter.increment(3)).toBe(5);
    }

    {
      let counter = new Counter(4);
      expect(await cap.incrementCounter(counter, 9)).toBe(13);
    }
  });
});

// =======================================================================================

describe("CBOR message size comparison", () => {

  // Simulated RPC message structures
  const positionUpdate = ["call", 1, 0, "updatePosition", [{ x: 12.456, y: 0.5, z: -8.234, rotation: 1.57 }]];
  const simpleCall = ["call", 1, 0, "square", [5]];
  const complexReturn = ["return", 5, {
    user: { id: 123, name: "Alice", roles: ["admin", "user"] },
    profile: { bio: "Developer", location: "SF" }
  }];

  it("sequential mode: first message includes structure, subsequent messages reference it", () => {
    const encoder = new Encoder({
      sequential: true,
      useRecords: true,
      useFloat32: FLOAT32_OPTIONS.ALWAYS,
    });

    // Encode position update multiple times
    const sizes: number[] = [];
    for (let i = 0; i < 5; i++) {
      const encoded = encoder.encode(positionUpdate);
      sizes.push(encoded.length);
    }

    console.log("Sequential mode (useRecords: true) - position update sizes:", sizes);

    // First message should be larger (includes structure definition)
    expect(sizes[0]).toBeGreaterThan(sizes[1]);

    // Subsequent messages should all be the same size
    expect(sizes[1]).toBe(sizes[2]);
    expect(sizes[2]).toBe(sizes[3]);
    expect(sizes[3]).toBe(sizes[4]);

    // Subsequent messages should be significantly smaller
    const savings = ((sizes[0] - sizes[1]) / sizes[0] * 100).toFixed(1);
    console.log(`  First message: ${sizes[0]} bytes, subsequent: ${sizes[1]} bytes (${savings}% smaller)`);
  });

  it("useRecords: false - all messages same size (no structure reuse)", () => {
    const encoder = new Encoder({
      useRecords: false,
      useFloat32: FLOAT32_OPTIONS.ALWAYS,
    });

    // Encode position update multiple times
    const sizes: number[] = [];
    for (let i = 0; i < 5; i++) {
      const encoded = encoder.encode(positionUpdate);
      sizes.push(encoded.length);
    }

    console.log("No records mode (useRecords: false) - position update sizes:", sizes);

    // All messages should be the same size (no structure learning)
    expect(sizes[0]).toBe(sizes[1]);
    expect(sizes[1]).toBe(sizes[2]);
  });

  it("compares sequential vs no-records for repeated structures", () => {
    const sequentialEncoder = new Encoder({
      sequential: true,
      useRecords: true,
      useFloat32: FLOAT32_OPTIONS.ALWAYS,
    });

    const noRecordsEncoder = new Encoder({
      useRecords: false,
      useFloat32: FLOAT32_OPTIONS.ALWAYS,
    });

    // Encode 10 position updates
    let sequentialTotal = 0;
    let noRecordsTotal = 0;

    for (let i = 0; i < 10; i++) {
      sequentialTotal += sequentialEncoder.encode(positionUpdate).length;
      noRecordsTotal += noRecordsEncoder.encode(positionUpdate).length;
    }

    console.log("\n10 position updates comparison:");
    console.log(`  Sequential (useRecords: true):  ${sequentialTotal} bytes total`);
    console.log(`  No records (useRecords: false): ${noRecordsTotal} bytes total`);
    console.log(`  Savings with records: ${((noRecordsTotal - sequentialTotal) / noRecordsTotal * 100).toFixed(1)}%`);

    // Sequential mode should use less total bytes for repeated structures
    expect(sequentialTotal).toBeLessThan(noRecordsTotal);
  });

  it("compares JSON vs CBOR for typical RPC messages", () => {
    const encoder = new Encoder({
      sequential: true,
      useRecords: true,
      useFloat32: FLOAT32_OPTIONS.ALWAYS,
    });

    const testCases = [
      { name: "Position update", data: positionUpdate },
      { name: "Simple call", data: simpleCall },
      { name: "Complex return", data: complexReturn },
    ];

    console.log("\nJSON vs CBOR comparison (first encode, includes structure):");

    for (const { name, data } of testCases) {
      const jsonSize = new TextEncoder().encode(JSON.stringify(data)).length;
      const cborSize = encoder.encode(data).length;
      const savings = ((jsonSize - cborSize) / jsonSize * 100).toFixed(1);

      console.log(`  ${name}: JSON ${jsonSize}B vs CBOR ${cborSize}B (${savings}% ${cborSize < jsonSize ? 'smaller' : 'larger'})`);
    }

    // Re-encode the same structures (now they're learned)
    console.log("\nJSON vs CBOR comparison (subsequent encode, structure reference only):");

    for (const { name, data } of testCases) {
      const jsonSize = new TextEncoder().encode(JSON.stringify(data)).length;
      const cborSize = encoder.encode(data).length;
      const savings = ((jsonSize - cborSize) / jsonSize * 100).toFixed(1);

      console.log(`  ${name}: JSON ${jsonSize}B vs CBOR ${cborSize}B (${savings}% ${cborSize < jsonSize ? 'smaller' : 'larger'})`);
    }
  });

  it("high-frequency update simulation (100 position updates)", () => {
    const sequentialEncoder = new Encoder({
      sequential: true,
      useRecords: true,
      useFloat32: FLOAT32_OPTIONS.ALWAYS,
    });

    const noRecordsEncoder = new Encoder({
      useRecords: false,
      useFloat32: FLOAT32_OPTIONS.ALWAYS,
    });

    // Simulate 100 position updates (like 5 seconds at 20Hz)
    const updates = Array.from({ length: 100 }, (_, i) =>
      ["call", 1, 0, "pos", [{ x: Math.random() * 20, y: 0.5, z: Math.random() * 20, r: Math.random() * 6.28 }]]
    );

    let sequentialTotal = 0;
    let noRecordsTotal = 0;
    let jsonTotal = 0;

    for (const update of updates) {
      sequentialTotal += sequentialEncoder.encode(update).length;
      noRecordsTotal += noRecordsEncoder.encode(update).length;
      jsonTotal += new TextEncoder().encode(JSON.stringify(update)).length;
    }

    console.log("\n100 position updates (5 seconds @ 20Hz):");
    console.log(`  JSON:                          ${jsonTotal} bytes (${(jsonTotal / 1024).toFixed(2)} KB)`);
    console.log(`  CBOR (no records):             ${noRecordsTotal} bytes (${(noRecordsTotal / 1024).toFixed(2)} KB)`);
    console.log(`  CBOR (sequential + records):   ${sequentialTotal} bytes (${(sequentialTotal / 1024).toFixed(2)} KB)`);
    console.log(`  Savings vs JSON:               ${((jsonTotal - sequentialTotal) / jsonTotal * 100).toFixed(1)}%`);
    console.log(`  Savings vs no-records:         ${((noRecordsTotal - sequentialTotal) / noRecordsTotal * 100).toFixed(1)}%`);

    // Sequential with records should be smallest
    expect(sequentialTotal).toBeLessThan(noRecordsTotal);
    expect(sequentialTotal).toBeLessThan(jsonTotal);
  });
});

// =======================================================================================

describe("Shared structures mode (sequential: false)", () => {
  it("requires shared structures array between encoder and decoder", () => {
    // This test demonstrates why structures must be shared when sequential: false
    const sharedStructures: object[] = [];

    const encoder = new Encoder({
      sequential: false,
      useRecords: true,
      structures: sharedStructures,
    });

    const decoder = new Decoder({
      sequential: false,
      useRecords: true,
      structures: sharedStructures,  // Same array - this is required!
    });

    const obj = { name: "Alice", age: 30 };
    const encoded = encoder.encode(obj);
    const decoded = decoder.decode(encoded);

    expect(decoded).toStrictEqual(obj);
  });

  it("fails gracefully when structures are not shared", () => {
    // Demonstrates what happens without shared structures
    const encoderStructures: object[] = [];
    const decoderStructures: object[] = [];

    const encoder = new Encoder({
      sequential: false,
      useRecords: true,
      structures: encoderStructures,
    });

    const decoder = new Decoder({
      sequential: false,
      useRecords: true,
      structures: decoderStructures,  // Different array - problematic!
    });

    const obj = { name: "Alice", age: 30 };
    const encoded = encoder.encode(obj);
    const decoded = decoder.decode(encoded);

    // Decoder returns a Tag object when it can't resolve the structure
    expect(decoded).toHaveProperty('tag');
    expect(decoded).toHaveProperty('value');
  });

  it("CborCodec handles shared structures correctly in non-sequential mode", () => {
    const codec = new CborCodec({ sequential: false });

    // Test various data types
    const testCases = [
      { name: "Alice", age: 30 },
      { x: 1.5, y: 2.5, z: 3.5 },
      { nested: { deep: { value: 42 } } },
      [{ id: 1 }, { id: 2 }, { id: 3 }],
    ];

    for (const original of testCases) {
      const encoded = codec.encode(original);
      const decoded = codec.decode(encoded);
      expect(decoded).toStrictEqual(original);
    }
  });

  it("learns structures incrementally and reuses them", () => {
    const structures: object[] = [];

    const encoder = new Encoder({
      sequential: false,
      useRecords: true,
      structures: structures,
    });

    const decoder = new Decoder({
      sequential: false,
      useRecords: true,
      structures: structures,
    });

    // First encode - creates structure
    const obj1 = { x: 1, y: 2 };
    const encoded1 = encoder.encode(obj1);
    expect(structures.length).toBeGreaterThan(0);
    const structuresAfterFirst = structures.length;

    // Second encode with same shape - reuses structure
    const obj2 = { x: 10, y: 20 };
    const encoded2 = encoder.encode(obj2);
    expect(structures.length).toBe(structuresAfterFirst);  // No new structures

    // Third encode with different shape - creates new structure
    const obj3 = { a: 1, b: 2, c: 3 };
    encoder.encode(obj3);
    expect(structures.length).toBeGreaterThan(structuresAfterFirst);

    // All decode correctly
    expect(decoder.decode(encoded1)).toStrictEqual(obj1);
    expect(decoder.decode(encoded2)).toStrictEqual(obj2);
  });

  it("produces smaller messages than sequential mode for independent streams", () => {
    // The key benefit of shared structures mode: structures persist across the
    // lifetime of a connection. Sequential mode is designed for streams where
    // the decoder might start reading mid-stream, so each encode could be
    // independent.
    //
    // This test simulates the real-world scenario: multiple independent message
    // sends. With shared structures, the structure is learned once. With
    // sequential mode (fresh encoder each time), each message must embed the
    // structure definition.

    const sharedStructures: object[] = [];
    const sharedEncoder = new Encoder({
      sequential: false,
      useRecords: true,
      structures: sharedStructures,
    });

    const obj = { type: "position", x: 12.5, y: 0.5, z: -8.5, rotation: 1.57 };

    // Warm up shared structures with first message
    sharedEncoder.encode(obj);

    // Now simulate 5 messages
    let sharedTotal = 0;
    let sequentialTotal = 0;

    for (let i = 0; i < 5; i++) {
      const msg = { ...obj, x: i * 10 };

      // Shared mode: reuses the same encoder with learned structures
      sharedTotal += sharedEncoder.encode(msg).length;

      // Sequential mode: fresh encoder each time (simulating independent streams)
      const freshSequentialEncoder = new Encoder({
        sequential: true,
        useRecords: true,
      });
      sequentialTotal += freshSequentialEncoder.encode(msg).length;
    }

    // Shared structures should use significantly less bandwidth
    // because it doesn't re-embed structure definitions
    expect(sharedTotal).toBeLessThan(sequentialTotal);
  });

  it("handles complex nested structures", () => {
    const codec = new CborCodec({ sequential: false });

    const complex = {
      user: {
        id: 123,
        profile: {
          name: "Alice",
          settings: {
            theme: "dark",
            notifications: true,
          },
        },
      },
      metadata: {
        created: 1234567890,
        tags: ["important", "reviewed"],
      },
    };

    const encoded = codec.encode(complex);
    const decoded = codec.decode(encoded);
    expect(decoded).toStrictEqual(complex);
  });

  it("handles arrays of objects with consistent structure", () => {
    const codec = new CborCodec({ sequential: false });

    const items = [
      { id: 1, name: "Item 1", price: 10.99 },
      { id: 2, name: "Item 2", price: 20.99 },
      { id: 3, name: "Item 3", price: 30.99 },
    ];

    const encoded = codec.encode(items);
    const decoded = codec.decode(encoded);
    expect(decoded).toStrictEqual(items);
  });

  it("handles mixed primitive and object values", () => {
    const codec = new CborCodec({ sequential: false });

    const mixed = {
      string: "hello",
      number: 42,
      float: 3.14,
      boolean: true,
      null: null,
      array: [1, 2, 3],
      nested: { key: "value" },
    };

    const encoded = codec.encode(mixed);
    const decoded = codec.decode(encoded);
    expect(decoded).toStrictEqual(mixed);
  });

  it("handles empty and single-key objects", () => {
    const codec = new CborCodec({ sequential: false });

    // Empty object
    const empty = {};
    expect(codec.decode(codec.encode(empty))).toStrictEqual(empty);

    // Single key
    const single = { only: "one" };
    expect(codec.decode(codec.encode(single))).toStrictEqual(single);
  });

  it("handles objects with many keys", () => {
    const codec = new CborCodec({ sequential: false });

    const manyKeys: Record<string, number> = {};
    for (let i = 0; i < 50; i++) {
      manyKeys[`key${i}`] = i;
    }

    const encoded = codec.encode(manyKeys);
    const decoded = codec.decode(encoded);
    expect(decoded).toStrictEqual(manyKeys);
  });

  it("maintains structure consistency across many encode/decode cycles", () => {
    const codec = new CborCodec({ sequential: false });

    const template = { type: "event", timestamp: 0, data: { value: 0 } };

    // Encode/decode many variations
    for (let i = 0; i < 100; i++) {
      const obj = {
        ...template,
        timestamp: Date.now() + i,
        data: { value: Math.random() },
      };

      const encoded = codec.encode(obj);
      const decoded = codec.decode(encoded);
      expect(decoded).toStrictEqual(obj);
    }
  });

  it("handles special number values", () => {
    const codec = new CborCodec({ sequential: false });

    const special = {
      infinity: Infinity,
      negInfinity: -Infinity,
      // NaN requires special handling in tests since NaN !== NaN
    };

    const encoded = codec.encode(special);
    const decoded = codec.decode(encoded) as typeof special;
    expect(decoded.infinity).toBe(Infinity);
    expect(decoded.negInfinity).toBe(-Infinity);

    // Test NaN separately
    const withNaN = { value: NaN };
    const decodedNaN = codec.decode(codec.encode(withNaN)) as typeof withNaN;
    expect(Number.isNaN(decodedNaN.value)).toBe(true);
  });

  it("handles Uint8Array values", () => {
    const codec = new CborCodec({ sequential: false });

    const data = {
      binary: new Uint8Array([1, 2, 3, 4, 5]),
      label: "binary data",
    };

    const encoded = codec.encode(data);
    const decoded = codec.decode(encoded) as typeof data;
    expect(decoded.label).toBe("binary data");
    expect(decoded.binary).toBeInstanceOf(Uint8Array);
    expect(Array.from(decoded.binary)).toStrictEqual([1, 2, 3, 4, 5]);
  });

  it("compares message sizes: shared structures vs sequential for repeated structures", () => {
    const sharedStructures: object[] = [];
    const sharedEncoder = new Encoder({
      sequential: false,
      useRecords: true,
      structures: sharedStructures,
      useFloat32: FLOAT32_OPTIONS.ALWAYS,
    });

    const sequentialEncoder = new Encoder({
      sequential: true,
      useRecords: true,
      useFloat32: FLOAT32_OPTIONS.ALWAYS,
    });

    const message = { type: "update", x: 1.5, y: 2.5, z: 3.5, timestamp: 12345 };

    // Warm up shared structures
    sharedEncoder.encode(message);

    // Now compare sizes for 10 messages
    let sharedTotal = 0;
    let sequentialTotal = 0;

    for (let i = 0; i < 10; i++) {
      const msg = { ...message, timestamp: 12345 + i };
      sharedTotal += sharedEncoder.encode(msg).length;
      sequentialTotal += sequentialEncoder.encode(msg).length;
    }

    console.log("\nShared structures vs Sequential (10 messages after warmup):");
    console.log(`  Shared structures: ${sharedTotal} bytes`);
    console.log(`  Sequential:        ${sequentialTotal} bytes`);
    console.log(`  Shared is ${((sequentialTotal - sharedTotal) / sequentialTotal * 100).toFixed(1)}% smaller`);

    // After warmup, shared mode should be more efficient
    expect(sharedTotal).toBeLessThan(sequentialTotal);
  });
});

// =======================================================================================

describe("MessagePorts", () => {
  it("can communicate over MessageChannel", async () => {
    // Create a MessageChannel for communication
    let channel = new MessageChannel();

    // Set up server side with a test object
    let serverMain = new TestTarget();
    newMessagePortRpcSession(channel.port1, serverMain);

    // Set up client side
    using clientStub = newMessagePortRpcSession<TestTarget>(channel.port2);

    // Test basic method call
    let result = await clientStub.square(5);
    expect(result).toBe(25);

    // Test nested object
    let counter = await clientStub.makeCounter(10);
    expect(await counter.increment()).toBe(11);
    expect(await counter.increment(5)).toBe(16);

    // Test method that takes a stub as parameter
    let incrementResult = await clientStub.incrementCounter(counter, 2);
    expect(incrementResult).toBe(18);
  });

  it("handles errors correctly", async () => {
    let channel = new MessageChannel();

    let serverMain = new TestTarget();
    newMessagePortRpcSession(channel.port1, serverMain);
    using clientStub = newMessagePortRpcSession<TestTarget>(channel.port2);

    // Test error handling
    await expect(() => clientStub.throwError()).rejects.toThrow("test error");
  });

  it("sends close signal when server stub is disposed", async () => {
    let channel = new MessageChannel();

    let serverMain = new TestTarget();
    let serverStub = newMessagePortRpcSession(channel.port1, serverMain);
    using clientStub = newMessagePortRpcSession<TestTarget>(channel.port2);

    // Test that connection works initially
    let result = await clientStub.square(3);
    expect(result).toBe(9);

    // Set up broken callback on client
    let brokenPromise = new Promise<void>((resolve, reject) => {
      clientStub.onRpcBroken(reject);
    });

    // Dispose the server stub, which should send a close signal
    serverStub[Symbol.dispose]();

    // Wait for the client to detect the broken connection
    await expect(() => brokenPromise).rejects.toThrow(
        new Error("Peer closed MessagePort connection."));
  });
});

// =======================================================================================

describe("PropertyPath reference caching", () => {
  /**
   * Verifies that PropertyPath encoding and decoding correctly roundtrips paths.
   */
  it("correctly encodes and decodes paths", () => {
    const codec = new CborCodec();

    const testPaths = [
      ["foo"],
      ["foo", "bar"],
      ["foo", "bar", "baz"],
      ["items", 0],
      ["items", 0, "name"],
      ["deeply", "nested", "path", "to", "value"],
    ];

    for (const path of testPaths) {
      const encoded = codec.encodePath(path);
      const decoded = codec.decodePath(encoded);
      expect(decoded).toStrictEqual(path);
    }
  });

  /**
   * Verifies that empty paths are handled correctly and not cached
   * (since they're cheap to encode directly).
   */
  it("handles empty paths without caching", () => {
    const codec = new CborCodec();

    // Empty paths should always return a definition with _pd: -1
    const encoded1 = codec.encodePath([]);
    const encoded2 = codec.encodePath([]);

    expect(encoded1).toHaveProperty("_pd", -1);
    expect(encoded1).toHaveProperty("p");
    expect(encoded2).toHaveProperty("_pd", -1);
    expect(encoded2).toHaveProperty("p");

    // Both should decode to empty array
    expect(codec.decodePath(encoded1)).toStrictEqual([]);
    expect(codec.decodePath(encoded2)).toStrictEqual([]);
  });

  /**
   * Verifies that the first occurrence of a path returns a definition,
   * and subsequent occurrences return a reference.
   */
  it("returns definition on first use, reference on subsequent uses", () => {
    const codec = new CborCodec();

    const path = ["users", "profile", "name"];

    // First encode should return a definition
    const first = codec.encodePath(path);
    expect(first).toHaveProperty("_pd");
    expect(first).toHaveProperty("p");
    expect((first as any).p).toStrictEqual(path);

    // Second encode should return a reference
    const second = codec.encodePath(path);
    expect(second).toHaveProperty("_pr");
    expect(second).not.toHaveProperty("p");

    // Both should decode to the same path
    expect(codec.decodePath(first)).toStrictEqual(path);
    expect(codec.decodePath(second)).toStrictEqual(path);
  });

  /**
   * Verifies that different paths get different IDs.
   */
  it("assigns different IDs to different paths", () => {
    const codec = new CborCodec();

    const path1 = ["foo", "bar"];
    const path2 = ["foo", "baz"];
    const path3 = ["qux"];

    const enc1 = codec.encodePath(path1) as any;
    const enc2 = codec.encodePath(path2) as any;
    const enc3 = codec.encodePath(path3) as any;

    // All should be definitions with unique IDs
    expect(enc1._pd).toBe(0);
    expect(enc2._pd).toBe(1);
    expect(enc3._pd).toBe(2);

    // Encoding the same paths again should return references
    expect(codec.encodePath(path1)).toStrictEqual({ _pr: 0 });
    expect(codec.encodePath(path2)).toStrictEqual({ _pr: 1 });
    expect(codec.encodePath(path3)).toStrictEqual({ _pr: 2 });
  });

  /**
   * Verifies that paths with numeric indices (for array access) work correctly.
   */
  it("handles paths with numeric indices", () => {
    const codec = new CborCodec();

    const path = ["items", 0, "children", 2, "name"];
    const encoded = codec.encodePath(path);
    const decoded = codec.decodePath(encoded);

    expect(decoded).toStrictEqual(path);
    expect(typeof decoded[1]).toBe("number");
    expect(typeof decoded[3]).toBe("number");
  });

  /**
   * Verifies that decoding an unknown reference throws an error.
   */
  it("throws error for unknown path reference", () => {
    const codec = new CborCodec();

    expect(() => codec.decodePath({ _pr: 999 })).toThrow("Unknown path reference: 999");
  });

  /**
   * Verifies backwards compatibility: raw arrays are accepted as paths.
   */
  it("accepts raw arrays for backwards compatibility", () => {
    const codec = new CborCodec();

    const rawPath = ["foo", "bar"];
    const decoded = codec.decodePath(rawPath);

    expect(decoded).toStrictEqual(rawPath);
  });

  /**
   * Benchmarks the size savings from PropertyPath reference caching.
   * Shows that repeated paths are encoded much more compactly.
   */
  it("demonstrates size savings from path caching", () => {
    const codec = new CborCodec();

    // Simulate a typical RPC scenario: many calls to the same method path
    const methodPath = ["users", "profile", "updateSettings"];

    // First call: full definition
    const firstEncoded = codec.encode({
      type: "call",
      path: codec.encodePath(methodPath),
      args: { theme: "dark" }
    });

    // Subsequent calls: just a reference
    const subsequentEncoded = codec.encode({
      type: "call",
      path: codec.encodePath(methodPath),
      args: { theme: "light" }
    });

    console.log("\nPropertyPath caching size comparison:");
    console.log(`  First call (with path definition): ${firstEncoded.length} bytes`);
    console.log(`  Subsequent call (path reference):  ${subsequentEncoded.length} bytes`);
    console.log(`  Savings per subsequent call: ${firstEncoded.length - subsequentEncoded.length} bytes`);

    // The subsequent message should be smaller because it uses a reference instead of the full path
    expect(subsequentEncoded.length).toBeLessThan(firstEncoded.length);
  });

  /**
   * Benchmarks cumulative savings over many repeated path uses.
   */
  it("shows cumulative savings over many calls", () => {
    const codec = new CborCodec();

    const paths = [
      ["users", "get"],
      ["users", "update"],
      ["users", "profile", "get"],
      ["items", "list"],
      ["items", "create"],
    ];

    // Simulate 100 RPC calls, each using one of the paths
    let totalWithCaching = 0;
    let totalWithoutCaching = 0;

    for (let i = 0; i < 100; i++) {
      const path = paths[i % paths.length];
      const args = { requestId: i };

      // With caching
      const withCaching = codec.encode({
        type: "call",
        path: codec.encodePath(path),
        args
      });
      totalWithCaching += withCaching.length;

      // Without caching (always send full path)
      const withoutCaching = codec.encode({
        type: "call",
        path: { _pd: -1, p: path }, // Always send as definition
        args
      });
      totalWithoutCaching += withoutCaching.length;
    }

    const savings = ((totalWithoutCaching - totalWithCaching) / totalWithoutCaching * 100).toFixed(1);

    console.log("\nPropertyPath caching over 100 calls (5 unique paths):");
    console.log(`  With caching:    ${totalWithCaching} bytes`);
    console.log(`  Without caching: ${totalWithoutCaching} bytes`);
    console.log(`  Savings: ${savings}%`);

    // Should see meaningful savings
    expect(totalWithCaching).toBeLessThan(totalWithoutCaching);
  });
});
