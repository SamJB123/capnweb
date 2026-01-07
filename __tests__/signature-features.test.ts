// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

/**
 * Signature Features Stress Test Suite
 *
 * This test suite validates that all of Cap'n Web's signature capabilities
 * work correctly through the PropertyPath caching optimization. Each test
 * makes genuine RPC calls over the transport layer, exercising the full
 * encode/decode path including PropertyPath reference caching.
 *
 * The 7 signature capabilities tested (10 tests each = 70 total):
 * 1. Bidirectional Calling - Client and server can call each other
 * 2. Pass-by-Reference for Functions - Functions become callable stubs
 * 3. Pass-by-Reference for Objects - RpcTarget instances are passed by reference
 * 4. Promise Pipelining - Chain dependent calls in single round trip
 * 5. Capability-Based Security - Object-capability model patterns
 * 6. Remote Map Operations - Transform values remotely with .map()
 * 7. Explicit Resource Management - Automatic disposal with Symbol.dispose
 */

import { expect, it, describe } from "vitest"
import { cborCodec, RpcSession, RpcTransport, RpcTarget, RpcStub, type RpcSessionOptions } from "../src/index.js"

// =======================================================================================
// Test Infrastructure
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

  async send(message: Uint8Array): Promise<void> {
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

async function pumpMicrotasks() {
  // Need enough iterations to process all pending messages in concurrent tests
  // Each RPC call can generate multiple messages (push, resolve, release)
  for (let i = 0; i < 100; i++) {
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
    this.server = new RpcSession<undefined>(this.serverTransport, target, serverOptions);
    this.stub = this.client.getRemoteMain();
  }

  checkAllDisposed() {
    expect(this.client.getStats(), "client").toStrictEqual({imports: 1, exports: 1});
    expect(this.server.getStats(), "server").toStrictEqual({imports: 1, exports: 1});
  }

  async [Symbol.asyncDispose]() {
    try {
      await pumpMicrotasks();
      this.checkAllDisposed();
    } catch (err) {
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

// =======================================================================================
// 1. BIDIRECTIONAL CALLING (10 tests)
// =======================================================================================

describe("Bidirectional Calling through PropertyPath caching", () => {
  /**
   * Test 1: Server calls client callback once
   */
  it("server calls client callback with single invocation", async () => {
    class ServerTarget extends RpcTarget {
      async callClientCallback(callback: RpcStub<(x: number) => number>) {
        return await callback(42);
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    const result = await harness.stub.callClientCallback((x: number) => x * 2);
    expect(result).toBe(84);
  });

  /**
   * Test 2: Server calls client callback multiple times in sequence
   */
  it("server calls client callback 50 times in sequence", async () => {
    class ServerTarget extends RpcTarget {
      async callClientCallbackManyTimes(callback: RpcStub<(x: number) => number>, count: number) {
        let sum = 0;
        for (let i = 0; i < count; i++) {
          sum += await callback(i);
        }
        return sum;
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    const result = await harness.stub.callClientCallbackManyTimes((x: number) => x * 2, 50);
    // Sum of 0*2 + 1*2 + 2*2 + ... + 49*2 = 2 * (0 + 1 + ... + 49) = 2 * (49*50/2) = 2450
    expect(result).toBe(2450);
  });

  /**
   * Test 3: Client passes multiple callbacks, server calls each
   */
  it("server calls multiple different client callbacks", async () => {
    class ServerTarget extends RpcTarget {
      async callMultipleCallbacks(
        add: RpcStub<(x: number) => number>,
        multiply: RpcStub<(x: number) => number>,
        subtract: RpcStub<(x: number) => number>
      ) {
        const a = await add(10);
        const b = await multiply(10);
        const c = await subtract(10);
        return { add: a, multiply: b, subtract: c };
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    const result = await harness.stub.callMultipleCallbacks(
      (x: number) => x + 5,
      (x: number) => x * 3,
      (x: number) => x - 2
    );
    expect(result).toStrictEqual({ add: 15, multiply: 30, subtract: 8 });
  });

  /**
   * Test 4: Nested bidirectional calling - callback calls back to server
   */
  it("callback calls back to server method", async () => {
    class ServerTarget extends RpcTarget {
      double(x: number) {
        return x * 2;
      }

      async callbackThatCallsServer(
        callback: RpcStub<(serverStub: RpcStub<ServerTarget>) => Promise<number>>,
        self: RpcStub<ServerTarget>
      ) {
        return await callback(self);
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    const result = await harness.stub.callbackThatCallsServer(
      async (server: RpcStub<ServerTarget>) => {
        const doubled = await server.double(21);
        return doubled;
      },
      harness.stub
    );
    expect(result).toBe(42);
  });

  /**
   * Test 5: Server stores callback and calls it later
   */
  it("server stores callback and calls it asynchronously", async () => {
    let storedCallback: RpcStub<(x: number) => number> | null = null;

    class ServerTarget extends RpcTarget {
      registerCallback(callback: RpcStub<(x: number) => number>) {
        // IMPORTANT: Must dup() to keep the stub alive after the call completes
        // "Any stubs the callee receives in the parameters are implicitly disposed when the call completes"
        storedCallback = callback.dup();
        return "registered";
      }

      async triggerCallback(value: number) {
        if (!storedCallback) throw new Error("No callback registered");
        return await storedCallback(value);
      }

      // Clean up the stored callback
      clearCallback() {
        if (storedCallback) {
          storedCallback[Symbol.dispose]();
          storedCallback = null;
        }
      }
    }

    await using harness = new TestHarness(new ServerTarget());

    // Register the callback
    const regResult = await harness.stub.registerCallback((x: number) => x * 10);
    expect(regResult).toBe("registered");

    // Trigger it later
    const result = await harness.stub.triggerCallback(7);
    expect(result).toBe(70);

    // Clean up before harness disposal
    await harness.stub.clearCallback();
  });

  /**
   * Test 6: Rapid bidirectional ping-pong
   */
  it("rapid bidirectional ping-pong 100 times", async () => {
    class ServerTarget extends RpcTarget {
      async pingPong(callback: RpcStub<(x: number) => number>, rounds: number) {
        let value = 0;
        for (let i = 0; i < rounds; i++) {
          value = await callback(value + 1);
        }
        return value;
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    const result = await harness.stub.pingPong((x: number) => x + 1, 100);
    // Each round: server adds 1, client adds 1 = 200 total
    expect(result).toBe(200);
  });

  /**
   * Test 7: Callback returns complex objects
   */
  it("callback returns complex objects that traverse the wire", async () => {
    class ServerTarget extends RpcTarget {
      async getComplexFromClient(
        callback: RpcStub<(id: number) => { name: string; data: number[]; nested: { value: number } }>
      ) {
        const results = [];
        for (let i = 0; i < 20; i++) {
          results.push(await callback(i));
        }
        return results;
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    const result = await harness.stub.getComplexFromClient((id: number) => ({
      name: `item-${id}`,
      data: [id, id * 2, id * 3],
      nested: { value: id * 100 }
    }));

    expect(result.length).toBe(20);
    expect(result[5]).toStrictEqual({
      name: "item-5",
      data: [5, 10, 15],
      nested: { value: 500 }
    });
  });

  /**
   * Test 8: Multiple concurrent bidirectional calls
   */
  it("multiple concurrent bidirectional calls", async () => {
    class ServerTarget extends RpcTarget {
      async callbackWithDelay(callback: RpcStub<(x: number) => number>, value: number) {
        return await callback(value);
      }
    }

    await using harness = new TestHarness(new ServerTarget());

    // Fire off 20 concurrent calls
    const promises = [];
    for (let i = 0; i < 20; i++) {
      promises.push(harness.stub.callbackWithDelay((x: number) => x * x, i));
    }

    const results = await Promise.all(promises);
    expect(results).toStrictEqual([0, 1, 4, 9, 16, 25, 36, 49, 64, 81, 100, 121, 144, 169, 196, 225, 256, 289, 324, 361]);
  });

  /**
   * Test 9: Callback throws error and it propagates back
   */
  it("callback error propagates back to server and then to client", async () => {
    class ServerTarget extends RpcTarget {
      async callCallbackSafely(callback: RpcStub<(x: number) => number>) {
        try {
          return await callback(42);
        } catch (e: any) {
          return `caught: ${e.message}`;
        }
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    const result = await harness.stub.callCallbackSafely(() => {
      throw new Error("intentional callback error");
    });
    expect(result).toBe("caught: intentional callback error");
  });

  /**
   * Test 10: Deep callback chain - server calls client calls server calls client
   */
  it("deep callback chain with 4 levels of nesting", async () => {
    class InnerProcessor extends RpcTarget {
      multiply(x: number) { return x * 2; }
    }

    class ServerTarget extends RpcTarget {
      private processor = new InnerProcessor();

      getProcessor() { return this.processor; }

      async callWithProcessor(
        callback: RpcStub<(proc: RpcStub<InnerProcessor>, x: number) => Promise<number>>,
        x: number
      ) {
        return await callback(this.processor, x);
      }
    }

    await using harness = new TestHarness(new ServerTarget());

    // Client calls server.callWithProcessor, which calls client callback with processor,
    // callback calls processor.multiply
    const result = await harness.stub.callWithProcessor(
      async (processor: RpcStub<InnerProcessor>, x: number) => {
        const doubled = await processor.multiply(x);
        return doubled + 10;
      },
      15
    );
    expect(result).toBe(40); // 15 * 2 + 10 = 40
  });
});

// =======================================================================================
// 2. PASS-BY-REFERENCE FOR FUNCTIONS (10 tests)
// =======================================================================================

describe("Pass-by-Reference for Functions through PropertyPath caching", () => {
  /**
   * Test 1: Simple function passed and called remotely
   */
  it("simple function is called remotely", async () => {
    class ServerTarget extends RpcTarget {
      async invokeFunction(fn: RpcStub<(a: number, b: number) => number>) {
        return await fn(10, 20);
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    const result = await harness.stub.invokeFunction((a: number, b: number) => a + b);
    expect(result).toBe(30);
  });

  /**
   * Test 2: Function with closure captures local state
   */
  it("function with closure captures local state", async () => {
    class ServerTarget extends RpcTarget {
      async invokeFunctionTwice(fn: RpcStub<() => number>) {
        const first = await fn();
        const second = await fn();
        return { first, second };
      }
    }

    await using harness = new TestHarness(new ServerTarget());

    let counter = 0;
    const result = await harness.stub.invokeFunctionTwice(() => {
      counter++;
      return counter;
    });

    expect(result).toStrictEqual({ first: 1, second: 2 });
    expect(counter).toBe(2);
  });

  /**
   * Test 3: Multiple functions passed in single call
   */
  it("multiple functions passed and invoked in single call", async () => {
    class ServerTarget extends RpcTarget {
      async invokeAllFunctions(
        add: RpcStub<(x: number) => number>,
        sub: RpcStub<(x: number) => number>,
        mul: RpcStub<(x: number) => number>,
        div: RpcStub<(x: number) => number>
      ) {
        const base = 100;
        return {
          added: await add(base),
          subtracted: await sub(base),
          multiplied: await mul(base),
          divided: await div(base)
        };
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    const result = await harness.stub.invokeAllFunctions(
      (x: number) => x + 50,
      (x: number) => x - 30,
      (x: number) => x * 2,
      (x: number) => x / 4
    );

    expect(result).toStrictEqual({
      added: 150,
      subtracted: 70,
      multiplied: 200,
      divided: 25
    });
  });

  /**
   * Test 4: Function invoked many times measures stub reuse
   */
  it("function stub invoked 100 times efficiently", async () => {
    class ServerTarget extends RpcTarget {
      async invokeNTimes(fn: RpcStub<(x: number) => number>, n: number) {
        let sum = 0;
        for (let i = 0; i < n; i++) {
          sum += await fn(i);
        }
        return sum;
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    const result = await harness.stub.invokeNTimes((x: number) => x * 2, 100);
    // Sum of 0*2 + 1*2 + ... + 99*2 = 2 * (99*100/2) = 9900
    expect(result).toBe(9900);
  });

  /**
   * Test 5: Function returned from server call
   */
  it("function returned from server is callable by client", async () => {
    class ServerTarget extends RpcTarget {
      getMultiplier(factor: number) {
        return (x: number) => x * factor;
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    using multiplier = await harness.stub.getMultiplier(7);

    const result1 = await multiplier(3);
    const result2 = await multiplier(10);
    const result3 = await multiplier(0);

    expect(result1).toBe(21);
    expect(result2).toBe(70);
    expect(result3).toBe(0);
  });

  /**
   * Test 6: Function passed to nested call
   */
  it("function passed through nested server calls", async () => {
    class InnerTarget extends RpcTarget {
      async applyFunction(fn: RpcStub<(x: number) => number>, value: number) {
        return await fn(value);
      }
    }

    class OuterTarget extends RpcTarget {
      inner = new InnerTarget();

      getInner() {
        return this.inner;
      }
    }

    await using harness = new TestHarness(new OuterTarget());
    using inner = await harness.stub.getInner();

    const result = await inner.applyFunction((x: number) => x * x * x, 5);
    expect(result).toBe(125);
  });

  /**
   * Test 7: Async function passed over RPC
   */
  it("async function is called remotely and awaited", async () => {
    class ServerTarget extends RpcTarget {
      async invokeAsyncFunction(fn: RpcStub<(x: number) => Promise<number>>) {
        return await fn(42);
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    const result = await harness.stub.invokeAsyncFunction(async (x: number) => {
      await Promise.resolve(); // Simulate async work
      return x + 100;
    });
    expect(result).toBe(142);
  });

  /**
   * Test 8: Function with complex argument and return types
   */
  it("function with complex types works over RPC", async () => {
    type ComplexInput = { values: number[]; multiplier: number };
    type ComplexOutput = { results: number[]; sum: number };

    class ServerTarget extends RpcTarget {
      async processWithFunction(fn: RpcStub<(input: ComplexInput) => ComplexOutput>) {
        return await fn({ values: [1, 2, 3, 4, 5], multiplier: 10 });
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    const result = await harness.stub.processWithFunction((input: ComplexInput) => {
      const results = input.values.map(v => v * input.multiplier);
      const sum = results.reduce((a, b) => a + b, 0);
      return { results, sum };
    });

    expect(result).toStrictEqual({
      results: [10, 20, 30, 40, 50],
      sum: 150
    });
  });

  /**
   * Test 9: Function array passed over RPC
   */
  it("array of functions passed and each invoked", async () => {
    class ServerTarget extends RpcTarget {
      async invokeAll(fns: RpcStub<(x: number) => number>[], value: number) {
        const results = [];
        for (const fn of fns) {
          results.push(await fn(value));
        }
        return results;
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    const result = await harness.stub.invokeAll([
      (x: number) => x + 1,
      (x: number) => x * 2,
      (x: number) => x - 5,
      (x: number) => x / 2,
      (x: number) => x ** 2
    ], 10);

    expect(result).toStrictEqual([11, 20, 5, 5, 100]);
  });

  /**
   * Test 10: Function passed, stored, and invoked across multiple calls
   */
  it("function stored and invoked across multiple separate calls", async () => {
    let storedFn: RpcStub<(x: number) => number> | null = null;

    class ServerTarget extends RpcTarget {
      storeFunction(fn: RpcStub<(x: number) => number>) {
        // IMPORTANT: Must dup() to keep the stub alive after the call completes
        storedFn = fn.dup();
        return "stored";
      }

      async invokeStored(x: number) {
        if (!storedFn) throw new Error("No function stored");
        return await storedFn(x);
      }

      // Clean up the stored function
      clearStored() {
        if (storedFn) {
          storedFn[Symbol.dispose]();
          storedFn = null;
        }
      }
    }

    await using harness = new TestHarness(new ServerTarget());

    // Store a function
    await harness.stub.storeFunction((x: number) => x * x);

    // Invoke it multiple times in separate calls
    expect(await harness.stub.invokeStored(3)).toBe(9);
    expect(await harness.stub.invokeStored(7)).toBe(49);
    expect(await harness.stub.invokeStored(12)).toBe(144);

    // Clean up before harness disposal
    await harness.stub.clearStored();
  });
});

// =======================================================================================
// 3. PASS-BY-REFERENCE FOR OBJECTS (10 tests)
// =======================================================================================

describe("Pass-by-Reference for Objects through PropertyPath caching", () => {
  /**
   * Test 1: RpcTarget returned and methods called
   */
  it("RpcTarget methods are called through stub", async () => {
    class Counter extends RpcTarget {
      private value = 0;
      increment(amount: number = 1) {
        this.value += amount;
        return this.value;
      }
      getValue() {
        return this.value;
      }
    }

    class ServerTarget extends RpcTarget {
      createCounter(initial: number) {
        const counter = new Counter();
        counter.increment(initial);
        return counter;
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    using counter = await harness.stub.createCounter(10);

    expect(await counter.getValue()).toBe(10);
    expect(await counter.increment(5)).toBe(15);
    expect(await counter.increment()).toBe(16);
  });

  /**
   * Test 2: Multiple RpcTargets returned and used independently
   */
  it("multiple RpcTargets are independent", async () => {
    class Counter extends RpcTarget {
      constructor(private value: number = 0) { super(); }
      increment(amount: number = 1) { this.value += amount; return this.value; }
      getValue() { return this.value; }
    }

    class ServerTarget extends RpcTarget {
      createCounter(initial: number) {
        return new Counter(initial);
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    using counter1 = await harness.stub.createCounter(0);
    using counter2 = await harness.stub.createCounter(100);
    using counter3 = await harness.stub.createCounter(1000);

    await counter1.increment(5);
    await counter2.increment(50);
    await counter3.increment(500);

    expect(await counter1.getValue()).toBe(5);
    expect(await counter2.getValue()).toBe(150);
    expect(await counter3.getValue()).toBe(1500);
  });

  /**
   * Test 3: RpcTarget passed back to server
   */
  it("RpcTarget passed back to server method", async () => {
    class Counter extends RpcTarget {
      constructor(private value: number = 0) { super(); }
      increment(amount: number = 1) { this.value += amount; return this.value; }
    }

    class ServerTarget extends RpcTarget {
      createCounter(initial: number) {
        return new Counter(initial);
      }
      async incrementCounter(counter: RpcStub<Counter>, amount: number) {
        return await counter.increment(amount);
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    using counter = await harness.stub.createCounter(50);

    const result = await harness.stub.incrementCounter(counter, 25);
    expect(result).toBe(75);
  });

  /**
   * Test 4: Nested RpcTargets
   */
  it("nested RpcTargets work correctly", async () => {
    class Inner extends RpcTarget {
      constructor(private value: number) { super(); }
      getValue() { return this.value; }
      double() { this.value *= 2; return this.value; }
    }

    class Outer extends RpcTarget {
      constructor(private inner: Inner) { super(); }
      getInner() { return this.inner; }
      getInnerValue() { return this.inner.getValue(); }
    }

    class ServerTarget extends RpcTarget {
      createNested(value: number) {
        return new Outer(new Inner(value));
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    using outer = await harness.stub.createNested(21);
    using inner = await outer.getInner();

    expect(await inner.getValue()).toBe(21);
    expect(await inner.double()).toBe(42);
    expect(await outer.getInnerValue()).toBe(42);
  });

  /**
   * Test 5: RpcTarget with getter property
   */
  it("RpcTarget getter properties are accessible", async () => {
    class DataHolder extends RpcTarget {
      constructor(private _name: string, private _count: number) { super(); }
      get name() { return this._name; }
      get count() { return this._count; }
      setName(name: string) { this._name = name; }
      incrementCount() { this._count++; return this._count; }
    }

    class ServerTarget extends RpcTarget {
      createDataHolder(name: string, count: number) {
        return new DataHolder(name, count);
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    using holder = await harness.stub.createDataHolder("test", 42);

    expect(await holder.name).toBe("test");
    expect(await holder.count).toBe(42);
    await holder.setName("updated");
    expect(await holder.name).toBe("updated");
    expect(await holder.incrementCount()).toBe(43);
  });

  /**
   * Test 6: RpcTarget created on client, sent to server
   */
  it("client-created RpcTarget is usable by server", async () => {
    class ClientCounter extends RpcTarget {
      private value = 0;
      increment(amount: number = 1) { this.value += amount; return this.value; }
      getValue() { return this.value; }
    }

    class ServerTarget extends RpcTarget {
      async useCounter(counter: RpcStub<ClientCounter>) {
        await counter.increment(10);
        await counter.increment(20);
        await counter.increment(30);
        return await counter.getValue();
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    const clientCounter = new ClientCounter();

    const result = await harness.stub.useCounter(clientCounter);
    expect(result).toBe(60);
  });

  /**
   * Test 7: RpcTarget with multiple method calls in sequence
   */
  it("RpcTarget handles 50 method calls in sequence", async () => {
    class Accumulator extends RpcTarget {
      private values: number[] = [];
      add(value: number) { this.values.push(value); return this.values.length; }
      getSum() { return this.values.reduce((a, b) => a + b, 0); }
      getValues() { return [...this.values]; }
    }

    class ServerTarget extends RpcTarget {
      createAccumulator() { return new Accumulator(); }
    }

    await using harness = new TestHarness(new ServerTarget());
    using acc = await harness.stub.createAccumulator();

    for (let i = 0; i < 50; i++) {
      await acc.add(i);
    }

    expect(await acc.getSum()).toBe(1225); // 0 + 1 + ... + 49
    const values = await acc.getValues();
    expect(values.length).toBe(50);
    expect(values[0]).toBe(0);
    expect(values[49]).toBe(49);
  });

  /**
   * Test 8: RpcTarget array returned
   */
  it("array of RpcTargets returned and used", async () => {
    class Worker extends RpcTarget {
      constructor(private id: number) { super(); }
      getId() { return this.id; }
      doWork(input: number) { return input * this.id; }
    }

    class ServerTarget extends RpcTarget {
      createWorkers(count: number) {
        return Array.from({ length: count }, (_, i) => new Worker(i + 1));
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    using workers = await harness.stub.createWorkers(5);

    const results = [];
    for (const worker of workers) {
      results.push(await worker.doWork(10));
    }

    expect(results).toStrictEqual([10, 20, 30, 40, 50]);
  });

  /**
   * Test 9: RpcTarget stored and accessed across multiple calls
   */
  it("RpcTarget stored and accessed across multiple calls", async () => {
    class Session extends RpcTarget {
      private data: Map<string, any> = new Map();
      set(key: string, value: any) { this.data.set(key, value); return true; }
      get(key: string) { return this.data.get(key); }
      keys() { return [...this.data.keys()]; }
    }

    let storedSession: Session | null = null;

    class ServerTarget extends RpcTarget {
      createSession() {
        storedSession = new Session();
        return storedSession;
      }
      getSession() {
        return storedSession;
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    using session1 = await harness.stub.createSession();

    await session1.set("foo", 123);
    await session1.set("bar", "hello");

    using session2 = await harness.stub.getSession();
    expect(await session2.get("foo")).toBe(123);
    expect(await session2.get("bar")).toBe("hello");
    expect(await session2.keys()).toStrictEqual(["foo", "bar"]);
  });

  /**
   * Test 10: Complex RpcTarget with method that returns another RpcTarget
   */
  it("RpcTarget method returns another RpcTarget", async () => {
    class Item extends RpcTarget {
      constructor(private name: string, private price: number) { super(); }
      getName() { return this.name; }
      getPrice() { return this.price; }
    }

    class Cart extends RpcTarget {
      private items: Item[] = [];
      addItem(name: string, price: number) {
        const item = new Item(name, price);
        this.items.push(item);
        return item;
      }
      getItem(index: number) {
        return this.items[index];
      }
      getTotal() {
        return this.items.reduce((sum, item) => sum + item.getPrice(), 0);
      }
    }

    class ServerTarget extends RpcTarget {
      createCart() { return new Cart(); }
    }

    await using harness = new TestHarness(new ServerTarget());
    using cart = await harness.stub.createCart();

    using item1 = await cart.addItem("Widget", 25);
    using item2 = await cart.addItem("Gadget", 50);
    using item3 = await cart.addItem("Gizmo", 15);

    expect(await item1.getName()).toBe("Widget");
    expect(await item2.getPrice()).toBe(50);
    expect(await cart.getTotal()).toBe(90);

    using retrievedItem = await cart.getItem(1);
    expect(await retrievedItem.getName()).toBe("Gadget");
  });
});

// =======================================================================================
// 4. PROMISE PIPELINING (10 tests)
// =======================================================================================

describe("Promise Pipelining through PropertyPath caching", () => {
  /**
   * Test 1: Basic pipelining - call method on promise result
   */
  it("calls method on promise without awaiting", async () => {
    class Counter extends RpcTarget {
      constructor(private value: number = 0) { super(); }
      getValue() { return this.value; }
      increment(amount: number = 1) { this.value += amount; return this.value; }
    }

    class ServerTarget extends RpcTarget {
      createCounter(initial: number) { return new Counter(initial); }
    }

    await using harness = new TestHarness(new ServerTarget());

    // Don't await createCounter - pipeline the call
    using counterPromise = harness.stub.createCounter(10);
    const result = await counterPromise.increment(5);

    expect(result).toBe(15);
  });

  /**
   * Test 2: Chain multiple pipelined calls
   */
  it("chains multiple pipelined method calls", async () => {
    class Builder extends RpcTarget {
      private parts: string[] = [];
      add(part: string) { this.parts.push(part); return this.parts.length; }  // Return count, not this
      build() { return this.parts.join("-"); }
    }

    class ServerTarget extends RpcTarget {
      createBuilder() { return new Builder(); }
    }

    await using harness = new TestHarness(new ServerTarget());

    using builder = harness.stub.createBuilder();
    // Chain without awaiting intermediate results
    // add() returns the count (a number), not the builder, so no extra stubs are created
    await builder.add("alpha");
    await builder.add("beta");
    await builder.add("gamma");
    const result = await builder.build();

    expect(result).toBe("alpha-beta-gamma");
  });

  /**
   * Test 3: D20 dice roll - the fundamental pipelining test
   *
   * This test proves pipelining works with genuinely unknown values.
   * The server rolls a D20 (1-20), and the client passes that unknown
   * result directly to an interpreter - all without knowing the roll.
   * The correct interpretation proves the pipeline worked.
   */
  it("pipelines a random D20 roll to an interpreter without knowing the value", async () => {
    class ServerTarget extends RpcTarget {
      private lastRoll: number = 0;

      // Roll a D20 - returns a random number 1-20
      rollD20() {
        this.lastRoll = Math.floor(Math.random() * 20) + 1;
        return this.lastRoll;
      }

      // Interpret a D20 roll result
      interpretRoll(roll: number): { roll: number; outcome: string } {
        let outcome: string;
        if (roll === 1) {
          outcome = "CRITICAL FAILURE";
        } else if (roll >= 2 && roll <= 10) {
          outcome = "FAILURE";
        } else if (roll >= 11 && roll <= 19) {
          outcome = "SUCCESS";
        } else if (roll === 20) {
          outcome = "CRITICAL SUCCESS";
        } else {
          outcome = "INVALID ROLL";
        }
        return { roll, outcome };
      }

      // For verification - what was the last roll?
      getLastRoll() {
        return this.lastRoll;
      }
    }

    await using harness = new TestHarness(new ServerTarget());

    // Client requests a D20 roll - has NO IDEA what number will come up
    using rollPromise = harness.stub.rollD20();

    // Client passes the UNKNOWN roll directly to the interpreter
    // This is pipelined - client never sees the roll value before passing it
    const result = await harness.stub.interpretRoll(rollPromise);

    // Verify the interpretation matches the actual roll
    const actualRoll = await harness.stub.getLastRoll();
    expect(result.roll).toBe(actualRoll);

    // Verify the outcome is correct for the roll
    if (actualRoll === 1) {
      expect(result.outcome).toBe("CRITICAL FAILURE");
    } else if (actualRoll >= 2 && actualRoll <= 10) {
      expect(result.outcome).toBe("FAILURE");
    } else if (actualRoll >= 11 && actualRoll <= 19) {
      expect(result.outcome).toBe("SUCCESS");
    } else {
      expect(result.outcome).toBe("CRITICAL SUCCESS");
    }
  });

  /**
   * Test 4: Pass UNKNOWN result as argument to dependent call
   *
   * This is the core pipelining test: the client does NOT know the userId
   * at call time, but passes it to getUserProfile anyway. The server
   * resolves authenticate first, extracts the userId, then calls getUserProfile.
   * All in a single round trip.
   */
  it("passes unknown property value as argument to dependent call", async () => {
    class ServerTarget extends RpcTarget {
      // Returns a user object with an ID the client doesn't know
      authenticate(token: string) {
        // Simulate looking up user by token
        const userId = token === "valid-token" ? 12345 : 0;
        return { userId, username: "Alice", role: "admin" };
      }

      // Uses the userId to fetch profile data
      getUserProfile(userId: number) {
        return {
          id: userId,
          displayName: `User #${userId}`,
          memberSince: "2024-01-01"
        };
      }
    }

    await using harness = new TestHarness(new ServerTarget());

    // Client authenticates - doesn't know what userId will be returned
    using user = harness.stub.authenticate("valid-token");

    // Client passes user.userId to getUserProfile WITHOUT knowing its value
    // This all happens in a single round trip via pipelining
    const profile = await harness.stub.getUserProfile(user.userId);

    expect(profile).toStrictEqual({
      id: 12345,
      displayName: "User #12345",
      memberSince: "2024-01-01"
    });
  });

  /**
   * Test 4: Access property of pipelined result
   */
  it("accesses property of pipelined promise result", async () => {
    class DataProvider extends RpcTarget {
      getData() {
        return { name: "test", value: 42, items: [1, 2, 3] };
      }
    }

    class ServerTarget extends RpcTarget {
      getProvider() { return new DataProvider(); }
    }

    await using harness = new TestHarness(new ServerTarget());

    using provider = harness.stub.getProvider();
    using data = provider.getData();

    // Access properties through pipelining
    expect(await data.name).toBe("test");
    expect(await data.value).toBe(42);
    expect(await data.items).toStrictEqual([1, 2, 3]);
  });

  /**
   * Test 5: Complex pipelining with nested property access
   */
  it("pipelines through nested property access", async () => {
    class User extends RpcTarget {
      constructor(private id: number, private name: string) { super(); }
      get profile() {
        return { userId: this.id, username: this.name, level: this.id * 10 };
      }
    }

    class ServerTarget extends RpcTarget {
      getUser(id: number, name: string) { return new User(id, name); }
    }

    await using harness = new TestHarness(new ServerTarget());

    using user = harness.stub.getUser(5, "Alice");
    const level = await user.profile.level;

    expect(level).toBe(50);
  });

  /**
   * Test 6: Multiple concurrent pipelined operations
   */
  it("runs 20 concurrent pipelined operations", async () => {
    class ServerTarget extends RpcTarget {
      square(x: number) { return x * x; }
    }

    await using harness = new TestHarness(new ServerTarget());

    // Fire off 20 pipelined calls concurrently on the main stub
    const promises = [];
    for (let i = 0; i < 20; i++) {
      promises.push(harness.stub.square(i));
    }

    const results = await Promise.all(promises);
    expect(results).toStrictEqual([0, 1, 4, 9, 16, 25, 36, 49, 64, 81, 100, 121, 144, 169, 196, 225, 256, 289, 324, 361]);
  });

  /**
   * Test 7: Pipelining with error propagation
   */
  it("propagates errors through pipelined calls", async () => {
    class Faulty extends RpcTarget {
      succeed() { return "ok"; }
      fail(): never { throw new Error("pipelined error"); }
    }

    class ServerTarget extends RpcTarget {
      getFaulty() { return new Faulty(); }
    }

    await using harness = new TestHarness(new ServerTarget());

    using faulty = harness.stub.getFaulty();

    // This should work
    expect(await faulty.succeed()).toBe("ok");

    // This should propagate the error
    await expect(() => faulty.fail()).rejects.toThrow("pipelined error");
  });

  /**
   * Test 8: Deep pipelining chain (5 levels)
   */
  it("handles 5 levels of pipelined nesting", async () => {
    class Level extends RpcTarget {
      constructor(private depth: number) { super(); }
      getNext() { return new Level(this.depth + 1); }
      getDepth() { return this.depth; }
    }

    class ServerTarget extends RpcTarget {
      getLevel() { return new Level(1); }
    }

    await using harness = new TestHarness(new ServerTarget());

    using level1 = harness.stub.getLevel();
    using level2 = level1.getNext();
    using level3 = level2.getNext();
    using level4 = level3.getNext();
    using level5 = level4.getNext();

    expect(await level5.getDepth()).toBe(5);
  });

  /**
   * Test 9: Chain of dependent calls where each uses the previous result
   *
   * Simulates a multi-step workflow: login -> get permissions -> fetch allowed data
   * Each step depends on the UNKNOWN result of the previous step.
   */
  it("chains multiple dependent calls using unknown intermediate results", async () => {
    class ServerTarget extends RpcTarget {
      // Step 1: Login returns a session with unknown sessionId
      login(username: string) {
        const sessionId = `session-${username}-${Date.now()}`;
        return { sessionId, user: username };
      }

      // Step 2: Get permissions using sessionId (unknown to client)
      getPermissions(sessionId: string) {
        return {
          sessionId,
          canRead: true,
          canWrite: sessionId.includes("admin"),
          maxItems: 100
        };
      }

      // Step 3: Fetch data using maxItems from permissions (unknown to client)
      fetchData(maxItems: number) {
        return Array.from({ length: Math.min(maxItems, 5) }, (_, i) => ({
          id: i + 1,
          name: `Item ${i + 1}`
        }));
      }
    }

    await using harness = new TestHarness(new ServerTarget());

    // Client initiates a chain of dependent calls:
    // 1. Login (result unknown)
    using session = harness.stub.login("admin");

    // 2. Get permissions using session.sessionId (value unknown at call time)
    using permissions = harness.stub.getPermissions(session.sessionId);

    // 3. Fetch data using permissions.maxItems (value unknown at call time)
    const data = await harness.stub.fetchData(permissions.maxItems);

    // All three calls were pipelined - the server resolved each in sequence
    expect(data.length).toBe(5);
    expect(data[0]).toStrictEqual({ id: 1, name: "Item 1" });
  });

  /**
   * Test 10: Complex dependent pipelining with method calls on unknown result
   *
   * The client gets an object, then calls a method on it to get a value,
   * then passes that value to another method. All without knowing any
   * intermediate values.
   */
  it("pipelines method call results into subsequent calls", async () => {
    class Wallet extends RpcTarget {
      constructor(private balance: number) { super(); }
      getBalance() { return this.balance; }
      withdraw(amount: number) {
        if (amount > this.balance) throw new Error("Insufficient funds");
        this.balance -= amount;
        return amount;
      }
    }

    class ServerTarget extends RpcTarget {
      // Returns a wallet with unknown balance
      getWallet(userId: string) {
        // Simulate different users having different balances
        const balances: Record<string, number> = {
          "user-1": 100,
          "user-2": 250,
          "user-3": 50
        };
        return new Wallet(balances[userId] || 0);
      }

      // Process a payment using an amount (unknown to client at call time)
      processPayment(amount: number) {
        return {
          transactionId: `txn-${Date.now()}`,
          amount,
          status: amount > 0 ? "completed" : "failed",
          fee: Math.floor(amount * 0.02)
        };
      }
    }

    await using harness = new TestHarness(new ServerTarget());

    // Get wallet (balance unknown to client)
    using wallet = harness.stub.getWallet("user-2");

    // Get balance from wallet (value unknown to client)
    using balance = wallet.getBalance();

    // Process payment using the unknown balance value
    const receipt = await harness.stub.processPayment(balance);

    // The server resolved: getWallet -> getBalance (250) -> processPayment(250)
    expect(receipt.amount).toBe(250);
    expect(receipt.status).toBe("completed");
    expect(receipt.fee).toBe(5); // 2% of 250
  });
});

// =======================================================================================
// 5. CAPABILITY-BASED SECURITY (10 tests)
// =======================================================================================

describe("Capability-Based Security through PropertyPath caching", () => {
  /**
   * Test 1: Access control through capability possession
   */
  it("only allows access with proper capability", async () => {
    class SecretData extends RpcTarget {
      getSecret() { return "top-secret-data"; }
    }

    class PublicApi extends RpcTarget {
      authenticate(password: string): SecretData | null {
        if (password === "correct-password") {
          return new SecretData();
        }
        return null;
      }
    }

    await using harness = new TestHarness(new PublicApi());

    // Wrong password returns null
    const badResult = await harness.stub.authenticate("wrong-password");
    expect(badResult).toBe(null);

    // Correct password returns capability
    using secret = await harness.stub.authenticate("correct-password");
    expect(secret).not.toBe(null);
    expect(await secret!.getSecret()).toBe("top-secret-data");
  });

  /**
   * Test 2: Attenuation - returning limited capability
   */
  it("supports capability attenuation (read-only view)", async () => {
    class FullAccess extends RpcTarget {
      private value = 100;
      read() { return this.value; }
      write(v: number) { this.value = v; return this.value; }
    }

    class ReadOnlyView extends RpcTarget {
      constructor(private source: FullAccess) { super(); }
      read() { return this.source.read(); }
      // No write method exposed
    }

    class ServerTarget extends RpcTarget {
      private data = new FullAccess();
      getFullAccess() { return this.data; }
      getReadOnlyView() { return new ReadOnlyView(this.data); }
    }

    await using harness = new TestHarness(new ServerTarget());

    // Full access can read and write
    using full = await harness.stub.getFullAccess();
    expect(await full.read()).toBe(100);
    expect(await full.write(200)).toBe(200);

    // Read-only view can only read
    using readOnly = await harness.stub.getReadOnlyView();
    expect(await readOnly.read()).toBe(200);
    // readOnly.write doesn't exist
    expect(await (readOnly as any).write).toBe(undefined);
  });

  /**
   * Test 3: Capability delegation - passing capability to third party
   */
  it("supports capability delegation", async () => {
    class Resource extends RpcTarget {
      private uses = 0;
      use() { this.uses++; return this.uses; }
      getUses() { return this.uses; }
    }

    class Intermediary extends RpcTarget {
      async useResource(resource: RpcStub<Resource>) {
        return await resource.use();
      }
    }

    class Owner extends RpcTarget {
      private resource = new Resource();
      private intermediary = new Intermediary();

      getResource() { return this.resource; }
      getIntermediary() { return this.intermediary; }
    }

    await using harness = new TestHarness(new Owner());

    using resource = await harness.stub.getResource();
    using intermediary = await harness.stub.getIntermediary();

    // Direct use
    expect(await resource.use()).toBe(1);

    // Delegated use through intermediary
    expect(await intermediary.useResource(resource)).toBe(2);
    expect(await intermediary.useResource(resource)).toBe(3);

    // Verify all uses were recorded
    expect(await resource.getUses()).toBe(3);
  });

  /**
   * Test 4: Capability revocation
   */
  it("supports capability revocation", async () => {
    class RevocableAccess extends RpcTarget {
      private revoked = false;
      revoke() { this.revoked = true; }
      access() {
        if (this.revoked) throw new Error("Access revoked");
        return "granted";
      }
    }

    class ServerTarget extends RpcTarget {
      createRevocable() { return new RevocableAccess(); }
    }

    await using harness = new TestHarness(new ServerTarget());
    using access = await harness.stub.createRevocable();

    // Access works initially
    expect(await access.access()).toBe("granted");

    // Revoke the capability
    await access.revoke();

    // Access now fails
    await expect(() => access.access()).rejects.toThrow("Access revoked");
  });

  /**
   * Test 5: Least privilege - minimal capabilities granted
   */
  it("enforces least privilege principle", async () => {
    class Database extends RpcTarget {
      private data: Map<string, string> = new Map([
        ["public", "public-value"],
        ["private", "private-value"],
        ["admin", "admin-value"]
      ]);

      get(key: string) { return this.data.get(key); }
      set(key: string, value: string) { this.data.set(key, value); }
      delete(key: string) { this.data.delete(key); }
    }

    class PublicReader extends RpcTarget {
      constructor(private db: Database) { super(); }
      read() { return this.db.get("public"); }
    }

    class PrivateReader extends RpcTarget {
      constructor(private db: Database) { super(); }
      readPublic() { return this.db.get("public"); }
      readPrivate() { return this.db.get("private"); }
    }

    class ServerTarget extends RpcTarget {
      private db = new Database();
      getPublicAccess() { return new PublicReader(this.db); }
      getPrivateAccess() { return new PrivateReader(this.db); }
    }

    await using harness = new TestHarness(new ServerTarget());

    using publicAccess = await harness.stub.getPublicAccess();
    expect(await publicAccess.read()).toBe("public-value");

    using privateAccess = await harness.stub.getPrivateAccess();
    expect(await privateAccess.readPublic()).toBe("public-value");
    expect(await privateAccess.readPrivate()).toBe("private-value");
  });

  /**
   * Test 6: Capability composition
   */
  it("supports capability composition", async () => {
    class Reader extends RpcTarget {
      constructor(private data: { value: number }) { super(); }
      read() { return this.data.value; }
    }

    class Writer extends RpcTarget {
      constructor(private data: { value: number }) { super(); }
      write(v: number) { this.data.value = v; return v; }
    }

    class Composite extends RpcTarget {
      constructor(
        private reader: Reader,
        private writer: Writer
      ) { super(); }
      getReader() { return this.reader; }
      getWriter() { return this.writer; }
    }

    class ServerTarget extends RpcTarget {
      private data = { value: 0 };
      getComposite() {
        return new Composite(
          new Reader(this.data),
          new Writer(this.data)
        );
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    using composite = await harness.stub.getComposite();
    using reader = await composite.getReader();
    using writer = await composite.getWriter();

    expect(await reader.read()).toBe(0);
    await writer.write(42);
    expect(await reader.read()).toBe(42);
  });

  /**
   * Test 7: Capability confinement - no ambient authority
   */
  it("enforces capability confinement", async () => {
    let globalAccessCount = 0;

    class ConfinedOperation extends RpcTarget {
      constructor(private resource: { access: () => void }) { super(); }
      operate() {
        this.resource.access();
        return "operated";
      }
    }

    class ServerTarget extends RpcTarget {
      createOperation() {
        return new ConfinedOperation({
          access: () => { globalAccessCount++; }
        });
      }
    }

    await using harness = new TestHarness(new ServerTarget());
    using op = await harness.stub.createOperation();

    expect(globalAccessCount).toBe(0);
    await op.operate();
    expect(globalAccessCount).toBe(1);
    await op.operate();
    expect(globalAccessCount).toBe(2);
  });

  /**
   * Test 8: Multi-level capability hierarchy
   */
  it("supports multi-level capability hierarchy", async () => {
    class Level3 extends RpcTarget {
      action() { return "level3-action"; }
    }

    class Level2 extends RpcTarget {
      private l3 = new Level3();
      action() { return "level2-action"; }
      getLevel3() { return this.l3; }
    }

    class Level1 extends RpcTarget {
      private l2 = new Level2();
      action() { return "level1-action"; }
      getLevel2() { return this.l2; }
    }

    class ServerTarget extends RpcTarget {
      getLevel1() { return new Level1(); }
    }

    await using harness = new TestHarness(new ServerTarget());
    using l1 = await harness.stub.getLevel1();
    using l2 = await l1.getLevel2();
    using l3 = await l2.getLevel3();

    expect(await l1.action()).toBe("level1-action");
    expect(await l2.action()).toBe("level2-action");
    expect(await l3.action()).toBe("level3-action");
  });

  /**
   * Test 9: Capability-based access control list
   */
  it("implements capability-based ACL", async () => {
    class Document extends RpcTarget {
      private content = "secret document content";
      private editors: Set<string> = new Set(["alice", "bob"]);

      read() { return this.content; }

      edit(editor: string, newContent: string) {
        if (!this.editors.has(editor)) {
          throw new Error("Not authorized to edit");
        }
        this.content = newContent;
        return this.content;
      }

      addEditor(admin: string, newEditor: string) {
        if (admin !== "alice") throw new Error("Only alice can add editors");
        this.editors.add(newEditor);
        return true;
      }
    }

    class ServerTarget extends RpcTarget {
      getDocument() { return new Document(); }
    }

    await using harness = new TestHarness(new ServerTarget());
    using doc = await harness.stub.getDocument();

    // Authorized edit
    expect(await doc.edit("alice", "edited by alice")).toBe("edited by alice");
    expect(await doc.edit("bob", "edited by bob")).toBe("edited by bob");

    // Unauthorized edit
    await expect(() => doc.edit("eve", "hacked")).rejects.toThrow("Not authorized");

    // Add new editor
    await doc.addEditor("alice", "charlie");
    expect(await doc.edit("charlie", "edited by charlie")).toBe("edited by charlie");
  });

  /**
   * Test 10: Capability-based rate limiting
   */
  it("implements capability-based rate limiting", async () => {
    class RateLimitedApi extends RpcTarget {
      private callCount = 0;
      private readonly limit = 5;

      call() {
        if (this.callCount >= this.limit) {
          throw new Error("Rate limit exceeded");
        }
        this.callCount++;
        return `call ${this.callCount}`;
      }

      getRemainingCalls() {
        return this.limit - this.callCount;
      }
    }

    class ServerTarget extends RpcTarget {
      createRateLimitedApi() { return new RateLimitedApi(); }
    }

    await using harness = new TestHarness(new ServerTarget());
    using api = await harness.stub.createRateLimitedApi();

    // Use up the rate limit
    for (let i = 1; i <= 5; i++) {
      expect(await api.call()).toBe(`call ${i}`);
      expect(await api.getRemainingCalls()).toBe(5 - i);
    }

    // Next call should fail
    await expect(() => api.call()).rejects.toThrow("Rate limit exceeded");
  });
});

// =======================================================================================
// 6. REMOTE MAP OPERATIONS (10 tests)
// =======================================================================================

describe("Remote Map Operations through PropertyPath caching", () => {
  /**
   * Test 1: Basic map over array
   */
  it("maps function over array result", async () => {
    class DataProvider extends RpcTarget {
      getNumbers() { return [1, 2, 3, 4, 5]; }
      square(x: number) { return x * x; }
    }

    await using harness = new TestHarness(new DataProvider());

    using numbers = harness.stub.getNumbers();
    const squares = await numbers.map((n: number) => harness.stub.square(n));

    expect(squares).toStrictEqual([1, 4, 9, 16, 25]);
  });

  /**
   * Test 2: Map returns complex objects
   */
  it("map returns complex objects for each element", async () => {
    class DataProvider extends RpcTarget {
      getIds() { return [1, 2, 3]; }
      getDetails(id: number) {
        return { id, name: `item-${id}`, value: id * 100 };
      }
    }

    await using harness = new TestHarness(new DataProvider());

    using ids = harness.stub.getIds();
    const details = await ids.map((id: number) => harness.stub.getDetails(id));

    expect(details).toStrictEqual([
      { id: 1, name: "item-1", value: 100 },
      { id: 2, name: "item-2", value: 200 },
      { id: 3, name: "item-3", value: 300 }
    ]);
  });

  /**
   * Test 3: Nested map operations
   */
  it("supports nested map operations", async () => {
    class DataProvider extends RpcTarget {
      getOuter() { return [1, 2, 3]; }
      getInner(x: number) { return [x, x * 2]; }
      double(x: number) { return x * 2; }
    }

    await using harness = new TestHarness(new DataProvider());

    using outer = harness.stub.getOuter();
    const result = await outer.map((x: number) => {
      return harness.stub.getInner(x).map((y: number) => {
        return harness.stub.double(y);
      });
    });

    expect(result).toStrictEqual([
      [2, 4],
      [4, 8],
      [6, 12]
    ]);
  });

  /**
   * Test 4: Map over nullable value (returns null)
   */
  it("handles null value correctly in map", async () => {
    class DataProvider extends RpcTarget {
      getNull() { return null; }
      transform(x: any) { return x * 2; }
    }

    await using harness = new TestHarness(new DataProvider());

    using nullValue = harness.stub.getNull();
    const result = await nullValue.map((_: any) => harness.stub.transform(42));

    expect(result).toBe(null);
  });

  /**
   * Test 5: Map over undefined value
   */
  it("handles undefined value correctly in map", async () => {
    class DataProvider extends RpcTarget {
      getUndefined() { return undefined; }
      transform(x: any) { return x * 2; }
    }

    await using harness = new TestHarness(new DataProvider());

    using undefinedValue = harness.stub.getUndefined();
    const result = await undefinedValue.map((_: any) => harness.stub.transform(42));

    expect(result).toBe(undefined);
  });

  /**
   * Test 6: Map over single non-null, non-array value
   */
  it("maps over single value", async () => {
    class DataProvider extends RpcTarget {
      getSingleValue() { return 42; }
      double(x: number) { return x * 2; }
    }

    await using harness = new TestHarness(new DataProvider());

    using single = harness.stub.getSingleValue();
    const result = await single.map((x: number) => harness.stub.double(x));

    expect(result).toBe(84);
  });

  /**
   * Test 7: Map with RpcTarget creation
   */
  it("map creates RpcTargets for each element", async () => {
    class Counter extends RpcTarget {
      constructor(private value: number) { super(); }
      getValue() { return this.value; }
      increment() { this.value++; return this.value; }
    }

    class DataProvider extends RpcTarget {
      getValues() { return [10, 20, 30]; }
      makeCounter(initial: number) { return new Counter(initial); }
    }

    await using harness = new TestHarness(new DataProvider());

    using values = harness.stub.getValues();
    using counters = await values.map((v: number) => harness.stub.makeCounter(v));

    expect(counters.length).toBe(3);
    expect(await counters[0].getValue()).toBe(10);
    expect(await counters[1].increment()).toBe(21);
    expect(await counters[2].getValue()).toBe(30);
  });

  /**
   * Test 8: Map over large array (100 elements)
   */
  it("efficiently maps over 100 elements", async () => {
    class DataProvider extends RpcTarget {
      getRange(n: number) { return Array.from({ length: n }, (_, i) => i); }
      square(x: number) { return x * x; }
    }

    await using harness = new TestHarness(new DataProvider());

    using range = harness.stub.getRange(100);
    const squares = await range.map((x: number) => harness.stub.square(x));

    expect(squares.length).toBe(100);
    expect(squares[0]).toBe(0);
    expect(squares[10]).toBe(100);
    expect(squares[99]).toBe(9801);
  });

  /**
   * Test 9: Map with multiple RPC calls per element
   */
  it("map callback makes multiple RPC calls per element", async () => {
    class DataProvider extends RpcTarget {
      getIds() { return [1, 2, 3]; }
      getName(id: number) { return `name-${id}`; }
      getScore(id: number) { return id * 100; }
      getLevel(id: number) { return id * 10; }
    }

    await using harness = new TestHarness(new DataProvider());

    using ids = harness.stub.getIds();
    const profiles = await ids.map((id: number) => {
      return {
        id,
        name: harness.stub.getName(id),
        score: harness.stub.getScore(id),
        level: harness.stub.getLevel(id)
      };
    });

    expect(profiles).toStrictEqual([
      { id: 1, name: "name-1", score: 100, level: 10 },
      { id: 2, name: "name-2", score: 200, level: 20 },
      { id: 3, name: "name-3", score: 300, level: 30 }
    ]);
  });

  /**
   * Test 10: Chained map operations - nested maps within callback
   */
  it("chains multiple map operations via nested maps", async () => {
    class DataProvider extends RpcTarget {
      getNumbers() { return [1, 2, 3, 4, 5]; }
      double(x: number) { return x * 2; }
      addTen(x: number) { return x + 10; }
    }

    await using harness = new TestHarness(new DataProvider());

    using numbers = harness.stub.getNumbers();

    // Chain maps: for each number, double it, then map over that result to add ten
    // This demonstrates nested .map() calls within the callback
    const result = await numbers.map((x: number) => {
      // harness.stub.double(x) returns an RpcPromise
      // We can call .map() on that promise to chain another operation
      return harness.stub.double(x).map((doubled: number) => {
        return harness.stub.addTen(doubled);
      });
    });

    // Each number: double then add 10
    // 1 -> 2 -> 12, 2 -> 4 -> 14, 3 -> 6 -> 16, 4 -> 8 -> 18, 5 -> 10 -> 20
    expect(result).toStrictEqual([12, 14, 16, 18, 20]);
  });
});

// =======================================================================================
// 7. EXPLICIT RESOURCE MANAGEMENT (10 tests)
// =======================================================================================

describe("Explicit Resource Management through PropertyPath caching", () => {
  /**
   * Test 1: Basic disposal with Symbol.dispose
   */
  it("disposes RpcTarget when stub is disposed", async () => {
    let disposed = false;

    class Disposable extends RpcTarget {
      getValue() { return 42; }
      [Symbol.dispose]() { disposed = true; }
    }

    class ServerTarget extends RpcTarget {
      getDisposable() { return new Disposable(); }
    }

    await using harness = new TestHarness(new ServerTarget());

    {
      using resource = await harness.stub.getDisposable();
      expect(await resource.getValue()).toBe(42);
      expect(disposed).toBe(false);
    } // disposed here

    await pumpMicrotasks();
    expect(disposed).toBe(true);
  });

  /**
   * Test 2: Multiple disposals tracked correctly
   */
  it("tracks multiple independent disposals", async () => {
    const disposals: number[] = [];

    class Disposable extends RpcTarget {
      constructor(private id: number) { super(); }
      getId() { return this.id; }
      [Symbol.dispose]() { disposals.push(this.id); }
    }

    class ServerTarget extends RpcTarget {
      getDisposable(id: number) { return new Disposable(id); }
    }

    await using harness = new TestHarness(new ServerTarget());

    {
      using r1 = await harness.stub.getDisposable(1);
      using r2 = await harness.stub.getDisposable(2);
      using r3 = await harness.stub.getDisposable(3);

      expect(await r1.getId()).toBe(1);
      expect(await r2.getId()).toBe(2);
      expect(await r3.getId()).toBe(3);
    }

    await pumpMicrotasks();
    expect(disposals.sort()).toStrictEqual([1, 2, 3]);
  });

  /**
   * Test 3: Stub.dup() creates reference that delays disposal
   */
  it("dup() delays disposal until all references disposed", async () => {
    let disposed = false;

    class Disposable extends RpcTarget {
      getValue() { return "alive"; }
      [Symbol.dispose]() { disposed = true; }
    }

    class ServerTarget extends RpcTarget {
      getDisposable() { return new Disposable(); }
    }

    await using harness = new TestHarness(new ServerTarget());

    let duped: RpcStub<Disposable>;

    {
      using original = await harness.stub.getDisposable();
      duped = original.dup();
      expect(await original.getValue()).toBe("alive");
    } // original disposed

    await pumpMicrotasks();
    expect(disposed).toBe(false); // duped still holds reference

    expect(await duped.getValue()).toBe("alive");
    duped[Symbol.dispose]();

    await pumpMicrotasks();
    expect(disposed).toBe(true);
  });

  /**
   * Test 4: Disposing promise disposes the eventual result
   */
  it("disposing promise disposes eventual stub result", async () => {
    let disposed = false;

    class Disposable extends RpcTarget {
      getValue() { return "data"; }
      [Symbol.dispose]() { disposed = true; }
    }

    class ServerTarget extends RpcTarget {
      getDisposable() { return new Disposable(); }
    }

    await using harness = new TestHarness(new ServerTarget());

    {
      using promise = harness.stub.getDisposable();
      // Don't await - dispose the promise directly
    }

    await pumpMicrotasks();
    expect(disposed).toBe(true);
  });

  /**
   * Test 5: Disposal on disconnect
   */
  it("disposes all stubs on connection disconnect", async () => {
    const disposals: number[] = [];

    class Disposable extends RpcTarget {
      constructor(private id: number) { super(); }
      [Symbol.dispose]() { disposals.push(this.id); }
    }

    class ServerTarget extends RpcTarget {
      getDisposable(id: number) { return new Disposable(id); }
    }

    // Don't use `using` since we're forcing disconnect
    const harness = new TestHarness(new ServerTarget());

    const r1 = await harness.stub.getDisposable(1);
    const r2 = await harness.stub.getDisposable(2);

    // Force disconnect
    harness.clientTransport.forceReceiveError(new Error("disconnected"));

    await pumpMicrotasks();
    expect(disposals.sort()).toStrictEqual([1, 2]);
  });

  /**
   * Test 6: Nested disposal - outer disposes inner
   */
  it("nested resources dispose correctly", async () => {
    const disposals: string[] = [];

    class Inner extends RpcTarget {
      getValue() { return "inner"; }
      [Symbol.dispose]() { disposals.push("inner"); }
    }

    class Outer extends RpcTarget {
      private inner = new Inner();
      getInner() { return this.inner; }
      [Symbol.dispose]() { disposals.push("outer"); }
    }

    class ServerTarget extends RpcTarget {
      getOuter() { return new Outer(); }
    }

    await using harness = new TestHarness(new ServerTarget());

    {
      using outer = await harness.stub.getOuter();
      using inner = await outer.getInner();
      expect(await inner.getValue()).toBe("inner");
    }

    await pumpMicrotasks();
    // Both should be disposed (order may vary)
    expect(disposals.sort()).toStrictEqual(["inner", "outer"]);
  });

  /**
   * Test 7: Idempotent disposal
   */
  it("disposal is idempotent - multiple dispose calls are safe", async () => {
    let disposeCount = 0;

    class Disposable extends RpcTarget {
      [Symbol.dispose]() { disposeCount++; }
    }

    class ServerTarget extends RpcTarget {
      getDisposable() { return new Disposable(); }
    }

    await using harness = new TestHarness(new ServerTarget());

    const resource = await harness.stub.getDisposable();

    // Dispose multiple times
    resource[Symbol.dispose]();
    resource[Symbol.dispose]();
    resource[Symbol.dispose]();

    await pumpMicrotasks();
    expect(disposeCount).toBe(1); // Only one actual disposal
  });

  /**
   * Test 8: Disposal cleans up resources after many operations
   */
  it("cleans up after many operations", async () => {
    const active = new Set<number>();

    class Resource extends RpcTarget {
      constructor(private id: number) {
        super();
        active.add(id);
      }
      use() { return this.id; }
      [Symbol.dispose]() { active.delete(this.id); }
    }

    class ServerTarget extends RpcTarget {
      createResource(id: number) { return new Resource(id); }
    }

    await using harness = new TestHarness(new ServerTarget());

    // Create and dispose 50 resources
    for (let i = 0; i < 50; i++) {
      using resource = await harness.stub.createResource(i);
      await resource.use();
    }

    await pumpMicrotasks();
    expect(active.size).toBe(0);
  });

  /**
   * Test 9: Return value disposal
   */
  it("disposing return value cleans up nested stubs", async () => {
    const disposed = new Set<string>();

    class Item extends RpcTarget {
      constructor(private name: string) { super(); }
      getName() { return this.name; }
      [Symbol.dispose]() { disposed.add(this.name); }
    }

    class ServerTarget extends RpcTarget {
      getItems() {
        return [
          new Item("first"),
          new Item("second"),
          new Item("third")
        ];
      }
    }

    await using harness = new TestHarness(new ServerTarget());

    {
      using items = await harness.stub.getItems();
      expect(await items[0].getName()).toBe("first");
      expect(await items[1].getName()).toBe("second");
      expect(await items[2].getName()).toBe("third");
    }

    await pumpMicrotasks();
    expect(disposed).toStrictEqual(new Set(["first", "second", "third"]));
  });

  /**
   * Test 10: onRpcBroken callback invoked on disposal
   */
  it("onRpcBroken is called when stub becomes unusable", async () => {
    class Disposable extends RpcTarget {
      getValue() { return 42; }
    }

    class ServerTarget extends RpcTarget {
      getDisposable() { return new Disposable(); }
    }

    // Don't use `using` since we're forcing disconnect
    const harness = new TestHarness(new ServerTarget());

    const resource = await harness.stub.getDisposable();

    let brokenError: any = null;
    resource.onRpcBroken((err) => { brokenError = err; });

    // Force disconnect
    harness.clientTransport.forceReceiveError(new Error("connection lost"));

    await pumpMicrotasks();
    expect(brokenError).toBeInstanceOf(Error);
    expect(brokenError.message).toBe("connection lost");
  });
});
