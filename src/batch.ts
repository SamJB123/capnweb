// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { RpcStub } from "./core.js";
import { RpcTransport, RpcSession, RpcSessionOptions } from "./rpc.js";
import type { IncomingMessage, ServerResponse, OutgoingHttpHeader, OutgoingHttpHeaders } from "node:http";

type SendBatchFunc = (batch: Uint8Array[]) => Promise<Uint8Array[]>;

/**
 * Encode multiple binary messages into a single buffer using length-prefixed framing.
 * Format: [4-byte length][message bytes][4-byte length][message bytes]...
 */
function encodeBatch(messages: Uint8Array[]): Uint8Array {
  let totalLength = 0;
  for (const msg of messages) {
    totalLength += 4 + msg.length;  // 4 bytes for length prefix
  }

  const result = new Uint8Array(totalLength);
  const view = new DataView(result.buffer);
  let offset = 0;

  for (const msg of messages) {
    view.setUint32(offset, msg.length, false);  // big-endian
    offset += 4;
    result.set(msg, offset);
    offset += msg.length;
  }

  return result;
}

/**
 * Decode a length-prefixed buffer back into individual messages.
 */
function decodeBatch(data: Uint8Array): Uint8Array[] {
  const messages: Uint8Array[] = [];
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let offset = 0;

  while (offset < data.length) {
    const length = view.getUint32(offset, false);  // big-endian
    offset += 4;
    messages.push(data.slice(offset, offset + length));
    offset += length;
  }

  return messages;
}

class BatchClientTransport implements RpcTransport {
  constructor(sendBatch: SendBatchFunc) {
    this.#promise = this.#scheduleBatch(sendBatch);
  }

  #promise: Promise<void>;
  #aborted: any;

  #batchToSend: Uint8Array[] | null = [];
  #batchToReceive: Uint8Array[] | null = null;

  async send(message: Uint8Array): Promise<void> {
    // If the batch was already sent, we just ignore the message, because throwing may cause the
    // RPC system to abort prematurely. Once the last receive() is done then we'll throw an error
    // that aborts the RPC system at the right time and will propagate to all other requests.
    if (this.#batchToSend !== null) {
      this.#batchToSend.push(message);
    }
  }

  async receive(): Promise<Uint8Array> {
    if (!this.#batchToReceive) {
      await this.#promise;
    }

    let msg = this.#batchToReceive!.shift();
    if (msg !== undefined) {
      return msg;
    } else {
      // No more messages. An error thrown here will propagate out of any calls that are still
      // open.
      throw new Error("Batch RPC request ended.");
    }
  }

  abort?(reason: any): void {
    this.#aborted = reason;
  }

  async #scheduleBatch(sendBatch: SendBatchFunc) {
    // Wait for microtask queue to clear before sending a batch.
    //
    // Note that simply waiting for one turn of the microtask queue (await Promise.resolve()) is
    // not good enough here as the application needs a chance to call `.then()` on every RPC
    // promise in order to explicitly indicate they want the results. Unfortunately, `await`ing
    // a thenable does not call `.then()` immediately -- for some reason it waits for a turn of
    // the microtask queue first, *then* calls `.then()`.
    await new Promise(resolve => setTimeout(resolve, 0));

    if (this.#aborted !== undefined) {
      throw this.#aborted;
    }

    let batch = this.#batchToSend!;
    this.#batchToSend = null;
    this.#batchToReceive = await sendBatch(batch);
  }
}

export function newHttpBatchRpcSession(
    urlOrRequest: string | Request, options?: RpcSessionOptions): RpcStub {
  let sendBatch: SendBatchFunc = async (batch: Uint8Array[]) => {
    const encoded = encodeBatch(batch);
    let response = await fetch(urlOrRequest, {
      method: "POST",
      headers: {
        "Content-Type": "application/octet-stream",
      },
      // Wrap in Blob for consistent BodyInit compatibility
      body: new Blob([encoded as BlobPart]),
    });

    if (!response.ok) {
      response.body?.cancel();
      throw new Error(`RPC request failed: ${response.status} ${response.statusText}`);
    }

    let body = new Uint8Array(await response.arrayBuffer());
    return body.length === 0 ? [] : decodeBatch(body);
  };

  let transport = new BatchClientTransport(sendBatch);
  let rpc = new RpcSession(transport, undefined, options);
  return rpc.getRemoteMain();
}

class BatchServerTransport implements RpcTransport {
  constructor(batch: Uint8Array[]) {
    this.#batchToReceive = batch;
  }

  #batchToSend: Uint8Array[] = [];
  #batchToReceive: Uint8Array[];
  #allReceived: PromiseWithResolvers<void> = Promise.withResolvers<void>();

  async send(message: Uint8Array): Promise<void> {
    this.#batchToSend.push(message);
  }

  async receive(): Promise<Uint8Array> {
    let msg = this.#batchToReceive!.shift();
    if (msg !== undefined) {
      return msg;
    } else {
      // No more messages.
      this.#allReceived.resolve();
      return new Promise(r => {});
    }
  }

  abort?(reason: any): void {
    this.#allReceived.reject(reason);
  }

  whenAllReceived() {
    return this.#allReceived.promise;
  }

  getResponseBody(): Uint8Array {
    return encodeBatch(this.#batchToSend);
  }
}

/**
 * Implements the server end of an HTTP batch session, using standard Fetch API types to represent
 * HTTP requests and responses.
 *
 * @param request The request received from the client initiating the session.
 * @param localMain The main stub or RpcTarget which the server wishes to expose to the client.
 * @param options Optional RPC session options.
 * @returns The HTTP response to return to the client. Note that the returned object has mutable
 *     headers, so you can modify them using e.g. `response.headers.set("Foo", "bar")`.
 */
export async function newHttpBatchRpcResponse(
    request: Request, localMain: any, options?: RpcSessionOptions): Promise<Response> {
  if (request.method !== "POST") {
    return new Response("This endpoint only accepts POST requests.", { status: 405 });
  }

  let body = new Uint8Array(await request.arrayBuffer());
  let batch = body.length === 0 ? [] : decodeBatch(body);

  let transport = new BatchServerTransport(batch);
  let rpc = new RpcSession(transport, localMain, options);

  // TODO: Arguably we should arrange so any attempts to pull promise resolutions from the client
  //   will reject rather than just hang. But it IS valid to make server->client calls in order to
  //   then pipeline the result into something returned to the client. We don't want the errors to
  //   prematurely cancel anything that would eventually complete. So for now we just say, it's the
  //   app's responsibility to not wait on any server -> client calls since they will never
  //   complete.

  await transport.whenAllReceived();
  await rpc.drain();

  // TODO: Ask RpcSession to dispose everything it is still holding on to?

  return new Response(new Blob([transport.getResponseBody() as BlobPart]), {
    headers: { "Content-Type": "application/octet-stream" },
  });
}

/**
 * Implements the server end of an HTTP batch session using traditional Node.js HTTP APIs.
 *
 * @param request The request received from the client initiating the session.
 * @param response The response object, to which the response should be written.
 * @param localMain The main stub or RpcTarget which the server wishes to expose to the client.
 * @param options Optional RPC session options. You can also pass headers to set on the response.
 */
export async function nodeHttpBatchRpcResponse(
    request: IncomingMessage, response: ServerResponse,
    localMain: any,
    options?: RpcSessionOptions & {
      headers?: OutgoingHttpHeaders | OutgoingHttpHeader[],
    }): Promise<void> {
  if (request.method !== "POST") {
    response.writeHead(405, "This endpoint only accepts POST requests.");
    response.end();
    return;
  }

  let body = await new Promise<Uint8Array>((resolve, reject) => {
    let chunks: Buffer[] = [];
    request.on("data", chunk => {
      chunks.push(chunk);
    });
    request.on("end", () => {
      resolve(new Uint8Array(Buffer.concat(chunks)));
    });
    request.on("error", reject);
  });
  let batch = body.length === 0 ? [] : decodeBatch(body);

  let transport = new BatchServerTransport(batch);
  let rpc = new RpcSession(transport, localMain, options);

  await transport.whenAllReceived();
  await rpc.drain();

  const headers = {
    ...options?.headers,
    "Content-Type": "application/octet-stream",
  };
  response.writeHead(200, headers);
  const responseBody = transport.getResponseBody();
  response.end(Buffer.from(responseBody.buffer, responseBody.byteOffset, responseBody.byteLength));
}
