// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { Encoder } from 'cbor-x'

/**
 * CBOR codec for Cap'n Web RPC messages.
 *
 * Uses cbor-x for efficient binary serialization. Key options:
 * - encodeUndefinedAsNil: false - preserves undefined vs null distinction
 * - useRecords: false - Cap'n Web uses arrays with type codes, not records
 */
export class CborCodec {
	private encoder: Encoder

	constructor() {
		this.encoder = new Encoder({
			// Preserve undefined vs null (Cap'n Web uses ["undefined"] encoding)
			encodeUndefinedAsNil: false,
			// Don't use record structures - Cap'n Web's array format is already compact
			useRecords: false,
			// Handle Uint8Array natively (no base64)
			tagUint8Array: true,
		})
	}

	encode(value: unknown): Uint8Array {
		return this.encoder.encode(value)
	}

	decode(data: Uint8Array | ArrayBuffer): unknown {
		if (data instanceof ArrayBuffer) {
			data = new Uint8Array(data)
		}
		return this.encoder.decode(data)
	}
}

// Singleton instance for convenience
export const cborCodec = new CborCodec()
