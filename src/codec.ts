// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { Encoder, Decoder, FLOAT32_OPTIONS } from 'cbor-x'

/**
 * CBOR codec for Cap'n Web RPC messages.
 *
 * Uses cbor-x for efficient binary serialization with sequential mode enabled.
 * Sequential mode embeds structure definitions inline in the stream, allowing
 * the decoder to learn structures automatically without prior coordination.
 *
 * Key benefits:
 * - 2-3x faster decoding (compiled structure readers)
 * - 15-50% smaller messages for repeated object shapes
 * - No manual structure coordination between encoder/decoder
 *
 * IMPORTANT: Each RPC session should create its own CborCodec instance to
 * maintain proper structure state. The singleton `cborCodec` is only for
 * standalone/testing use and does NOT use sequential mode.
 */
export class CborCodec {
	private encoder: Encoder
	private decoder: Decoder
	// Separate structures arrays for encoder and decoder
	// Each maintains its own state for the structures it has seen
	private encoderStructures: object[] = []
	private decoderStructures: object[] = []

	constructor(options: { sequential?: boolean } = {}) {
		// With sequential mode (default), maxSharedStructures is automatically set to 0,
		// which means EVERY object gets an inline structure definition (tag 0xDFFF).
		//
		// IMPORTANT: The decoder MUST have a structures array to store the inline
		// definitions it receives. Without this, it can't reconstruct objects from
		// the record format.
		const sequential = options.sequential ?? true

		this.encoder = new Encoder({
			sequential,
			useRecords: true,
			encodeUndefinedAsNil: false,
			tagUint8Array: true,
			useFloat32: FLOAT32_OPTIONS.ALWAYS,
			structures: this.encoderStructures,
		})

		this.decoder = new Decoder({
			sequential,
			useRecords: true,
			structures: this.decoderStructures,
		})
	}

	encode(value: unknown): Uint8Array {
		return this.encoder.encode(value)
	}

	decode(data: Uint8Array | ArrayBuffer): unknown {
		if (data instanceof ArrayBuffer) {
			data = new Uint8Array(data)
		}
		return this.decoder.decode(data)
	}
}

// Singleton instance for standalone/testing use.
// Uses sequential: false for compatibility with independent encode/decode calls.
// RPC sessions create their own CborCodec instances with sequential: true.
export const cborCodec = new CborCodec({ sequential: false })
