// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { Encoder, Decoder } from 'cbor-x'
import type { PropertyPath } from './core.js'

// Path reference format: first occurrence sends definition, subsequent send reference
// Definition: { _pd: id, p: path } - defines and uses path
// Reference: { _pr: id } - references previously defined path
type PathDefinition = { _pd: number; p: PropertyPath }
type PathReference = { _pr: number }

function isPathDefinition(value: unknown): value is PathDefinition {
	return (
		typeof value === 'object' &&
		value !== null &&
		'_pd' in value &&
		'p' in value &&
		typeof (value as PathDefinition)._pd === 'number' &&
		Array.isArray((value as PathDefinition).p)
	)
}

function isPathReference(value: unknown): value is PathReference {
	return (
		typeof value === 'object' &&
		value !== null &&
		'_pr' in value &&
		typeof (value as PathReference)._pr === 'number'
	)
}

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
 * Additionally implements PropertyPath reference caching:
 * - First occurrence of a path sends the full definition with an ID
 * - Subsequent occurrences send only a compact reference
 * - Typically reduces path encoding from 15-30 bytes to 3-5 bytes
 *
 * IMPORTANT: Each RPC session should create its own CborCodec instance to
 * maintain proper structure state. The singleton `cborCodec` is only for
 * standalone/testing use.
 */
export class CborCodec {
	private encoder: Encoder
	private decoder: Decoder
	// Structures array for record encoding/decoding.
	// When sequential: true, encoder and decoder can use separate arrays because
	// structure definitions are embedded inline in the stream.
	// When sequential: false, encoder and decoder MUST share the same array
	// so the decoder can look up structures that the encoder defined.
	private encoderStructures: object[] = []
	private decoderStructures: object[] = []

	// PropertyPath reference caching
	// Encoder side: maps stringified path to assigned ID
	private encodePathRegistry = new Map<string, number>()
	// Decoder side: maps ID to path (array index = ID)
	private decodePathRegistry: PropertyPath[] = []
	private nextPathId = 0

	constructor(options: { sequential?: boolean } = {}) {
		// With sequential mode (default), maxSharedStructures is automatically set to 0,
		// which means EVERY object gets an inline structure definition (tag 0xDFFF).
		//
		// IMPORTANT: The decoder MUST have a structures array to store the inline
		// definitions it receives. Without this, it can't reconstruct objects from
		// the record format.
		const sequential = options.sequential ?? true

		// When not using sequential mode, share the structures array between
		// encoder and decoder so that structure IDs resolve correctly.
		const sharedStructures = sequential ? undefined : this.encoderStructures

		this.encoder = new Encoder({
			sequential,
			useRecords: true,
			encodeUndefinedAsNil: false,
			tagUint8Array: true,
			structures: sharedStructures ?? this.encoderStructures,
		})

		this.decoder = new Decoder({
			sequential,
			useRecords: true,
			structures: sharedStructures ?? this.decoderStructures,
		})
	}

	/**
	 * Encodes a PropertyPath, using reference caching for repeated paths.
	 * First occurrence: returns { _pd: id, p: path } (definition)
	 * Subsequent: returns { _pr: id } (reference)
	 */
	encodePath(path: PropertyPath): PathDefinition | PathReference {
		// Empty paths are common and cheap, don't bother caching
		if (path.length === 0) {
			return { _pd: -1, p: path }
		}

		const key = JSON.stringify(path)
		const existingId = this.encodePathRegistry.get(key)

		if (existingId !== undefined) {
			// Path already registered, send reference
			return { _pr: existingId }
		}

		// New path, assign ID and send definition
		const id = this.nextPathId++
		this.encodePathRegistry.set(key, id)
		return { _pd: id, p: path }
	}

	/**
	 * Decodes a PropertyPath from either a definition or reference.
	 * Definitions are registered for future reference lookups.
	 */
	decodePath(value: unknown): PropertyPath {
		if (isPathDefinition(value)) {
			// It's a definition - register it (unless it's the empty path marker)
			if (value._pd >= 0) {
				this.decodePathRegistry[value._pd] = value.p
			}
			return value.p
		}

		if (isPathReference(value)) {
			// It's a reference - look it up
			const path = this.decodePathRegistry[value._pr]
			if (path === undefined) {
				throw new Error(`Unknown path reference: ${value._pr}`)
			}
			return path
		}

		// For backwards compatibility, also accept raw arrays
		if (Array.isArray(value)) {
			return value as PropertyPath
		}

		throw new Error(`Invalid path encoding: ${JSON.stringify(value)}`)
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
// Uses sequential: true (the default) which embeds structure definitions inline,
// making it compatible with independent encode/decode calls and matching RPC session behavior.
export const cborCodec = new CborCodec()
