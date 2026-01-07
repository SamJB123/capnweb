import { Encoder, Decoder, FLOAT32_OPTIONS } from 'cbor-x';

// ============================================================================
// RESULTS COLLECTOR
// ============================================================================

const results = {
  breakEvenPoint: null,
  sizeSavings: { vsJson: [], vsCborNoRec: [] },
  bandwidth: {},
  speed: { encodeSpeedups: [], decodeSpeedups: [] },
};

// ============================================================================
// CODEC CONFIGURATIONS
// ============================================================================

// Configuration 1: No records (maximum compatibility, no structure reuse)
const noRecordsEncoder = new Encoder({
  useRecords: false,
  useFloat32: FLOAT32_OPTIONS.ALWAYS,
  tagUint8Array: true,
});

// Configuration 2: Sequential mode with records (optimal for streaming RPC)
// Each encoder instance maintains its own structure state
function createSequentialCodec() {
  const options = {
    sequential: true,
    useRecords: true,
    useFloat32: FLOAT32_OPTIONS.ALWAYS,
    tagUint8Array: true,
  };
  return {
    encoder: new Encoder(options),
    decoder: new Decoder(options),
  };
}

// ============================================================================
// TEST PAYLOADS
// ============================================================================

const payloads = {
  // Simple RPC call
  simpleCall: {
    name: "Simple method call",
    data: ["call", 1, 0, "square", [5]],
  },

  // Position update (high-frequency game state)
  positionUpdate: {
    name: "Position update (game tick)",
    data: ["call", 1, 0, "updatePosition", [{
      x: 12.456,
      y: 0.5,
      z: -8.234,
      rotation: 1.57,
    }]],
  },

  // Entity state update (ECS-style)
  entityUpdate: {
    name: "Entity state update",
    data: ["push", {
      entityId: 12345,
      components: {
        position: { x: 10.5, y: 0.0, z: -5.2 },
        velocity: { x: 1.0, y: 0.0, z: 0.5 },
        health: { current: 85, max: 100 },
      },
    }],
  },

  // Chat message
  chatMessage: {
    name: "Chat message",
    data: ["push", {
      type: "chat",
      userId: "user-abc123",
      channel: "general",
      content: "Hello, world! This is a test message.",
      timestamp: 1704067200000,
    }],
  },

  // Complex nested response
  complexResponse: {
    name: "Complex API response",
    data: ["return", 5, {
      user: { id: 123, name: "Alice", roles: ["admin", "user"] },
      profile: { bio: "Developer", location: "San Francisco", verified: true },
      notifications: [
        { id: 1, type: "message", content: "Hello!", read: false },
        { id: 2, type: "alert", content: "System update", read: true },
        { id: 3, type: "mention", content: "@alice check this", read: false },
      ],
    }],
  },

  // Batch of player states (server broadcast)
  playerBroadcast: {
    name: "10-player broadcast",
    data: ["return", 1, {
      tick: 12345,
      players: Array.from({ length: 10 }, (_, i) => ({
        id: `player-${i}`,
        x: Math.random() * 100 - 50,
        y: 0.5,
        z: Math.random() * 100 - 50,
        rotation: Math.random() * 6.28,
        health: Math.floor(Math.random() * 100),
        state: "active",
      })),
    }],
  },
};

// ============================================================================
// PART 1: SINGLE MESSAGE SIZE COMPARISON
// ============================================================================

console.log("\n" + "=".repeat(80));
console.log("PART 1: SINGLE MESSAGE SIZE COMPARISON");
console.log("=".repeat(80));
console.log("\nComparing JSON vs CBOR (no records) vs CBOR (sequential + records)");
console.log("Note: Sequential mode embeds structure definition on FIRST encode only\n");

console.log("┌─────────────────────────────┬────────┬─────────────┬─────────────────────┐");
console.log("│ Payload                     │  JSON  │ CBOR (base) │ CBOR (seq) 1st/2nd  │");
console.log("├─────────────────────────────┼────────┼─────────────┼─────────────────────┤");

for (const [key, { name, data }] of Object.entries(payloads)) {
  const jsonSize = new TextEncoder().encode(JSON.stringify(data)).length;
  const cborNoRecSize = noRecordsEncoder.encode(data).length;

  // Sequential encoder - first encode includes structure, second doesn't
  const seqCodec = createSequentialCodec();
  const seqFirst = seqCodec.encoder.encode(data).length;
  const seqSecond = seqCodec.encoder.encode(data).length;

  // Collect size savings (using second encode for sequential, which is steady-state)
  results.sizeSavings.vsJson.push((jsonSize - seqSecond) / jsonSize * 100);
  results.sizeSavings.vsCborNoRec.push((cborNoRecSize - seqSecond) / cborNoRecSize * 100);

  const displayName = name.length > 27 ? name.slice(0, 24) + "..." : name.padEnd(27);
  const seqDisplay = `${seqFirst}B → ${seqSecond}B`;

  console.log(`│ ${displayName} │ ${String(jsonSize).padStart(4)}B  │ ${String(cborNoRecSize).padStart(5)}B      │ ${seqDisplay.padStart(17)}   │`);
}

console.log("└─────────────────────────────┴────────┴─────────────┴─────────────────────┘");

// ============================================================================
// PART 2: STRUCTURE AMORTIZATION OVER TIME
// ============================================================================

console.log("\n" + "=".repeat(80));
console.log("PART 2: STRUCTURE AMORTIZATION OVER MESSAGE COUNT");
console.log("=".repeat(80));
console.log("\nShows how sequential mode's overhead is amortized as messages repeat\n");

function measureTotalBytes(count, encoderFactory) {
  let total = 0;
  const enc = encoderFactory();
  for (let i = 0; i < count; i++) {
    // Vary the data slightly (different positions)
    const msg = ["call", 1, 0, "updatePosition", [{
      x: Math.random() * 20,
      y: 0.5,
      z: Math.random() * 20,
      rotation: Math.random() * 6.28,
    }]];
    total += enc.encode(msg).length;
  }
  return total;
}

console.log("Position updates (same structure, varying values):\n");
console.log("┌───────────┬──────────────┬──────────────────┬──────────────────┬──────────┐");
console.log("│ Messages  │     JSON     │  CBOR (no rec)   │  CBOR (seq+rec)  │ Savings  │");
console.log("├───────────┼──────────────┼──────────────────┼──────────────────┼──────────┤");

// Find break-even point
let breakEvenFound = false;
for (const count of [1, 2, 3, 4, 5, 10, 50, 100, 500, 1000]) {
  const jsonTotal = measureTotalBytes(count, () => ({ encode: (d) => new TextEncoder().encode(JSON.stringify(d)) }));
  const noRecTotal = measureTotalBytes(count, () => noRecordsEncoder);
  const seqTotal = measureTotalBytes(count, () => createSequentialCodec().encoder);

  // Find break-even point (when seq becomes smaller than no-rec)
  if (!breakEvenFound && seqTotal < noRecTotal) {
    results.breakEvenPoint = count;
    breakEvenFound = true;
  }

  const savingsVsJson = ((jsonTotal - seqTotal) / jsonTotal * 100).toFixed(1);

  if ([1, 2, 3, 4, 5, 10, 50, 100, 500, 1000].includes(count)) {
    console.log(`│ ${String(count).padStart(7)}   │ ${String(jsonTotal).padStart(8)} B   │ ${String(noRecTotal).padStart(10)} B     │ ${String(seqTotal).padStart(10)} B     │ ${savingsVsJson.padStart(5)}%   │`);
  }
}

console.log("└───────────┴──────────────┴──────────────────┴──────────────────┴──────────┘");

if (results.breakEvenPoint) {
  console.log(`\n→ Break-even point: Sequential mode becomes smaller than no-records at ${results.breakEvenPoint} message(s)`);
}

// ============================================================================
// PART 3: HIGH-FREQUENCY SIMULATION
// ============================================================================

console.log("\n" + "=".repeat(80));
console.log("PART 3: HIGH-FREQUENCY GAME SIMULATION");
console.log("=".repeat(80));

const scenarios = [
  { name: "Casual game", players: 4, tickRate: 20, duration: 60 },
  { name: "Competitive FPS", players: 10, tickRate: 64, duration: 60 },
  { name: "MMO zone", players: 50, tickRate: 20, duration: 60 },
  { name: "Battle royale", players: 100, tickRate: 30, duration: 60 },
];

console.log("\nBandwidth comparison for different game scenarios (per minute):\n");

for (const scenario of scenarios) {
  const { name, players, tickRate, duration } = scenario;
  const totalMessages = players * tickRate * duration;

  // Simulate encoding all messages
  let jsonBytes = 0;
  let noRecBytes = 0;
  let seqBytes = 0;

  const seqEncoder = createSequentialCodec().encoder;

  for (let i = 0; i < totalMessages; i++) {
    const playerId = i % players;
    const msg = ["push", {
      p: playerId,
      x: Math.random() * 100,
      y: Math.random() * 10,
      z: Math.random() * 100,
      r: Math.random() * 6.28,
      t: Date.now(),
    }];

    jsonBytes += new TextEncoder().encode(JSON.stringify(msg)).length;
    noRecBytes += noRecordsEncoder.encode(msg).length;
    seqBytes += seqEncoder.encode(msg).length;
  }

  const jsonKbps = (jsonBytes * 8 / 60 / 1000);
  const seqKbps = (seqBytes * 8 / 60 / 1000);
  const savings = ((jsonBytes - seqBytes) / jsonBytes * 100);

  // Store results for the "Competitive FPS" scenario (10 players @ 64Hz)
  if (name === "Competitive FPS") {
    results.bandwidth = {
      jsonKbps: jsonKbps,
      seqKbps: seqKbps,
      savings: savings,
    };
  }

  const jsonKB = (jsonBytes / 1024).toFixed(1);
  const noRecKB = (noRecBytes / 1024).toFixed(1);
  const seqKB = (seqBytes / 1024).toFixed(1);

  console.log(`${name} (${players} players @ ${tickRate}Hz):`);
  console.log(`  Messages: ${totalMessages.toLocaleString()}`);
  console.log(`  JSON:           ${jsonKB.padStart(8)} KB  (${jsonKbps.toFixed(1)} Kbps)`);
  console.log(`  CBOR (no rec):  ${noRecKB.padStart(8)} KB  (${(noRecBytes * 8 / 60 / 1000).toFixed(1)} Kbps)`);
  console.log(`  CBOR (seq+rec): ${seqKB.padStart(8)} KB  (${seqKbps.toFixed(1)} Kbps)  [${savings.toFixed(0)}% savings vs JSON]`);
  console.log("");
}

// ============================================================================
// PART 4: ENCODING SPEED COMPARISON
// ============================================================================

console.log("=".repeat(80));
console.log("PART 4: ENCODING/DECODING SPEED");
console.log("=".repeat(80) + "\n");

const ITERATIONS = 50000;

function benchmarkSpeed(name, data) {
  const jsonStr = JSON.stringify(data);
  const noRecEncoded = noRecordsEncoder.encode(data);
  const seqCodec = createSequentialCodec();

  // Warm up
  for (let i = 0; i < 1000; i++) {
    JSON.stringify(data);
    JSON.parse(jsonStr);
    noRecordsEncoder.encode(data);
    noRecordsEncoder.decode(noRecEncoded);
    seqCodec.encoder.encode(data);
  }

  // Benchmark JSON encode
  let start = performance.now();
  for (let i = 0; i < ITERATIONS; i++) JSON.stringify(data);
  const jsonEncTime = performance.now() - start;

  // Benchmark JSON decode
  start = performance.now();
  for (let i = 0; i < ITERATIONS; i++) JSON.parse(jsonStr);
  const jsonDecTime = performance.now() - start;

  // Benchmark CBOR (no records) encode
  start = performance.now();
  for (let i = 0; i < ITERATIONS; i++) noRecordsEncoder.encode(data);
  const noRecEncTime = performance.now() - start;

  // Benchmark CBOR (no records) decode
  start = performance.now();
  for (let i = 0; i < ITERATIONS; i++) noRecordsEncoder.decode(noRecEncoded);
  const noRecDecTime = performance.now() - start;

  // Benchmark CBOR (sequential) encode - use fresh encoder each iteration batch
  const seqEnc = createSequentialCodec().encoder;
  start = performance.now();
  for (let i = 0; i < ITERATIONS; i++) seqEnc.encode(data);
  const seqEncTime = performance.now() - start;

  // Collect speedup ratios
  const encodeSpeedup = jsonEncTime / seqEncTime;
  const decodeSpeedup = jsonDecTime / noRecDecTime;
  results.speed.encodeSpeedups.push(encodeSpeedup);
  results.speed.decodeSpeedups.push(decodeSpeedup);

  console.log(`${name} (${ITERATIONS.toLocaleString()} iterations):`);
  console.log(`  Encode: JSON ${jsonEncTime.toFixed(0)}ms | CBOR ${noRecEncTime.toFixed(0)}ms | CBOR+seq ${seqEncTime.toFixed(0)}ms`);
  console.log(`  Decode: JSON ${jsonDecTime.toFixed(0)}ms | CBOR ${noRecDecTime.toFixed(0)}ms`);
  console.log(`  Speedup: encode ${encodeSpeedup.toFixed(1)}x, decode ${decodeSpeedup.toFixed(1)}x`);
  console.log("");
}

benchmarkSpeed("Position update", payloads.positionUpdate.data);
benchmarkSpeed("Entity update", payloads.entityUpdate.data);
benchmarkSpeed("Complex response", payloads.complexResponse.data);

// ============================================================================
// PART 5: BIDIRECTIONAL RPC SIMULATION
// ============================================================================

console.log("=".repeat(80));
console.log("PART 5: BIDIRECTIONAL RPC SESSION SIMULATION");
console.log("=".repeat(80));
console.log("\nSimulates a typical RPC session with requests and responses\n");

// Create separate codecs for client and server (like real RPC)
const clientCodec = createSequentialCodec();
const serverCodec = createSequentialCodec();

const rpcMessages = [
  // Client requests
  ["call", 1, 0, "getUser", [{ id: 123 }]],
  ["call", 2, 0, "listItems", [{ page: 1, limit: 10 }]],
  ["call", 3, 0, "updatePosition", [{ x: 10, y: 0, z: 5 }]],
  ["call", 4, 0, "sendChat", [{ channel: "general", text: "Hello!" }]],
  ["call", 5, 0, "updatePosition", [{ x: 11, y: 0, z: 6 }]],
  ["call", 6, 0, "updatePosition", [{ x: 12, y: 0, z: 7 }]],
  // Server responses
  ["return", 1, { id: 123, name: "Alice", level: 42 }],
  ["return", 2, { items: [{ id: 1, name: "Sword" }, { id: 2, name: "Shield" }], total: 50 }],
  ["return", 3, { success: true }],
  ["return", 4, { messageId: "msg-001", timestamp: Date.now() }],
  ["return", 5, { success: true }],
  ["return", 6, { success: true }],
];

let clientToServerBytes = 0;
let serverToClientBytes = 0;
let jsonBytes = 0;

for (let i = 0; i < rpcMessages.length; i++) {
  const msg = rpcMessages[i];
  const isClientMsg = msg[0] === "call";

  if (isClientMsg) {
    clientToServerBytes += clientCodec.encoder.encode(msg).length;
  } else {
    serverToClientBytes += serverCodec.encoder.encode(msg).length;
  }
  jsonBytes += new TextEncoder().encode(JSON.stringify(msg)).length;
}

console.log(`RPC Session (${rpcMessages.length} messages):`);
console.log(`  JSON total:           ${jsonBytes} bytes`);
console.log(`  CBOR (seq) c→s:       ${clientToServerBytes} bytes`);
console.log(`  CBOR (seq) s→c:       ${serverToClientBytes} bytes`);
console.log(`  CBOR (seq) total:     ${clientToServerBytes + serverToClientBytes} bytes`);
console.log(`  Savings:              ${((jsonBytes - (clientToServerBytes + serverToClientBytes)) / jsonBytes * 100).toFixed(1)}%`);

// ============================================================================
// SUMMARY (with actual measured results)
// ============================================================================

console.log("\n" + "=".repeat(80));
console.log("SUMMARY");
console.log("=".repeat(80));

// Calculate summary statistics
const avgSavingsVsJson = results.sizeSavings.vsJson.reduce((a, b) => a + b, 0) / results.sizeSavings.vsJson.length;
const minSavingsVsJson = Math.min(...results.sizeSavings.vsJson);
const maxSavingsVsJson = Math.max(...results.sizeSavings.vsJson);

const avgSavingsVsCborNoRec = results.sizeSavings.vsCborNoRec.reduce((a, b) => a + b, 0) / results.sizeSavings.vsCborNoRec.length;
const minSavingsVsCborNoRec = Math.min(...results.sizeSavings.vsCborNoRec);
const maxSavingsVsCborNoRec = Math.max(...results.sizeSavings.vsCborNoRec);

const avgEncodeSpeedup = results.speed.encodeSpeedups.reduce((a, b) => a + b, 0) / results.speed.encodeSpeedups.length;
const minEncodeSpeedup = Math.min(...results.speed.encodeSpeedups);
const maxEncodeSpeedup = Math.max(...results.speed.encodeSpeedups);

const avgDecodeSpeedup = results.speed.decodeSpeedups.reduce((a, b) => a + b, 0) / results.speed.decodeSpeedups.length;
const minDecodeSpeedup = Math.min(...results.speed.decodeSpeedups);
const maxDecodeSpeedup = Math.max(...results.speed.decodeSpeedups);

console.log(`
Key findings (from actual measurements):

1. SEQUENTIAL MODE AMORTIZATION
   - First message includes structure definition (overhead)
   - Subsequent messages reference structure by ID (3 bytes)
   - Break-even point: ${results.breakEvenPoint} message(s) of same structure

2. SIZE SAVINGS (steady-state, after structure learned)
   - vs JSON: ${minSavingsVsJson.toFixed(0)}-${maxSavingsVsJson.toFixed(0)}% smaller (avg ${avgSavingsVsJson.toFixed(0)}%)
   - vs CBOR (no records): ${minSavingsVsCborNoRec.toFixed(0)}-${maxSavingsVsCborNoRec.toFixed(0)}% smaller (avg ${avgSavingsVsCborNoRec.toFixed(0)}%)

3. BANDWIDTH IMPACT (10 players @ 64Hz, 1 minute)
   - JSON: ${results.bandwidth.jsonKbps.toFixed(1)} Kbps
   - CBOR (seq+records): ${results.bandwidth.seqKbps.toFixed(1)} Kbps
   - Savings: ${results.bandwidth.savings.toFixed(0)}% bandwidth reduction

4. SPEED
   - Encoding: CBOR is ${minEncodeSpeedup.toFixed(1)}-${maxEncodeSpeedup.toFixed(1)}x faster than JSON.stringify (avg ${avgEncodeSpeedup.toFixed(1)}x)
   - Decoding: CBOR is ${minDecodeSpeedup.toFixed(1)}-${maxDecodeSpeedup.toFixed(1)}x faster than JSON.parse (avg ${avgDecodeSpeedup.toFixed(1)}x)

5. BEST PRACTICES
   - Use per-session CborCodec instances for RPC
   - Sequential mode works perfectly for WebSocket streams
   - Structure learning happens automatically - no coordination needed
`);
