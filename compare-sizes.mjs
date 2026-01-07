import { Encoder } from 'cbor-x';

const encoder = new Encoder({
  encodeUndefinedAsNil: false,
  useRecords: false,
  tagUint8Array: true,
});

// Typical RPC message structures
const testCases = [
  {
    name: "Simple method call",
    data: ["call", 1, 0, "square", [5]]
  },
  {
    name: "Method call with object arg",
    data: ["call", 2, 0, "createUser", [{ name: "John Doe", email: "john@example.com", age: 30 }]]
  },
  {
    name: "Response with simple value",
    data: ["return", 1, 25]
  },
  {
    name: "Response with object",
    data: ["return", 2, { id: 12345, name: "John Doe", email: "john@example.com", created: "2024-01-15T10:30:00Z" }]
  },
  {
    name: "Response with array of objects",
    data: ["return", 3, [
      { id: 1, title: "Notification 1", read: false },
      { id: 2, title: "Notification 2", read: true },
      { id: 3, title: "Notification 3", read: false },
    ]]
  },
  {
    name: "Error response",
    data: ["throw", 4, ["Error", "Something went wrong", null]]
  },
  {
    name: "Complex nested structure",
    data: ["return", 5, {
      user: { id: 123, name: "Alice", roles: ["admin", "user"] },
      profile: { bio: "Software developer", location: "San Francisco", skills: ["JavaScript", "TypeScript", "Rust"] },
      notifications: [
        { id: 1, type: "message", content: "Hello!" },
        { id: 2, type: "alert", content: "System update" },
      ]
    }]
  },
  {
    name: "Binary data (16 bytes)",
    data: ["return", 6, ["bytes", new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])]]
  },
  {
    name: "Large binary data (1KB)",
    data: ["return", 7, ["bytes", new Uint8Array(1024).fill(42)]]
  },
];

console.log("Message Size Comparison: CBOR vs JSON\n");
console.log("=".repeat(70));
console.log("");

let totalJson = 0;
let totalCbor = 0;

for (const { name, data } of testCases) {
  // For JSON, we need to handle Uint8Array specially (base64 encode)
  const jsonData = JSON.stringify(data, (key, value) => {
    if (value instanceof Uint8Array) {
      // Base64 encode binary data for JSON
      let binary = '';
      for (let i = 0; i < value.length; i++) {
        binary += String.fromCharCode(value[i]);
      }
      return btoa(binary);
    }
    return value;
  });
  const jsonSize = new TextEncoder().encode(jsonData).length;

  const cborData = encoder.encode(data);
  const cborSize = cborData.length;

  const savings = ((jsonSize - cborSize) / jsonSize * 100).toFixed(1);
  const ratio = (jsonSize / cborSize).toFixed(2);

  totalJson += jsonSize;
  totalCbor += cborSize;

  console.log(`${name}:`);
  console.log(`  JSON: ${jsonSize.toString().padStart(5)} bytes`);
  console.log(`  CBOR: ${cborSize.toString().padStart(5)} bytes`);
  console.log(`  Savings: ${savings}% (${ratio}x smaller)`);
  console.log("");
}

console.log("=".repeat(70));
console.log(`TOTAL:`);
console.log(`  JSON: ${totalJson} bytes`);
console.log(`  CBOR: ${totalCbor} bytes`);
console.log(`  Overall savings: ${((totalJson - totalCbor) / totalJson * 100).toFixed(1)}%`);
