// Test script to verify varuint buffer overflow fix
const { parse } = require('./dist/api.js');

// Create a buffer that's exactly 107 bytes with a varuint at the end
// This simulates the corrupted data scenario
const buffer = Buffer.alloc(107);
// Set some data in the buffer
buffer.writeUInt8(0x00, 0);
buffer.writeUInt8(0x01, 1);

// Try to create a scenario where varuint reading would fail
// Put a varuint marker at position 105 (leaving only 2 bytes)
buffer.writeUInt8(0x80, 105); // High bit set, indicating 4-byte varuint
buffer.writeUInt8(0x00, 106);
// No bytes at 107 and 108, which should cause the buffer overflow

try {
    // This should now give a better error message
    const result = parse.object.read(buffer, {});
    console.log("Result:", result);
} catch (error) {
    console.log("Caught error:", error.message);
    console.log("Error contains improved message:", error.message.includes("Need at least 2 bytes"));
}
