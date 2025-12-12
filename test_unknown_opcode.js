// Test script to verify unknown opcode handling
const { FileParser } = require('./dist/opdecoder.js');

// Create a simple test with unknown opcode 0xFF
function testUnknownOpcode() {
    try {
        console.log("Testing unknown opcode 0xFF handling...");
        
        // Create a buffer with opcode 0xFF followed by terminator 0x00
        const buffer = Buffer.from([0xFF, 0x00]);
        
        // Create a simple parser that would fail on unknown opcode
        const parser = new FileParser({
            "0x01": { "name": "test", "read": "byte" }
        }, {});
        
        // Try to parse the buffer
        const result = parser.read(buffer, {});
        console.log("Successfully parsed with unknown opcode:", result);
        console.log("Test PASSED: Unknown opcode was handled gracefully");
        
    } catch (error) {
        console.log("Caught error:", error.message);
        if (error.message.includes('unknown chunk')) {
            console.log("Test FAILED: Unknown opcode still throws error");
        } else {
            console.log("Test FAILED with different error:", error.message);
        }
    }
}

testUnknownOpcode();
