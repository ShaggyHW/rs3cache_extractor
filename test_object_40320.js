// Test script to verify object 40320 parsing with unknown opcode 0xFF
const { parse } = require('./dist/api.js');

async function testObject40320() {
    try {
        console.log("Testing object 40320 parsing...");
        
        // Create a mock cache engine
        const mockEngine = {
            getGameFile: async (type, id) => {
                if (type === "objects" && id === 40320) {
                    // Return a buffer with opcode 0xFF to simulate the issue
                    const buffer = Buffer.alloc(10);
                    buffer.writeUInt8(0xFF, 0); // Unknown opcode
                    buffer.writeUInt8(0x00, 1); // Terminator
                    return buffer;
                }
                throw new Error(`File not found: ${type} ${id}`);
            }
        };
        
        // Try to parse the object
        const result = parse.object.read(mockEngine.getGameFile("objects", 40320), mockEngine);
        console.log("Successfully parsed object 40320:", result);
        
    } catch (error) {
        console.log("Caught error:", error.message);
        console.log("Error contains 'unknown chunk':", error.message.includes('unknown chunk'));
        console.log("Error contains '0xFF':", error.message.includes('0xFF'));
    }
}

testObject40320();
