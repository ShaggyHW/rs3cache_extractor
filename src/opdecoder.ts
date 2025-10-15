// import * as fs from "fs";
import * as opcode_reader from "./opcode_reader";
import commentJson from "comment-json";
import type { CacheFileSource } from "cache";
import { readFileSync } from "fs";
import path from "path";

const readText = (relPath: string) => {
    try {
        const filePath = path.resolve(__dirname, relPath.replace(/^\.\//, ""));
        return readFileSync(filePath, "utf8");
    } catch (e1) {
        return readFileSync(require.resolve(relPath), "utf8");
    }
};
const typedef = commentJson.parse(readText("./opcodes/typedef.jsonc")) as any;

//alloc a large static buffer to write data to without knowing the data size
//then copy what we need out of it
//the buffer is reused so it saves a ton of buffer allocs
const scratchbuf = Buffer.alloc(2 * 1024 * 1024);

let bytesleftoverwarncount = 0;

export class FileParser<T> {
	parser: opcode_reader.ChunkParser;
	originalSource: string;
	totaltime = 0;

	static fromJson<T>(jsonObject: string) {
		let opcodeobj = commentJson.parse(jsonObject, undefined, true) as any
		return new FileParser<T>(opcodeobj, jsonObject);
	}

	constructor(opcodeobj: unknown, originalSource?: string) {
		this.parser = opcode_reader.buildParser(null, opcodeobj, typedef as any);
		this.originalSource = originalSource ?? JSON.stringify(opcodeobj, undefined, "\t");
	}

	readInternal(state: opcode_reader.DecodeState) {
		let t = performance.now();
		let res = this.parser.read(state);
		this.totaltime += performance.now() - t;
		if (state.scan != state.endoffset) {
			bytesleftoverwarncount++;
			if (bytesleftoverwarncount < 100) {
				console.log(`bytes left over after decoding file: ${state.endoffset - state.scan}`);
				// let name = `cache/bonusbytes-${Date.now()}.bin`;
				// require("fs").writeFileSync(name, scanbuf.slice(scanbuf.scan));
			}
			if (bytesleftoverwarncount == 100) {
				console.log("too many bytes left over warning, no more warnings will be logged");
			}
			// TODO remove this stupid condition, needed this to fail only in some situations
			if (state.buffer.byteLength < 100000) {
				throw new Error(`bytes left over after decoding file: ${state.endoffset - state.scan}`);
			}
		}
		return res;
	}

	read(buffer: Buffer, source: CacheFileSource, args?: Record<string, any>) {
		let state: opcode_reader.DecodeState = {
			isWrite: false,
			buffer,
			stack: [],
			hiddenstack: [],
			scan: 0,
			endoffset: buffer.byteLength,
			args: {
				...source.getDecodeArgs(),
				...args
			}
		};
		return this.readInternal(state) as T;
	}

	write(obj: T, args?: Record<string, any>) {
		let state: opcode_reader.EncodeState = {
			isWrite: true,
			stack: [],
			hiddenstack: [],
			buffer: scratchbuf,
			scan: 0,
			endoffset: scratchbuf.byteLength,
			args: {
				clientVersion: 1000,//TODO
				...args
			}
		};
		this.parser.write(state, obj);
		if (state.scan > state.endoffset) { throw new Error("tried to write file larger than scratchbuffer size"); }
		//append footer data to end of normal data
		state.buffer.copyWithin(state.scan, state.endoffset, scratchbuf.byteLength);
		state.scan += scratchbuf.byteLength - state.endoffset;
		//do the weird prototype slice since we need a copy, not a ref
		let r: Buffer = Uint8Array.prototype.slice.call(scratchbuf, 0, state.scan);
		//clear it for next use
		scratchbuf.fill(0, 0, state.scan);
		return r;
	}
}

globalThis.parserTimings = () => {
	let all = Object.entries(parse).map(q => ({ name: q[0], t: q[1].totaltime }));
	all.sort((a, b) => b.t - a.t);
	all.slice(0, 10).filter(q => q.t > 0.01).forEach(q => console.log(`${q.name} ${q.t.toFixed(3)}s`));
}

export const parse = allParsers();
function allParsers() {
    return {
        cacheIndex: FileParser.fromJson<import("../generated/cacheindex").cacheindex>(readText("./opcodes/cacheindex.json")),
        npc: FileParser.fromJson<import("../generated/npcs").npcs>(readText("./opcodes/npcs.jsonc")),
        item: FileParser.fromJson<import("../generated/items").items>(readText("./opcodes/items.jsonc")),
        object: FileParser.fromJson<import("../generated/objects").objects>(readText("./opcodes/objects.jsonc")),
        achievement: FileParser.fromJson<import("../generated/achievements").achievements>(readText("./opcodes/achievements.jsonc")),
        mapsquareTiles: FileParser.fromJson<import("../generated/mapsquare_tiles").mapsquare_tiles>(readText("./opcodes/mapsquare_tiles.jsonc")),
        mapsquareTilesNxt: FileParser.fromJson<import("../generated/mapsquare_tiles_nxt").mapsquare_tiles_nxt>(readText("./opcodes/mapsquare_tiles_nxt.jsonc")),
        mapsquareWaterTiles: FileParser.fromJson<import("../generated/mapsquare_watertiles").mapsquare_watertiles>(readText("./opcodes/mapsquare_watertiles.json")),
        mapsquareUnderlays: FileParser.fromJson<import("../generated/mapsquare_underlays").mapsquare_underlays>(readText("./opcodes/mapsquare_underlays.jsonc")),
        mapsquareOverlays: FileParser.fromJson<import("../generated/mapsquare_overlays").mapsquare_overlays>(readText("./opcodes/mapsquare_overlays.jsonc")),
        mapsquareLocations: FileParser.fromJson<import("../generated/mapsquare_locations").mapsquare_locations>(readText("./opcodes/mapsquare_locations.json")),
        mapsquareEnvironment: FileParser.fromJson<import("../generated/mapsquare_envs").mapsquare_envs>(readText("./opcodes/mapsquare_envs.jsonc")),
        mapZones: FileParser.fromJson<import("../generated/mapzones").mapzones>(readText("./opcodes/mapzones.json")),
        enums: FileParser.fromJson<import("../generated/enums").enums>(readText("./opcodes/enums.json")),
        mapscenes: FileParser.fromJson<import("../generated/mapscenes").mapscenes>(readText("./opcodes/mapscenes.json")),
        sequences: FileParser.fromJson<import("../generated/sequences").sequences>(readText("./opcodes/sequences.json")),
        framemaps: FileParser.fromJson<import("../generated/framemaps").framemaps>(readText("./opcodes/framemaps.jsonc")),
        frames: FileParser.fromJson<import("../generated/frames").frames>(readText("./opcodes/frames.json")),
        animgroupConfigs: FileParser.fromJson<import("../generated/animgroupconfigs").animgroupconfigs>(readText("./opcodes/animgroupconfigs.jsonc")),
        models: FileParser.fromJson<import("../generated/models").models>(readText("./opcodes/models.jsonc")),
        oldmodels: FileParser.fromJson<import("../generated/oldmodels").oldmodels>(readText("./opcodes/oldmodels.jsonc")),
        classicmodels: FileParser.fromJson<import("../generated/classicmodels").classicmodels>(readText("./opcodes/classicmodels.jsonc")),
        spotAnims: FileParser.fromJson<import("../generated/spotanims").spotanims>(readText("./opcodes/spotanims.json")),
        rootCacheIndex: FileParser.fromJson<import("../generated/rootcacheindex").rootcacheindex>(readText("./opcodes/rootcacheindex.jsonc")),
        skeletalAnim: FileParser.fromJson<import("../generated/skeletalanim").skeletalanim>(readText("./opcodes/skeletalanim.jsonc")),
        materials: FileParser.fromJson<import("../generated/materials").materials>(readText("./opcodes/materials.jsonc")),
        oldmaterials: FileParser.fromJson<import("../generated/oldmaterials").oldmaterials>(readText("./opcodes/oldmaterials.jsonc")),
        quickchatCategories: FileParser.fromJson<import("../generated/quickchatcategories").quickchatcategories>(readText("./opcodes/quickchatcategories.jsonc")),
        quickchatLines: FileParser.fromJson<import("../generated/quickchatlines").quickchatlines>(readText("./opcodes/quickchatlines.jsonc")),
        environments: FileParser.fromJson<import("../generated/environments").environments>(readText("./opcodes/environments.jsonc")),
        avatars: FileParser.fromJson<import("../generated/avatars").avatars>(readText("./opcodes/avatars.jsonc")),
        avatarOverrides: FileParser.fromJson<import("../generated/avataroverrides").avataroverrides>(readText("./opcodes/avataroverrides.jsonc")),
        identitykit: FileParser.fromJson<import("../generated/identitykit").identitykit>(readText("./opcodes/identitykit.jsonc")),
        structs: FileParser.fromJson<import("../generated/structs").structs>(readText("./opcodes/structs.jsonc")),
        params: FileParser.fromJson<import("../generated/params").params>(readText("./opcodes/params.jsonc")),
        particles_0: FileParser.fromJson<import("../generated/particles_0").particles_0>(readText("./opcodes/particles_0.jsonc")),
        particles_1: FileParser.fromJson<import("../generated/particles_1").particles_1>(readText("./opcodes/particles_1.jsonc")),
        audio: FileParser.fromJson<import("../generated/audio").audio>(readText("./opcodes/audio.jsonc")),
        proctexture: FileParser.fromJson<import("../generated/proctexture").proctexture>(readText("./opcodes/proctexture.jsonc")),
        oldproctexture: FileParser.fromJson<import("../generated/oldproctexture").oldproctexture>(readText("./opcodes/oldproctexture.jsonc")),
        maplabels: FileParser.fromJson<import("../generated/maplabels").maplabels>(readText("./opcodes/maplabels.jsonc")),
        cutscenes: FileParser.fromJson<import("../generated/cutscenes").cutscenes>(readText("./opcodes/cutscenes.jsonc")),
        clientscript: FileParser.fromJson<import("../generated/clientscript").clientscript>(readText("./opcodes/clientscript.jsonc")),
        clientscriptdata: FileParser.fromJson<import("../generated/clientscriptdata").clientscriptdata>(readText("./opcodes/clientscriptdata.jsonc")),
        interfaces: FileParser.fromJson<import("../generated/interfaces").interfaces>(readText("./opcodes/interfaces.jsonc")),
        dbtables: FileParser.fromJson<import("../generated/dbtables").dbtables>(readText("./opcodes/dbtables.jsonc")),
        dbrows: FileParser.fromJson<import("../generated/dbrows").dbrows>(readText("./opcodes/dbrows.jsonc"))
    }
}