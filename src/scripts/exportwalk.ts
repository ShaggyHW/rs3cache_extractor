import { CacheFileSource } from "../cache";
import { ScriptFS, ScriptOutput } from "../scriptrunner";
import { EngineCache } from "../3d/modeltothree";
import { parseMapsquare, squareLevels, rs2ChunkSize, classicChunkSize, TileGrid } from "../3d/mapsquare";

export async function exportWalkFlags(output: ScriptOutput, save: ScriptFS, source: CacheFileSource, startx = 0, startz = 0, sizex = 128, sizez = 200) {
	const engine = await EngineCache.create(source);
	const chunkSize = (engine.classicData ? classicChunkSize : rs2ChunkSize);
	await save.mkDir("walk");

	const processSquare = async (cx: number, cz: number) => {
		const parsed = await parseMapsquare(engine, cx, cz, { collision: true, map2d: true, minimap: true, hashboxes: true, skybox: true, invisibleLayers: true, padfloor: true });
		if (!parsed.chunk) { return; }

		const grid = parsed.grid as TileGrid;
		const rect = parsed.chunk.tilerect;
		const tiles: any[] = [];

		for (let plane = 0; plane < squareLevels; plane++) {
			for (let dz = 0; dz < rect.zsize; dz++) {
				for (let dx = 0; dx < rect.xsize; dx++) {
					const gx = rect.x + dx;
					const gz = rect.z + dz;
					const tile = grid.getTile(gx, gz, plane);
					if (!tile || !tile.effectiveCollision) { continue; }
					const col = tile.effectiveCollision;

					const centerBlocked = !!col.walk[0];
					let allowedMask = 0;
					let blockedMask = 0;
					const dirMap: number[] = [1, 2, 3, 4, 5, 6, 7, 8];
					// deltas aligned with dirMap order, and opposite-direction indices
					const deltas = [
						{ dx: -1, dz: 0, opp: 3 },
						{ dx: 0, dz: -1, opp: 4 },
						{ dx: 1, dz: 0, opp: 1 },
						{ dx: 0, dz: 1, opp: 2 },
						{ dx: -1, dz: 1, opp: 7 },
						{ dx: -1, dz: -1, opp: 8 },
						{ dx: 1, dz: -1, opp: 5 },
						{ dx: 1, dz: 1, opp: 6 }
					];
					const alloweds: boolean[] = new Array(8).fill(false);
					const diagonalDeps: number[][] = [[], [], [], [], [0, 3], [0, 1], [2, 1], [2, 3]];
					for (let i = 0; i < dirMap.length; i++) {
						const idx = dirMap[i];
						const bit = (1 << i);
						let blocked = !!col.walk[idx];
						const d = deltas[i];
						const neigh = grid.getTile(gx + d.dx, gz + d.dz, plane);
						const ncol = neigh?.effectiveCollision;
						if (ncol) {
							if (ncol.walk[0] || ncol.walk[d.opp]) { blocked = true; }
						}
						if (!blocked) {
							const deps = diagonalDeps[i];
							if (deps.length) {
								for (const dep of deps) {
									if (!alloweds[dep]) { blocked = true; break; }
								}
							}
						}
						if (blocked) { blockedMask |= bit; } else { allowedMask |= bit; alloweds[i] = true; }
					}

					tiles.push({
						x: gx,
						y: gz,
						plane,
						flag: tile.settings | 0,
						blocked: centerBlocked,
						walkMask: allowedMask,
						blockedMask,
						walk: {
							left: alloweds[0],
							bottom: alloweds[1],
							right: alloweds[2],
							top: alloweds[3],
							topleft: alloweds[4],
							bottomleft: alloweds[5],
							bottomright: alloweds[6],
							topright: alloweds[7]
						}
					});
				}
			}
		}

		const out = JSON.stringify({ chunk: { x: cx, z: cz, chunkSize }, tiles });
		await save.writeFile(`walk/${cx}-${cz}.json`, out);
		output.log("walkflags:", cx, cz, "tiles:", tiles.length);
	};

	const coords: { x: number, z: number }[] = [];
	for (let cx = startx; cx < startx + sizex; cx++) {
		for (let cz = startz; cz < startz + sizez; cz++) {
			coords.push({ x: cx, z: cz });
		}
	}
	if (!coords.length) { return; }

	const concurrencyValue = Number(process.env.EXPORT_WALK_CONCURRENCY);
	const maxConcurrency = Number.isFinite(concurrencyValue) && concurrencyValue > 0 ? Math.floor(concurrencyValue) : 200;
	let taskIndex = 0;
	const worker = async (): Promise<void> => {
		while (true) {
			const currentIndex = taskIndex;
			taskIndex += 1;
			if (currentIndex >= coords.length) { return; }
			const coord = coords[currentIndex];
			await processSquare(coord.x, coord.z);
		}
	};
	const workerCount = Math.min(maxConcurrency, coords.length);
	await Promise.all(Array.from({ length: workerCount }, () => worker()));
}
