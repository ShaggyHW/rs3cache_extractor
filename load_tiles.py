#!/usr/bin/env python3
import os
import json
import sqlite3

# Folder containing your JSON files
JSON_FOLDER = os.path.join(os.path.dirname(__file__), "out", "walk")
DB_FILE = "tiles.db"

def _table_has_columns(cur, table_name, required_columns):
    cur.execute(f"PRAGMA table_info({table_name})")
    existing = {row[1] for row in cur.fetchall()}
    return required_columns.issubset(existing)

def _cluster_intraconnections_requires_migration(cur):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cluster_intraconnections'")
    if not cur.fetchone():
        return False
    cur.execute("PRAGMA table_info(cluster_intraconnections)")
    info = cur.fetchall()
    present = {row[1] for row in info}
    required = {"chunk_x_from", "chunk_z_from", "plane_from", "entrance_from", "entrance_to", "cost", "path_blob"}
    if not required.issubset(present):
        return True
    cur.execute("PRAGMA foreign_key_list(cluster_intraconnections)")
    fk_targets = [row[2] for row in cur.fetchall() if row[2]]
    return fk_targets.count("cluster_entrances") < 2

def _cluster_interconnections_requires_migration(cur):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cluster_interconnections'")
    if not cur.fetchone():
        return False
    cur.execute("PRAGMA table_info(cluster_interconnections)")
    info = cur.fetchall()
    pk_map = {row[1]: row[5] for row in info}
    if pk_map.get("entrance_from") is None or pk_map.get("entrance_to") is None:
        return True
    cur.execute("PRAGMA foreign_key_list(cluster_interconnections)")
    fk_map = {row[3]: row[2] for row in cur.fetchall()}
    return fk_map.get("entrance_from") != "cluster_entrances" or fk_map.get("entrance_to") != "cluster_entrances"

def create_tables(conn):
    cur = conn.cursor()
    conn.execute("PRAGMA foreign_keys = ON")
    tiles_columns = {"x", "y", "plane", "chunk_x", "chunk_z", "flag", "blocked", "walk_mask", "blocked_mask", "walk_data"}
    chunks_columns = {"chunk_x", "chunk_z", "chunk_size", "tile_count"}

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tiles'")
    if cur.fetchone() and not _table_has_columns(cur, "tiles", tiles_columns):
        cur.execute("DROP TABLE tiles")

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='chunks'")
    if cur.fetchone() and not _table_has_columns(cur, "chunks", chunks_columns):
        cur.execute("DROP TABLE chunks")

    if _cluster_intraconnections_requires_migration(cur):
        cur.execute("""
            CREATE TABLE cluster_intraconnections_new (
              chunk_x_from  INTEGER NOT NULL,
              chunk_z_from  INTEGER NOT NULL,
              plane_from    INTEGER NOT NULL,
              entrance_from INTEGER NOT NULL,
              entrance_to   INTEGER NOT NULL,
              cost          INTEGER NOT NULL,
              path_blob     BLOB,
              PRIMARY KEY (chunk_x_from, chunk_z_from, plane_from, entrance_from, entrance_to),
              FOREIGN KEY (entrance_from) REFERENCES cluster_entrances(entrance_id),
              FOREIGN KEY (entrance_to) REFERENCES cluster_entrances(entrance_id)
            )
        """)
        cur.execute("""
            INSERT INTO cluster_intraconnections_new
              (chunk_x_from, chunk_z_from, plane_from, entrance_from, entrance_to, cost, path_blob)
            SELECT chunk_x_from, chunk_z_from, plane_from, entrance_from, entrance_to, cost, path_blob
            FROM cluster_intraconnections
        """)
        cur.execute("DROP TABLE cluster_intraconnections")
        cur.execute("ALTER TABLE cluster_intraconnections_new RENAME TO cluster_intraconnections")

    if _cluster_interconnections_requires_migration(cur):
        cur.execute("""
            CREATE TABLE cluster_interconnections_new (
              entrance_from INTEGER NOT NULL,
              entrance_to   INTEGER NOT NULL,
              cost          INTEGER NOT NULL,
              PRIMARY KEY (entrance_from, entrance_to),
              FOREIGN KEY (entrance_from) REFERENCES cluster_entrances(entrance_id),
              FOREIGN KEY (entrance_to) REFERENCES cluster_entrances(entrance_id)
            )
        """)
        cur.execute("""
            INSERT INTO cluster_interconnections_new
              (entrance_from, entrance_to, cost)
            SELECT entrance_from, entrance_to, cost
            FROM cluster_interconnections
        """)
        cur.execute("DROP TABLE cluster_interconnections")
        cur.execute("ALTER TABLE cluster_interconnections_new RENAME TO cluster_interconnections")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS tiles (
            x INTEGER,
            y INTEGER,
            plane INTEGER,
            chunk_x INTEGER,
            chunk_z INTEGER,
            flag INTEGER,
            blocked INTEGER,
            walk_mask INTEGER,
            blocked_mask INTEGER,
            walk_data TEXT,
            FOREIGN KEY (chunk_x, chunk_z) REFERENCES chunks(chunk_x, chunk_z),
            PRIMARY KEY (x, y, plane)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS chunks (
            chunk_x INTEGER,
            chunk_z INTEGER,
            chunk_size INTEGER,
            tile_count INTEGER,
            PRIMARY KEY (chunk_x, chunk_z)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chunk_clusters (
          cluster_id INTEGER PRIMARY KEY,
          chunk_x    INTEGER NOT NULL,
          chunk_z    INTEGER NOT NULL,
          plane      INTEGER NOT NULL,
          label      INTEGER,
          tile_count INTEGER,
          FOREIGN KEY (chunk_x, chunk_z) REFERENCES chunks(chunk_x, chunk_z)
        )
    """)
    cur.execute("""
        CREATE TABLE "teleports_door_nodes" (
	"id"	INTEGER,
	"direction"	REAL,
	"real_id_open"	INTEGER,
	"real_id_closed"	INTEGER,
	"location_open_x"	INTEGER,
	"location_open_y"	INTEGER,
	"location_open_plane"	INTEGER,
	"location_closed_x"	INTEGER,
	"location_closed_y"	INTEGER,
	"location_closed_plane"	INTEGER,
	"tile_inside_x"	INTEGER,
	"tile_inside_y"	INTEGER,
	"tile_inside_plane"	INTEGER,
	"tile_outside_x"	INTEGER,
	"tile_outside_y"	INTEGER,
	"tile_outside_plane"	INTEGER,
	"open_action"	TEXT,
	"cost"	INTEGER,
	"next_node_type"	REAL,
	"next_node_id"	REAL,
	"requirement_id"	REAL
);
    """)
    cur.execute("""
        CREATE TABLE "teleports_ifslot_nodes" (
	"id"	INTEGER,
	"interface_id"	INTEGER,
	"component_id"	INTEGER,
	"slot_id"	INTEGER,
	"click_id"	INTEGER,
	"dest_min_x"	REAL,
	"dest_max_x"	REAL,
	"dest_min_y"	REAL,
	"dest_max_y"	REAL,
	"dest_plane"	REAL,
	"cost"	INTEGER,
	"next_node_type"	TEXT,
	"next_node_id"	REAL,
	"requirement_id"	REAL
);
    """)
    cur.execute("""
     CREATE TABLE "teleports_item_nodes" (
	"id"	INTEGER,
	"item_id"	INTEGER,
	"action"	TEXT,
	"dest_min_x"	INTEGER,
	"dest_max_x"	INTEGER,
	"dest_min_y"	INTEGER,
	"dest_max_y"	INTEGER,
	"dest_plane"	INTEGER,
	"next_node_type"	REAL,
	"next_node_id"	REAL,
	"cost"	INTEGER,
	"requirement_id"	INTEGER
);
    """)

    cur.execute("""
   CREATE TABLE "teleports_lodestone_nodes" (
	"id"	INTEGER,
	"lodestone"	TEXT,
	"dest_x"	INTEGER,
	"dest_y"	INTEGER,
	"dest_plane"	INTEGER,
	"cost"	INTEGER,
	"next_node_type"	REAL,
	"next_node_id"	REAL,
	"requirement_id"	REAL
);
    """)
    cur.execute("""
   CREATE TABLE "teleports_npc_nodes" (
	"id"	INTEGER,
	"match_type"	TEXT,
	"npc_id"	INTEGER,
	"npc_name"	REAL,
	"action"	TEXT,
	"dest_min_x"	REAL,
	"dest_max_x"	REAL,
	"dest_min_y"	REAL,
	"dest_max_y"	REAL,
	"dest_plane"	INTEGER,
	"search_radius"	INTEGER,
	"cost"	INTEGER,
	"orig_min_x"	REAL,
	"orig_max_x"	REAL,
	"orig_min_y"	REAL,
	"orig_max_y"	REAL,
	"orig_plane"	INTEGER,
	"next_node_type"	TEXT,
	"next_node_id"	INTEGER,
	"requirement_id"	INTEGER
);
    """)
    cur.execute("""
   CREATE TABLE "teleports_object_nodes" (
	"id"	INTEGER,
	"match_type"	TEXT,
	"object_id"	REAL,
	"object_name"	TEXT,
	"action"	TEXT,
	"dest_min_x"	INTEGER,
	"dest_max_x"	INTEGER,
	"dest_min_y"	INTEGER,
	"dest_max_y"	INTEGER,
	"dest_plane"	INTEGER,
	"orig_min_x"	INTEGER,
	"orig_max_x"	INTEGER,
	"orig_min_y"	INTEGER,
	"orig_max_y"	INTEGER,
	"orig_plane"	INTEGER,
	"search_radius"	INTEGER,
	"cost"	INTEGER,
	"next_node_type"	TEXT,
	"next_node_id"	REAL,
	"requirement_id"	REAL
);
    """)
    cur.execute("""
CREATE TABLE "teleports_requirements" (
	"id"	INTEGER,
	"metaInfo"	TEXT,
	"key"	TEXT,
	"value"	REAL,
	"comparison"	TEXT
);
    """)

    cur.execute("""
CREATE INDEX IF NOT EXISTS idx_tiles_xyplane ON tiles(x, y, plane);
    """)

    cur.execute("""
CREATE INDEX IF NOT EXISTS idx_tiles_chunk ON tiles(chunk_x, chunk_z, plane);
    """)

    cur.execute("""
CREATE INDEX IF NOT EXISTS idx_tiles_walkable
ON tiles(x, y, plane)
WHERE blocked = 0;
    """)

    cur.execute("""
CREATE INDEX IF NOT EXISTS idx_tiles_chunk_boundary
ON tiles(chunk_x, chunk_z, (x % 64), (y % 64), plane);
    """)

    cur.execute("""
CREATE VIEW IF NOT EXISTS teleports_all AS
SELECT
  'door' AS kind, id,
  tile_outside_x AS src_x, tile_outside_y AS src_y, tile_outside_plane AS src_plane,
  tile_inside_x  AS dst_x,  tile_inside_y  AS dst_y,  tile_inside_plane  AS dst_plane,
  cost, requirement_id
FROM teleports_door_nodes
UNION ALL
SELECT 'lodestone', id, dest_x, dest_y, dest_plane, dest_x, dest_y, dest_plane, cost, requirement_id
FROM teleports_lodestone_nodes
UNION ALL
SELECT 'npc', id, orig_min_x, orig_min_y, orig_plane, dest_min_x, dest_min_y, dest_plane, cost, requirement_id
FROM teleports_npc_nodes
UNION ALL
SELECT 'object', id, orig_min_x, orig_min_y, orig_plane, dest_min_x, dest_min_y, dest_plane, cost, requirement_id
FROM teleports_object_nodes
UNION ALL
SELECT 'item', id, dest_min_x, dest_min_y, dest_plane, dest_min_x, dest_min_y, dest_plane, cost, requirement_id
FROM teleports_item_nodes;
    """)

    cur.execute("""
CREATE TABLE IF NOT EXISTS cluster_entrances (
  entrance_id   INTEGER PRIMARY KEY,
  chunk_x       INTEGER NOT NULL,
  chunk_z       INTEGER NOT NULL,
  plane         INTEGER NOT NULL,
  x             INTEGER NOT NULL,
  y             INTEGER NOT NULL,
  neighbor_dir  TEXT    NOT NULL CHECK (neighbor_dir IN ('N','S','E','W')),
  UNIQUE (chunk_x, chunk_z, plane, x, y)
);
    """)

    cur.execute("""
CREATE TABLE IF NOT EXISTS cluster_intraconnections (
  chunk_x_from  INTEGER NOT NULL,
  chunk_z_from  INTEGER NOT NULL,
  plane_from    INTEGER NOT NULL,
  entrance_from INTEGER NOT NULL,
  entrance_to   INTEGER NOT NULL,
  cost          INTEGER NOT NULL,
  path_blob     BLOB,
  PRIMARY KEY (chunk_x_from, chunk_z_from, plane_from, entrance_from, entrance_to),
  FOREIGN KEY (entrance_from) REFERENCES cluster_entrances(entrance_id),
  FOREIGN KEY (entrance_to) REFERENCES cluster_entrances(entrance_id)
);
    """)

    cur.execute("""
CREATE TABLE IF NOT EXISTS cluster_interconnections (
  entrance_from INTEGER NOT NULL,
  entrance_to   INTEGER NOT NULL,
  cost          INTEGER NOT NULL,
  PRIMARY KEY (entrance_from, entrance_to),
  FOREIGN KEY (entrance_from) REFERENCES cluster_entrances(entrance_id),
  FOREIGN KEY (entrance_to) REFERENCES cluster_entrances(entrance_id)
);
    """)

    cur.execute("""
CREATE TABLE IF NOT EXISTS abstract_teleport_edges (
  edge_id       INTEGER PRIMARY KEY,
  src_x         INTEGER NOT NULL,
  src_y         INTEGER NOT NULL,
  src_plane     INTEGER NOT NULL,
  dst_x         INTEGER NOT NULL,
  dst_y         INTEGER NOT NULL,
  dst_plane     INTEGER NOT NULL,
  cost          INTEGER NOT NULL,
  requirement_id INTEGER,
  src_entrance  INTEGER,
  dst_entrance  INTEGER
);
    """)

    cur.execute("""
CREATE INDEX IF NOT EXISTS idx_cluster_entrances_plane_xy
ON cluster_entrances(plane, x, y);
    """)

    cur.execute("""
CREATE INDEX IF NOT EXISTS idx_cluster_intra_from_to
ON cluster_intraconnections(chunk_x_from, chunk_z_from, plane_from, entrance_from, entrance_to);
    """)

    cur.execute("""
CREATE INDEX IF NOT EXISTS idx_cluster_inter_to
ON cluster_interconnections(entrance_to);
    """)

    cur.execute("""
CREATE INDEX IF NOT EXISTS idx_abstract_teleport_src
ON abstract_teleport_edges(src_plane, src_x, src_y);
    """)

    cur.execute("""
CREATE INDEX IF NOT EXISTS idx_abstract_teleport_dst
ON abstract_teleport_edges(dst_plane, dst_x, dst_y);
    """)

    cur.execute("""
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
    """)

    cur.execute("""
CREATE TABLE IF NOT EXISTS movement_policy (
  policy_id INTEGER PRIMARY KEY CHECK(policy_id = 1),
  allow_diagonals INTEGER NOT NULL,
  allow_corner_cut INTEGER NOT NULL,
  unit_radius_tiles INTEGER NOT NULL
);
    """)

    cur.execute("""
CREATE TABLE IF NOT EXISTS jps_jump (
  x INTEGER NOT NULL,
  y INTEGER NOT NULL,
  plane INTEGER NOT NULL,
  dir INTEGER NOT NULL,
  next_x INTEGER,
  next_y INTEGER,
  forced_mask INTEGER,
  PRIMARY KEY (x, y, plane, dir)
);
    """)

    cur.execute("""
CREATE TABLE IF NOT EXISTS jps_spans (
  x INTEGER NOT NULL,
  y INTEGER NOT NULL,
  plane INTEGER NOT NULL,
  left_block_at INTEGER,
  right_block_at INTEGER,
  up_block_at INTEGER,
  down_block_at INTEGER,
  PRIMARY KEY (x, y, plane)
);
    """)

    cur.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        ("movement_cost_straight", "1024")
    )
    cur.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        ("movement_cost_diagonal", "1448")
    )
    cur.execute(
        """
        INSERT INTO movement_policy(policy_id, allow_diagonals, allow_corner_cut, unit_radius_tiles)
        VALUES(1, 1, 0, 1)
        ON CONFLICT(policy_id) DO UPDATE SET
          allow_diagonals=excluded.allow_diagonals,
          allow_corner_cut=excluded.allow_corner_cut,
          unit_radius_tiles=excluded.unit_radius_tiles
        """
    )

def insert_tiles(cur, chunk, tiles):
    chunk_x = None
    chunk_z = None
    chunk_size = None

    if chunk:
        chunk_x = chunk.get("x")
        chunk_z = chunk.get("z")
        chunk_size = chunk.get("chunkSize")
        if chunk_x is not None and chunk_z is not None:
            cur.execute("""
                INSERT OR REPLACE INTO chunks (chunk_x, chunk_z, chunk_size, tile_count)
                VALUES (?, ?, ?, ?)
            """, (chunk_x, chunk_z, chunk_size, len(tiles)))

    entries = []
    batch_size = 50000
    for tile in tiles:
        walk = tile.get("walk", {})
        entries.append((
            tile["x"],
            tile["y"],
            tile["plane"],
            chunk_x,
            chunk_z,
            tile.get("flag"),
            int(bool(tile.get("blocked", False))),
            tile.get("walkMask"),
            tile.get("blockedMask"),
            json.dumps(walk)
        ))
        if len(entries) >= batch_size:
            cur.executemany("""
                INSERT OR REPLACE INTO tiles 
                (x, y, plane, chunk_x, chunk_z, flag, blocked, walk_mask, blocked_mask, walk_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, entries)
            entries.clear()

    if entries:
        cur.executemany("""
            INSERT OR REPLACE INTO tiles 
            (x, y, plane, chunk_x, chunk_z, flag, blocked, walk_mask, blocked_mask, walk_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, entries)

def load_json_files(folder, conn):
    with conn:
        cur = conn.cursor()
        for filename in os.listdir(folder):
            if filename.endswith(".json"):
                filepath = os.path.join(folder, filename)
                print(f"Loading {filepath}...")
                with open(filepath, "r") as f:
                    data = json.load(f)
                tiles = data.get("tiles", [])
                if not tiles:
                    continue
                insert_tiles(cur, data.get("chunk", {}), tiles)

def main():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    create_tables(conn)
    load_json_files(JSON_FOLDER, conn)
    conn.close()
    print(f"Tiles successfully loaded into {DB_FILE}")

if __name__ == "__main__":
    main()
