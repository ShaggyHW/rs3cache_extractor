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

def create_tables(conn):
    cur = conn.cursor()
    tiles_columns = {"x", "y", "plane", "chunk_x", "chunk_z", "flag", "blocked", "walk_mask", "blocked_mask", "walk_data"}
    chunks_columns = {"chunk_x", "chunk_z", "chunk_size", "tile_count"}

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tiles'")
    if cur.fetchone() and not _table_has_columns(cur, "tiles", tiles_columns):
        cur.execute("DROP TABLE tiles")

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='chunks'")
    if cur.fetchone() and not _table_has_columns(cur, "chunks", chunks_columns):
        cur.execute("DROP TABLE chunks")

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
    conn.commit()

def insert_tiles(conn, chunk, tiles):
    cur = conn.cursor()
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

    if entries:
        cur.executemany("""
            INSERT OR REPLACE INTO tiles 
            (x, y, plane, chunk_x, chunk_z, flag, blocked, walk_mask, blocked_mask, walk_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, entries)
    conn.commit()

def load_json_files(folder, conn):
    for filename in os.listdir(folder):
        if filename.endswith(".json"):
            filepath = os.path.join(folder, filename)
            print(f"Loading {filepath}...")
            with open(filepath, "r") as f:
                data = json.load(f)
            tiles = data.get("tiles", [])
            if not tiles:
                continue
            insert_tiles(conn, data.get("chunk", {}), tiles)

def main():
    conn = sqlite3.connect(DB_FILE)
    create_tables(conn)
    load_json_files(JSON_FOLDER, conn)
    conn.close()
    print(f"Tiles successfully loaded into {DB_FILE}")

if __name__ == "__main__":
    main()
