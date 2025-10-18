use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet};

pub fn create_tables(conn: &mut Connection) -> Result<()> {
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;

    let tiles_columns: BTreeSet<&'static str> = [
        "x", "y", "plane", "chunk_x", "chunk_z", "flag", "blocked", "walk_mask", "blocked_mask", "walk_data",
    ]
    .into_iter()
    .collect();
    let chunks_columns: BTreeSet<&'static str> = ["chunk_x", "chunk_z", "chunk_size", "tile_count"].into_iter().collect();

    if table_exists(conn, "tiles")? && !table_has_columns(conn, "tiles", &tiles_columns)? {
        conn.execute("DROP TABLE tiles", [])?;
    }
    if table_exists(conn, "chunks")? && !table_has_columns(conn, "chunks", &chunks_columns)? {
        conn.execute("DROP TABLE chunks", [])?;
    }

    if cluster_intraconnections_requires_migration(conn)? {
        conn.execute_batch(
            r#"
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
            );
            INSERT INTO cluster_intraconnections_new
              (chunk_x_from, chunk_z_from, plane_from, entrance_from, entrance_to, cost, path_blob)
            SELECT chunk_x_from, chunk_z_from, plane_from, entrance_from, entrance_to, cost, path_blob
            FROM cluster_intraconnections;
            DROP TABLE cluster_intraconnections;
            ALTER TABLE cluster_intraconnections_new RENAME TO cluster_intraconnections;
        "#,
        )?;
    }

    if cluster_interconnections_requires_migration(conn)? {
        conn.execute_batch(
            r#"
            CREATE TABLE cluster_interconnections_new (
              entrance_from INTEGER NOT NULL,
              entrance_to   INTEGER NOT NULL,
              cost          INTEGER NOT NULL,
              PRIMARY KEY (entrance_from, entrance_to),
              FOREIGN KEY (entrance_from) REFERENCES cluster_entrances(entrance_id),
              FOREIGN KEY (entrance_to) REFERENCES cluster_entrances(entrance_id)
            );
            INSERT INTO cluster_interconnections_new
              (entrance_from, entrance_to, cost)
            SELECT entrance_from, entrance_to, cost
            FROM cluster_interconnections;
            DROP TABLE cluster_interconnections;
            ALTER TABLE cluster_interconnections_new RENAME TO cluster_interconnections;
        "#,
        )?;
    }

    if cluster_entrances_requires_migration(conn)? {
        conn.execute_batch(
            r#"
            DROP TABLE IF EXISTS cluster_entrances_new;
            CREATE TABLE cluster_entrances_new (
              entrance_id  INTEGER PRIMARY KEY,
              cluster_id   INTEGER NOT NULL REFERENCES chunk_clusters(cluster_id),
              x            INTEGER NOT NULL,
              y            INTEGER NOT NULL,
              plane        INTEGER NOT NULL,
              neighbor_dir TEXT NOT NULL CHECK (neighbor_dir IN ('N','S','E','W')),
              teleport_edge_id INTEGER REFERENCES abstract_teleport_edges(edge_id),
              UNIQUE (cluster_id, x, y, plane, neighbor_dir)
            );
            DROP TABLE IF EXISTS cluster_entrances;
            ALTER TABLE cluster_entrances_new RENAME TO cluster_entrances;
        "#,
        )?;
    }    

    conn.execute_batch(
        r#"
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
        );

        CREATE TABLE IF NOT EXISTS chunks (
            chunk_x INTEGER,
            chunk_z INTEGER,
            chunk_size INTEGER,
            tile_count INTEGER,
            PRIMARY KEY (chunk_x, chunk_z)
        );

        CREATE TABLE IF NOT EXISTS chunk_clusters (
          cluster_id INTEGER PRIMARY KEY,
          chunk_x    INTEGER NOT NULL,
          chunk_z    INTEGER NOT NULL,
          plane      INTEGER NOT NULL,
          label      INTEGER,
          tile_count INTEGER,
          FOREIGN KEY (chunk_x, chunk_z) REFERENCES chunks(chunk_x, chunk_z)
        );

        CREATE TABLE IF NOT EXISTS cluster_tiles (
          cluster_id INTEGER NOT NULL REFERENCES chunk_clusters(cluster_id),
          x INTEGER NOT NULL,
          y INTEGER NOT NULL,
          plane INTEGER NOT NULL,
          PRIMARY KEY (cluster_id, x, y, plane)
        );

        CREATE TABLE IF NOT EXISTS teleports_door_nodes (
            id	INTEGER PRIMARY KEY,
            direction	TEXT,
            real_id_open	INTEGER,
            real_id_closed	INTEGER,
            location_open_x	INTEGER,
            location_open_y	INTEGER,
            location_open_plane	INTEGER,
            location_closed_x	INTEGER,
            location_closed_y	INTEGER,
            location_closed_plane	INTEGER,
            tile_inside_x	INTEGER,
            tile_inside_y	INTEGER,
            tile_inside_plane	INTEGER,
            tile_outside_x	INTEGER,
            tile_outside_y	INTEGER,
            tile_outside_plane	INTEGER,
            open_action	TEXT,
            cost	INTEGER,
            next_node_type	TEXT,
            next_node_id	INTEGER,
            requirement_id	INTEGER
        );

        CREATE TABLE IF NOT EXISTS teleports_ifslot_nodes (
            id	INTEGER PRIMARY KEY,
            interface_id	INTEGER,
            component_id	INTEGER,
            slot_id	INTEGER,
            click_id	INTEGER,
            dest_min_x	INTEGER,
            dest_max_x	INTEGER,
            dest_min_y	INTEGER,
            dest_max_y	INTEGER,
            dest_plane	INTEGER,
            cost	INTEGER,
            next_node_type	TEXT,
            next_node_id	INTEGER,
            requirement_id	INTEGER
        );

        CREATE TABLE IF NOT EXISTS teleports_item_nodes (
            id	INTEGER PRIMARY KEY,
            item_id	INTEGER,
            action	TEXT,
            dest_min_x	INTEGER,
            dest_max_x	INTEGER,
            dest_min_y	INTEGER,
            dest_max_y	INTEGER,
            dest_plane	INTEGER,
            next_node_type	TEXT,
            next_node_id	INTEGER,
            cost	INTEGER,
            requirement_id	INTEGER
        );

        CREATE TABLE IF NOT EXISTS teleports_lodestone_nodes (
            id	INTEGER PRIMARY KEY,
            lodestone	TEXT,
            dest_x	INTEGER,
            dest_y	INTEGER,
            dest_plane	INTEGER,
            cost	INTEGER,
            next_node_type	TEXT,
            next_node_id	INTEGER,
            requirement_id	INTEGER
        );

        CREATE TABLE IF NOT EXISTS teleports_npc_nodes (
            id	INTEGER PRIMARY KEY,
            match_type	TEXT,
            npc_id	INTEGER,
            npc_name	TEXT,
            action	TEXT,
            dest_min_x	INTEGER,
            dest_max_x	INTEGER,
            dest_min_y	INTEGER,
            dest_max_y	INTEGER,
            dest_plane	INTEGER,
            search_radius	INTEGER,
            cost	INTEGER,
            orig_min_x	INTEGER,
            orig_max_x	INTEGER,
            orig_min_y	INTEGER,
            orig_max_y	INTEGER,
            orig_plane	INTEGER,
            next_node_type	TEXT,
            next_node_id	INTEGER,
            requirement_id	INTEGER
        );

        CREATE TABLE IF NOT EXISTS teleports_object_nodes (
            id	INTEGER PRIMARY KEY,
            match_type	TEXT,
            object_id	INTEGER,
            object_name	TEXT,
            action	TEXT,
            dest_min_x	INTEGER,
            dest_max_x	INTEGER,
            dest_min_y	INTEGER,
            dest_max_y	INTEGER,
            dest_plane	INTEGER,
            orig_min_x	INTEGER,
            orig_max_x	INTEGER,
            orig_min_y	INTEGER,
            orig_max_y	INTEGER,
            orig_plane	INTEGER,
            search_radius	INTEGER,
            cost	INTEGER,
            next_node_type	TEXT,
            next_node_id	INTEGER,
            requirement_id	INTEGER
        );

        CREATE TABLE IF NOT EXISTS teleports_requirements (
            id	INTEGER PRIMARY KEY,
            metaInfo	TEXT,
            key	TEXT,
            value	TEXT,
            comparison	TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_tiles_xyplane ON tiles(x, y, plane);
        CREATE INDEX IF NOT EXISTS idx_tiles_chunk ON tiles(chunk_x, chunk_z, plane);
        CREATE INDEX IF NOT EXISTS idx_tiles_walkable ON tiles(x, y, plane) WHERE blocked = 0;
        CREATE INDEX IF NOT EXISTS idx_tiles_chunk_boundary ON tiles(chunk_x, chunk_z, (x % 64), (y % 64), plane);
        
        CREATE VIEW IF NOT EXISTS teleports_all AS
        SELECT
          'door' AS kind, id,
          tile_outside_x AS src_x, tile_outside_y AS src_y, tile_outside_plane AS src_plane,
          tile_inside_x  AS dst_x,  tile_inside_y  AS dst_y,  tile_inside_plane  AS dst_plane,
          cost, requirement_id
        FROM teleports_door_nodes
        UNION ALL
        SELECT 'lodestone', id, 'null', 'null', 'null', dest_x, dest_y, dest_plane, cost, requirement_id
        FROM teleports_lodestone_nodes
        UNION ALL
        SELECT 'npc', id, orig_min_x, orig_min_y, orig_plane, dest_min_x, dest_min_y, dest_plane, cost, requirement_id
        FROM teleports_npc_nodes
        UNION ALL
        SELECT 'object', id, orig_min_x, orig_min_y, orig_plane, dest_min_x, dest_min_y, dest_plane, cost, requirement_id
        FROM teleports_object_nodes
        UNION ALL
        SELECT 'item', id, dest_min_x, dest_min_y, dest_plane, dest_min_x, dest_min_y, dest_plane, cost, requirement_id
        FROM teleports_item_nodes
        UNION ALL
        SELECT 'ifslot', id,
               'null', 'null', 'null',
               CAST(dest_min_x AS INTEGER), CAST(dest_min_y AS INTEGER), CAST(dest_plane AS INTEGER),
               cost, requirement_id
        FROM teleports_ifslot_nodes where dest_min_x is not null;

        CREATE TABLE IF NOT EXISTS cluster_entrances (
          entrance_id  INTEGER PRIMARY KEY,
          cluster_id   INTEGER NOT NULL REFERENCES chunk_clusters(cluster_id),
          x            INTEGER NOT NULL,
          y            INTEGER NOT NULL,
          plane        INTEGER NOT NULL,
          neighbor_dir TEXT NOT NULL CHECK (neighbor_dir IN ('N','S','E','W')),
          teleport_edge_id INTEGER REFERENCES abstract_teleport_edges(edge_id),
          UNIQUE (cluster_id, x, y, plane, neighbor_dir)
        );

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

        CREATE TABLE IF NOT EXISTS cluster_interconnections (
          entrance_from INTEGER NOT NULL,
          entrance_to   INTEGER NOT NULL,
          cost          INTEGER NOT NULL,
          PRIMARY KEY (entrance_from, entrance_to),
          FOREIGN KEY (entrance_from) REFERENCES cluster_entrances(entrance_id),
          FOREIGN KEY (entrance_to) REFERENCES cluster_entrances(entrance_id)
        );

        CREATE TABLE IF NOT EXISTS abstract_teleport_edges (
          edge_id       INTEGER PRIMARY KEY,
          src_x         INTEGER NULL,
          src_y         INTEGER NULL,
          src_plane     INTEGER NULL,
          dst_x         INTEGER NOT NULL,
          dst_y         INTEGER NOT NULL,
          dst_plane     INTEGER NOT NULL,
          cost          INTEGER NOT NULL,
          requirement_id INTEGER,
          src_entrance  INTEGER,
          dst_entrance  INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_cluster_entrances_plane_xy ON cluster_entrances(plane, x, y);
        CREATE INDEX IF NOT EXISTS idx_cluster_intra_from_to ON cluster_intraconnections(chunk_x_from, chunk_z_from, plane_from, entrance_from, entrance_to);
        CREATE INDEX IF NOT EXISTS idx_cluster_inter_to ON cluster_interconnections(entrance_to);
        CREATE INDEX IF NOT EXISTS idx_abstract_teleport_src ON abstract_teleport_edges(src_plane, src_x, src_y);
        CREATE INDEX IF NOT EXISTS idx_abstract_teleport_dst ON abstract_teleport_edges(dst_plane, dst_x, dst_y);

        CREATE TABLE IF NOT EXISTS meta (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS movement_policy (
          policy_id INTEGER PRIMARY KEY CHECK(policy_id = 1),
          allow_diagonals INTEGER NOT NULL,
          allow_corner_cut INTEGER NOT NULL,
          unit_radius_tiles INTEGER NOT NULL
        );

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

        INSERT INTO meta(key, value) VALUES('movement_cost_straight', '1024') ON CONFLICT(key) DO UPDATE SET value=excluded.value;
        INSERT INTO meta(key, value) VALUES('movement_cost_diagonal', '1448') ON CONFLICT(key) DO UPDATE SET value=excluded.value;
        INSERT INTO movement_policy(policy_id, allow_diagonals, allow_corner_cut, unit_radius_tiles)
        VALUES(1, 1, 0, 1)
        ON CONFLICT(policy_id) DO UPDATE SET
          allow_diagonals=excluded.allow_diagonals,
          allow_corner_cut=excluded.allow_corner_cut,
          unit_radius_tiles=excluded.unit_radius_tiles;
    "#,
    )?;

    Ok(())
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
            [table],
            |row| row.get(0),
        )
        .optional()?;
    Ok(exists.is_some())
}

fn table_has_columns(conn: &Connection, table: &str, required: &BTreeSet<&str>) -> Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table))?;
    let mut rows = stmt.query([])?;
    let mut present = BTreeSet::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        present.insert(name);
    }
    Ok(required.iter().all(|c| present.contains(*c)))
}

fn cluster_intraconnections_requires_migration(conn: &Connection) -> Result<bool> {
    if !table_exists(conn, "cluster_intraconnections")? {
        return Ok(false);
    }
    let required: BTreeSet<&'static str> = [
        "chunk_x_from",
        "chunk_z_from",
        "plane_from",
        "entrance_from",
        "entrance_to",
        "cost",
        "path_blob",
    ]
    .into_iter()
    .collect();
    if !table_has_columns(conn, "cluster_intraconnections", &required)? {
        return Ok(true);
    }
    let mut stmt = conn.prepare("PRAGMA foreign_key_list(cluster_intraconnections)")?;
    let mut rows = stmt.query([])?;
    let mut count = 0i32;
    while let Some(row) = rows.next()? {
        let target_table: String = row.get(2)?;
        if target_table == "cluster_entrances" {
            count += 1;
        }
    }
    Ok(count < 2)
}

fn cluster_interconnections_requires_migration(conn: &Connection) -> Result<bool> {
    if !table_exists(conn, "cluster_interconnections")? {
        return Ok(false);
    }
    let mut stmt = conn.prepare("PRAGMA table_info(cluster_interconnections)")?;
    let mut rows = stmt.query([])?;
    let mut pk_map: BTreeMap<String, i64> = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        let pk: i64 = row.get(5)?;
        pk_map.insert(name, pk);
    }
    if *pk_map.get("entrance_from").unwrap_or(&0) == 0 || *pk_map.get("entrance_to").unwrap_or(&0) == 0 {
        return Ok(true);
    }
    let mut stmt = conn.prepare("PRAGMA foreign_key_list(cluster_interconnections)")?;
    let mut rows = stmt.query([])?;
    let mut fk_map: BTreeMap<String, String> = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let table: String = row.get(2)?;
        let from_col: String = row.get(3)?;
        fk_map.insert(from_col, table);
    }
    let ok_from = fk_map.get("entrance_from").map(|s| s.as_str()) == Some("cluster_entrances");
    let ok_to = fk_map.get("entrance_to").map(|s| s.as_str()) == Some("cluster_entrances");
    Ok(!(ok_from && ok_to))
}

fn cluster_entrances_requires_migration(conn: &Connection) -> Result<bool> {
    if !table_exists(conn, "cluster_entrances")? {
        return Ok(false);
    }
    // Check columns: must have cluster_id, and should NOT have legacy chunk_x/chunk_z
    let mut stmt = conn.prepare("PRAGMA table_info(cluster_entrances)")?;
    let mut rows = stmt.query([])?;
    let mut cols: BTreeSet<String> = BTreeSet::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        cols.insert(name);
    }
    let has_cluster_id = cols.contains("cluster_id");
    let has_legacy_chunk = cols.contains("chunk_x") || cols.contains("chunk_z");
    Ok(!has_cluster_id || has_legacy_chunk)
}
