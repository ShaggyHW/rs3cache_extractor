use anyhow::{bail, Context, Result};
use calamine::{open_workbook_auto, DataType, Reader};
use rusqlite::{params_from_iter, Connection};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;
use std::io::Write;
use url::Url;

const ALLOWED_NEXT_NODE_TYPES: &[&str] = &["object", "npc", "ifslot", "door", "lodestone", "item"];
const ALLOWED_DOOR_DIRECTIONS: &[&str] = &["IN", "OUT"];
const ALLOWED_REQUIREMENT_COMPARISONS: &[&str] = &["=", "!=", "<", "<=", ">", ">="];
const TELEPORT_NODE_TABLES: &[&str] = &[
    "teleports_door_nodes",
    "teleports_ifslot_nodes",
    "teleports_item_nodes",
    "teleports_lodestone_nodes",
    "teleports_npc_nodes",
    "teleports_object_nodes",
    // include requirements to rebuild edges when requirement_id changes
    "teleports_requirements",
];

#[derive(Clone, Debug)]
struct Column {
    name: String,
    decl_type: String,
    notnull: bool,
    pk: bool,
}

fn normalize_specials(table_name: &str, row: &mut BTreeMap<String, rusqlite::types::Value>) -> Result<()> {
    let t = table_name.to_ascii_lowercase();
    // Normalize door_nodes.direction to uppercase
    if t == "door_nodes" {
        if let Some(v) = row.get_mut("direction") {
            if let rusqlite::types::Value::Text(ref mut s) = v {
                *s = s.trim().to_ascii_uppercase();
            }
        }
    }
    // Normalize next_node_type to lowercase
    if let Some(v) = row.get_mut("next_node_type") {
        if let rusqlite::types::Value::Text(ref mut s) = v {
            *s = s.trim().to_ascii_lowercase();
        }
    }
    Ok(())
}

impl Column {
    fn is_integer(&self) -> bool {
        self.decl_type.to_uppercase().contains("INT")
    }
    fn is_text(&self) -> bool {
        let up = self.decl_type.to_uppercase();
        up.contains("CHAR") || up.contains("CLOB") || up.contains("TEXT")
    }
}

#[derive(Clone, Debug)]
struct Table {
    name: String,
    columns: HashMap<String, Column>, // lowercased key -> Column
}

pub fn cmd_import_xlsx(xlsx: &str, db: &Path, dry_run: bool, truncate: &[String], sheets: &[String]) -> Result<()> {
    if !db.exists() {
        bail!("SQLite DB not found: {}", db.display());
    }

    // Obtain local XLSX path, downloading if Google Sheets URL.
    let (xlsx_path, cleanup_temp): (PathBuf, bool) = if is_google_sheets_url(xlsx) {
        println!("Downloading Google Sheet as .xlsx ...");
        (download_google_sheet_as_xlsx(xlsx)?, true)
    } else {
        let p = PathBuf::from(xlsx);
        if !p.exists() {
            bail!("XLSX file not found: {}", p.display());
        }
        (p, false)
    };

    let mut conn = Connection::open(db).with_context(|| format!("Open DB {}", db.display()))?;
    conn.execute_batch("PRAGMA foreign_keys=ON;")?;

    // Introspect tables outside the transaction for simple typing
    let tables = fetch_existing_tables(&conn)?;
    let mut tx = conn.transaction()?;
    let truncate_set: HashSet<String> = truncate.iter().map(|s| s.to_lowercase()).collect();
    let only_set: Option<HashSet<String>> = if sheets.is_empty() {
        None
    } else {
        Some(sheets.iter().map(|s| s.to_lowercase()).collect())
    };
    let mut teleports_touched = false;

    // Validate requested truncations and sheets
    for t in &truncate_set {
        if !tables.contains_key(t) {
            bail!("--truncate table not found in DB: {}", t);
        }
    }
    if let Some(only) = &only_set {
        for s in only {
            if !tables.contains_key(s) {
                bail!("Requested sheet/table not found in DB: {}", s);
            }
        }
    }

    // Truncate if requested
    for tkey in &truncate_set {
        let t = &tables[tkey];
        println!("Truncating table: {}", t.name);
        if !dry_run {
            tx.execute(&format!("DELETE FROM {}", t.name), [])?;
        }
        if TELEPORT_NODE_TABLES.contains(&t.name.to_ascii_lowercase().as_str()) {
            teleports_touched = true;
        }
    }

    // Open workbook
    let mut wb = open_workbook_auto(&xlsx_path)
        .with_context(|| format!("Open workbook {}", xlsx_path.display()))?;

    // Sheet order: process 'requirements' first
    let mut sheet_names: Vec<String> = wb
        .sheet_names()
        .into_iter()
        .cloned()
        .collect();
    sheet_names.sort_by_key(|n| if n.eq_ignore_ascii_case("requirements") { 0 } else { 1 });

    let mut total_inserted: usize = 0;
    for sheet in sheet_names {
        let sheet_key = sheet.to_lowercase();
        if let Some(only) = &only_set {
            if !only.contains(&sheet_key) {
                continue;
            }
        }
        let Some(table) = tables.get(&sheet_key) else {
            println!("Skipping worksheet '{}' (no matching table in DB)", sheet);
            continue;
        };

        if let Some(Ok(range)) = wb.worksheet_range(&sheet) {
            println!("Processing worksheet '{}' -> table '{}'", sheet, table.name);
            let rows = read_worksheet(&range, table)?;
            println!("  Prepared {} row(s)", rows.len());
            let mut sheet_preview = 0usize;
            if TELEPORT_NODE_TABLES.contains(&table.name.to_ascii_lowercase().as_str()) {
                teleports_touched = true;
            }
            for mut r in rows {
                normalize_specials(&table.name, &mut r)?;
                validate_specials(&table.name, &r)?;
                let (sql, params) = build_insert_sql(table, &r)?;
                if dry_run {
                    if sheet_preview < 5 {
                        println!("  SQL: {}\n  Params: {:?}", sql, params);
                        sheet_preview += 1;
                    }
                } else {
                    tx.execute(&sql, params_from_iter(params))?;
                }
                total_inserted += 1;
            }
        } else {
            println!("Skipping worksheet '{}' (unable to read range)", sheet);
        }
    }

    // If any teleport-related tables were touched (truncated or inserted), rebuild abstract_teleport_edges
    if teleports_touched {
        if dry_run {
            println!(
                "Dry-run: would rebuild abstract_teleport_edges from teleports_all (DELETE + INSERT)."
            );
        } else {
            println!("Rebuilding abstract_teleport_edges from teleports_all ...");
            tx.execute_batch(
                r#"
                DELETE FROM abstract_teleport_edges;
                INSERT INTO abstract_teleport_edges (
                  src_x, src_y, src_plane,
                  dst_x, dst_y, dst_plane,
                  cost, requirement_id
                )
                SELECT src_x, src_y, src_plane, dst_x, dst_y, dst_plane, cost, requirement_id
                FROM teleports_all;
                "#,
            )?;
        }
    }

    if dry_run {
        println!("Dry-run complete. Rows that would be inserted: {}", total_inserted);
        // Drop transaction without commit -> rollback
    } else {
        tx.commit()?;
        println!("Import complete. Rows inserted: {}", total_inserted);
    }

    // Cleanup temp file if downloaded
    if cleanup_temp {
        let _ = fs::remove_file(&xlsx_path);
    }

    Ok(())
}

fn is_google_sheets_url(s: &str) -> bool {
    if let Ok(url) = Url::parse(s) {
        (url.scheme() == "http" || url.scheme() == "https")
            && url.domain().map(|d| d.contains("docs.google.com")).unwrap_or(false)
            && url.path().contains("/spreadsheets/")
    } else {
        false
    }
}

fn build_gsheet_export_url(doc_url: &str) -> Result<String> {
    let url = Url::parse(doc_url)?;
    // Typical path: /spreadsheets/d/<sheet_id>/edit
    let parts: Vec<&str> = url.path().split('/').filter(|s| !s.is_empty()).collect();
    let mut sheet_id: Option<&str> = None;
    for i in 0..parts.len() {
        if parts[i] == "d" && i + 1 < parts.len() {
            sheet_id = Some(parts[i + 1]);
            break;
        }
    }
    let Some(sheet_id) = sheet_id else { bail!("Unable to parse Google Sheets ID from URL"); };

    let mut base = format!(
        "https://docs.google.com/spreadsheets/d/{}/export?format=xlsx",
        sheet_id
    );
    if let Some(q) = url.query() {
        for pair in q.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                if k == "gid" {
                    base.push_str("&gid=");
                    base.push_str(v);
                }
            }
        }
    }
    Ok(base)
}

fn download_google_sheet_as_xlsx(doc_url: &str) -> Result<PathBuf> {
    let export = build_gsheet_export_url(doc_url)?;
    let resp = reqwest::blocking::get(&export)
        .with_context(|| format!("Download {}", export))?;
    if !resp.status().is_success() {
        bail!("Failed to download Google Sheet: HTTP {}", resp.status());
    }
    let bytes = resp.bytes()?;
    let mut tmp = NamedTempFile::new()?;
    tmp.as_file_mut().write_all(&bytes)?;
    let (_file, path) = tmp.keep()?;
    Ok(path)
}

fn fetch_existing_tables(conn: &Connection) -> Result<HashMap<String, Table>> {
    let mut out: HashMap<String, Table> = HashMap::new();
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
    let names: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<_, _>>()?;
    for tname in names {
        if tname.starts_with("sqlite_") {
            continue;
        }
        let mut cols_stmt = conn.prepare(&format!("PRAGMA table_info('{}')", tname.replace('\'', "''")))?;
        let mut cols: HashMap<String, Column> = HashMap::new();
        let mut rows = cols_stmt.query([])?;
        while let Some(r) = rows.next()? {
            let name: String = r.get(1)?;
            let decl_type: String = r.get::<_, Option<String>>(2)?.unwrap_or_default();
            let notnull: i64 = r.get(3)?;
            let pk: i64 = r.get(5)?;
            let c = Column {
                name: name.clone(),
                decl_type,
                notnull: notnull != 0,
                pk: pk != 0,
            };
            cols.insert(name.to_lowercase(), c);
        }
        out.insert(
            tname.to_lowercase(),
            Table {
                name: tname,
                columns: cols,
            },
        );
    }
    Ok(out)
}

fn normalize_header(h: &DataType) -> Option<String> {
    match h {
        DataType::Empty => None,
        DataType::String(s) => {
            let s = s.trim();
            if s.is_empty() { None } else { Some(s.to_string()) }
        }
        other => {
            let s = other.to_string();
            let s = s.trim();
            if s.is_empty() { None } else { Some(s.to_string()) }
        }
    }
}

fn coerce_value(raw: &DataType, col: &Column) -> Option<rusqlite::types::Value> {
    use rusqlite::types::Value as V;
    match raw {
        DataType::Empty => None,
        DataType::Bool(b) => {
            if col.is_integer() { Some(V::Integer(if *b {1} else {0})) } else { Some(V::Text(b.to_string())) }
        }
        DataType::Int(i) => {
            if col.is_integer() { Some(V::Integer(*i as i64)) } else { Some(V::Text(i.to_string())) }
        }
        DataType::Float(f) => {
            if col.is_integer() {
                let v = if f.fract() == 0.0 { *f as i64 } else { f.round() as i64 };
                Some(V::Integer(v))
            } else {
                Some(V::Text(f.to_string()))
            }
        }
        DataType::String(s) => {
            let v = s.trim();
            if v.is_empty() { return None; }
            if col.is_integer() {
                let low = v.to_ascii_lowercase();
                if ["true","yes","y","on"].contains(&low.as_str()) {
                    return Some(V::Integer(1));
                }
                if ["false","no","n","off"].contains(&low.as_str()) {
                    return Some(V::Integer(0));
                }
                if let Ok(iv) = v.parse::<i64>() {
                    return Some(V::Integer(iv));
                }
                if let Ok(fv) = v.parse::<f64>() {
                    return Some(V::Integer(fv.round() as i64));
                }
            }
            Some(V::Text(v.to_string()))
        }
        DataType::DateTime(f) => {
            if col.is_integer() {
                Some(rusqlite::types::Value::Integer(f.round() as i64))
            } else {
                Some(rusqlite::types::Value::Text(f.to_string()))
            }
        }
        _ => None,
    }
}

fn read_worksheet(range: &calamine::Range<DataType>, table: &Table) -> Result<Vec<BTreeMap<String, rusqlite::types::Value>>> {
    let mut rows_iter = range.rows();
    let headers_row = match rows_iter.next() {
        Some(r) => r,
        None => return Ok(vec![]),
    };
    let headers: Vec<Option<String>> = headers_row.iter().map(normalize_header).collect();

    // Map headers to DB columns
    let header_to_colname: Vec<Option<String>> = headers
        .into_iter()
        .map(|h| {
            if let Some(h) = h {
                let key = h.to_lowercase();
                if let Some(c) = table.columns.get(&key) {
                    Some(c.name.clone())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut out = Vec::new();
    for data_row in rows_iter {
        let mut row_map: BTreeMap<String, rusqlite::types::Value> = BTreeMap::new();
        let mut empty = true;
        for (idx, raw) in data_row.iter().enumerate() {
            if idx >= header_to_colname.len() { break; }
            if let Some(ref colname) = header_to_colname[idx] {
                if let Some(col) = table.columns.get(&colname.to_lowercase()) {
                    if let Some(val) = coerce_value(raw, col) {
                        empty = false;
                        row_map.insert(col.name.clone(), val);
                    } else {
                        // explicit None
                        row_map.insert(col.name.clone(), rusqlite::types::Value::Null);
                    }
                }
            }
        }
        if !empty {
            out.push(row_map);
        }
    }
    Ok(out)
}

fn validate_specials(table_name: &str, row: &BTreeMap<String, rusqlite::types::Value>) -> Result<()> {
    let t = table_name.to_ascii_lowercase();
    let get_text = |key: &str| -> Option<String> {
        row.get(key).and_then(|v| match v {
            rusqlite::types::Value::Text(s) => Some(s.clone()),
            rusqlite::types::Value::Integer(i) => Some(i.to_string()),
            _ => None,
        })
    };

    if t == "door_nodes" {
        if let Some(mut dir) = get_text("direction") {
            dir = dir.trim().to_ascii_uppercase();
            if !ALLOWED_DOOR_DIRECTIONS.contains(&dir.as_str()) {
                bail!("Invalid door_nodes.direction: {} (allowed {:?})", dir, ALLOWED_DOOR_DIRECTIONS);
            }
        }
    }
    if let Some(mut nt) = get_text("next_node_type") {
        nt = nt.trim().to_ascii_lowercase();
        if !ALLOWED_NEXT_NODE_TYPES.contains(&nt.as_str()) {
            bail!(
                "Invalid next_node_type: {} (allowed {:?})",
                nt, ALLOWED_NEXT_NODE_TYPES
            );
        }
    }
    if t == "requirements" {
        if let Some(cmpv) = get_text("comparison") {
            if !ALLOWED_REQUIREMENT_COMPARISONS.contains(&cmpv.as_str()) {
                bail!(
                    "Invalid requirements.comparison: {} (allowed {:?})",
                    cmpv, ALLOWED_REQUIREMENT_COMPARISONS
                );
            }
        }
    }
    Ok(())
}

fn build_insert_sql(
    table: &Table,
    row: &BTreeMap<String, rusqlite::types::Value>,
) -> Result<(String, Vec<rusqlite::types::Value>)> {
    // Only include known columns; skip PK if value is Null/empty
    let mut cols: Vec<String> = Vec::new();
    let mut vals: Vec<rusqlite::types::Value> = Vec::new();
    for (k, v) in row.iter() {
        if let Some(col) = table.columns.get(&k.to_lowercase()) {
            let include = if col.pk {
                match v {
                    rusqlite::types::Value::Null => false,
                    rusqlite::types::Value::Text(s) if s.is_empty() => false,
                    _ => true,
                }
            } else {
                true
            };
            if include {
                cols.push(col.name.clone());
                vals.push(v.clone());
            }
        }
    }
    if cols.is_empty() {
        bail!("No valid columns to insert after filtering");
    }

    let pk_names: Vec<String> = table
        .columns
        .values()
        .filter(|c| c.pk)
        .map(|c| c.name.clone())
        .collect();
    let pk_name = if pk_names.len() == 1 { Some(pk_names[0].clone()) } else { None };

    let placeholders = (0..cols.len()).map(|_| "?").collect::<Vec<_>>().join(",");
    let base_insert = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table.name,
        cols.join(", "),
        placeholders
    );

    if let Some(pk) = pk_name {
        if cols.iter().any(|c| c == &pk) {
            let assignments: Vec<String> = cols
                .iter()
                .filter(|c| c.as_str() != pk)
                .map(|c| format!("{}=excluded.{}", c, c))
                .collect();
            let sql = if assignments.is_empty() {
                base_insert.replace("INSERT ", "INSERT OR IGNORE ")
            } else {
                format!("{} ON CONFLICT({}) DO UPDATE SET {}", base_insert, pk, assignments.join(", "))
            };
            return Ok((sql, vals));
        }
    }

    Ok((base_insert, vals))
}
