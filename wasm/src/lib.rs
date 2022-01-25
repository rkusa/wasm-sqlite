use std::cell::RefCell;
use std::ptr::NonNull;

use rusqlite::{params_from_iter, Connection, OpenFlags, Row, Rows};
use serde::ser::Serializer;
use serde::Serialize;
use serde_json::Value as JsonValue;
use sqlite_vfs::register;

pub use crate::vfs::PagesVfs;

mod vfs;

extern "C" {
    pub fn get_page(ix: u32) -> *mut u8;
    pub fn put_page(ix: u32, ptr: *const u8);
}

// TODO: is there any way to provide this method for SQLite, but not export it as part of the WASM
// module?
#[no_mangle]
extern "C" fn sqlite3_os_init() -> i32 {
    if register("cfdo", PagesVfs::<4096>).is_ok() {
        0
    } else {
        1
    }
}

fn connect() -> Connection {
    let conn = Connection::open_with_flags_and_vfs(
        "main.db3",
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        "cfdo",
    )
    .expect("open connection");

    // TODO: detect new DB and only execute once after being created
    conn.execute("PRAGMA page_size = 4096;", [])
        .expect("set page_size = 4096");
    let journal_mode: String = conn
        .query_row("PRAGMA journal_mode = MEMORY", [], |row| row.get(0))
        .expect("set journal_mode = MEMORY");
    assert_eq!(journal_mode, "memory");

    conn
}

#[derive(serde::Deserialize)]
struct Query {
    sql: String,
    params: Vec<JsonValue>,
}

#[no_mangle]
extern "C" fn execute(ptr: *const u8, len: usize) {
    let query = unsafe { std::slice::from_raw_parts::<'_, u8>(ptr, len) };
    let query: Query = serde_json::from_slice(query).expect("deserialize query");
    let conn = connect();

    conn.execute(&query.sql, params_from_iter(&query.params))
        .expect("execute query");
}

pub struct JsonString {
    ptr: NonNull<u8>,
    len: usize,
    cap: usize,
}

impl JsonString {
    fn new(json: String) -> Self {
        let mut v = std::mem::ManuallyDrop::new(json);
        Self {
            ptr: unsafe { NonNull::new_unchecked(v.as_mut_ptr()) },
            len: v.len(),
            cap: v.capacity(),
        }
    }

    fn into_raw(self) -> *mut Self {
        Box::into_raw(Box::new(self))
    }
}

#[no_mangle]
extern "C" fn query(ptr: *const u8, len: usize) -> *const JsonString {
    let query = unsafe { std::slice::from_raw_parts::<'_, u8>(ptr, len) };
    let query: Query = serde_json::from_slice(query).expect("deserialize query");
    let conn = connect();

    let mut stmt = conn.prepare(&query.sql).expect("prepare query");
    let names = stmt
        .column_names()
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    let rows = stmt.query(params_from_iter(&query.params)).expect("query");
    let rows = NamedRows {
        names,
        rows: RefCell::new(rows),
    };

    let result = serde_json::to_string(&rows).expect("serialize query result");
    JsonString::new(result).into_raw()
}

struct NamedRows<'a> {
    names: Vec<String>,
    rows: RefCell<Rows<'a>>,
}

impl<'a, 's> Serialize for NamedRows<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeSeq;

        let mut rows = self.rows.borrow_mut();
        let mut seq = serializer.serialize_seq(None)?;
        while let Some(row) = rows.next().expect("next row") {
            let row = NamedRow {
                names: &self.names,
                row,
            };
            seq.serialize_element(&row)?;
        }
        seq.end()
    }
}

struct NamedRow<'a> {
    names: &'a [String],
    row: &'a Row<'a>,
}

impl<'a, 's> Serialize for NamedRow<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use rusqlite::types::ValueRef;
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(self.names.len()))?;
        for i in 0..self.names.len() {
            let val = self.row.get_ref_unwrap(i);
            match val {
                ValueRef::Null => map.serialize_entry(&self.names[i], &JsonValue::Null)?,
                ValueRef::Integer(v) => map.serialize_entry(&self.names[i], &v)?,
                ValueRef::Real(v) => map.serialize_entry(&self.names[i], &v)?,
                ValueRef::Text(v) => {
                    let s = String::from_utf8_lossy(v);
                    map.serialize_entry(&self.names[i], &s)?
                }
                ValueRef::Blob(v) => map.serialize_entry(&self.names[i], &v)?,
            }
        }
        map.end()
    }
}

#[no_mangle]
unsafe fn alloc(size: usize) -> *mut u8 {
    use std::alloc::{alloc, Layout};

    let align = std::mem::align_of::<usize>();
    let layout = Layout::from_size_align_unchecked(size, align);
    alloc(layout)
}

#[no_mangle]
unsafe fn dealloc(ptr: *mut u8, size: usize) {
    use std::alloc::{dealloc, Layout};
    let align = std::mem::align_of::<usize>();
    let layout = Layout::from_size_align_unchecked(size, align);
    dealloc(ptr, layout);
}

#[no_mangle]
unsafe extern "C" fn query_result_destroy(json: *mut JsonString) {
    Box::from_raw(json);
}

impl Drop for JsonString {
    fn drop(&mut self) {
        unsafe {
            String::from_raw_parts(self.ptr.as_ptr(), self.len as usize, self.cap as usize);
        }
    }
}
