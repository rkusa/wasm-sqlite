use std::cell::RefCell;
use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr::NonNull;

use rusqlite::{params_from_iter, OpenFlags, Row, Rows};
use serde::ser::Serializer;
use serde::Serialize;
use serde_json::Value as JsonValue;
use sqlite_vfs::{register, RegisterError};

pub use crate::vfs::PagesVfs;

mod vfs;

extern "C" {
    pub fn page_count() -> u32;
    pub fn get_page(ix: u32, ptr: *mut u8);
    pub fn put_page(ix: u32, ptr: *const u8);
    pub fn del_page(ix: u32);
    pub fn conn_sleep(ms: u32);
}

// TODO: is there any way to provide this method for SQLite, but not export it as part of the WASM
// module?
#[no_mangle]
extern "C" fn sqlite3_os_init() -> i32 {
    const SQLITE_OK: i32 = 0;
    const SQLITE_ERROR: i32 = 1;

    pretty_env_logger::formatted_builder()
        .filter(Some("sqlite_vfs"), log::LevelFilter::Trace)
        .try_init()
        .ok();

    match register("cfdo", PagesVfs::<4096>::default(), true) {
        Ok(_) => SQLITE_OK,
        Err(RegisterError::Nul(_)) => SQLITE_ERROR,
        Err(RegisterError::Register(code)) => code,
    }
}

pub struct Connection {
    conn: rusqlite::Connection,
    last_error: Option<Box<dyn std::error::Error>>,
}

#[no_mangle]
pub unsafe extern "C" fn conn_new() -> *mut Connection {
    let is_new = page_count() == 0;

    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        "main.db",
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        "cfdo",
    )
    .expect("open connection");

    if is_new {
        conn.execute("PRAGMA page_size = 4096;", [])
            .expect("set page_size = 4096");
    }

    let journal_mode: String = conn
        .query_row("PRAGMA journal_mode = MEMORY", [], |row| row.get(0))
        .expect("set journal_mode = MEMORY");
    assert_eq!(journal_mode, "memory");

    Box::into_raw(Box::new(Connection {
        conn,
        last_error: None,
    }))
}

#[no_mangle]
pub unsafe extern "C" fn conn_last_error(conn: *mut Connection) -> *mut c_char {
    use std::fmt::Write;

    let conn: &mut Connection = unsafe { conn.as_mut().unwrap() };

    if let Some(err) = conn.last_error.take() {
        let mut message = err.to_string();

        let mut source = std::error::Error::source(err.as_ref());
        let mut i = 0;

        if source.is_some() {
            message += "\n\nCaused by:\n";
        }

        while let Some(err) = source {
            if i > 0 {
                writeln!(&mut message).ok();
            }
            write!(&mut message, "{i:>4}: {err}").ok();
            source = std::error::Error::source(err);
            i += 1;
        }

        CString::new(message).unwrap().into_raw()
    } else {
        std::ptr::null_mut()
    }
}

#[no_mangle]
pub unsafe extern "C" fn conn_last_error_drop(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    let _ = CString::from_raw(s);
}

#[no_mangle]
pub unsafe extern "C" fn conn_drop(conn: *mut Connection) {
    drop(Box::from_raw(conn));
}

#[derive(serde::Deserialize)]
struct Query {
    sql: String,
    params: Vec<JsonValue>,
}

#[no_mangle]
extern "C" fn conn_execute(conn: *mut Connection, ptr: *const u8, len: usize) -> i32 {
    let conn: &mut Connection = unsafe { conn.as_mut().unwrap() };

    let query = unsafe { std::slice::from_raw_parts::<'_, u8>(ptr, len) };
    let query: Query = match serde_json::from_slice(query) {
        Ok(query) => query,
        Err(err) => {
            conn.last_error = Some(Box::new(err));
            return 0;
        }
    };

    if let Err(err) = conn
        .conn
        .execute(&query.sql, params_from_iter(&query.params))
    {
        conn.last_error = Some(Box::new(err));
        0
    } else {
        1
    }
}

#[repr(C)]
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
extern "C" fn conn_query(conn: *mut Connection, ptr: *const u8, len: usize) -> *const JsonString {
    let conn: &mut Connection = unsafe { conn.as_mut().unwrap() };

    let query = unsafe { std::slice::from_raw_parts::<'_, u8>(ptr, len) };
    let query: Query = match serde_json::from_slice(query) {
        Ok(query) => query,
        Err(err) => {
            conn.last_error = Some(Box::new(err));
            return std::ptr::null();
        }
    };

    let mut stmt = match conn.conn.prepare(&query.sql) {
        Ok(stmt) => stmt,
        Err(err) => {
            conn.last_error = Some(Box::new(err));
            return std::ptr::null();
        }
    };
    let names = stmt
        .column_names()
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    let rows = match stmt.query(params_from_iter(&query.params)) {
        Ok(rows) => rows,
        Err(err) => {
            conn.last_error = Some(Box::new(err));
            return std::ptr::null();
        }
    };
    let rows = NamedRows {
        names,
        rows: RefCell::new(rows),
    };

    let result = match serde_json::to_string(&rows) {
        Ok(result) => result,
        Err(err) => {
            conn.last_error = Some(Box::new(err));
            return std::ptr::null();
        }
    };
    JsonString::new(result).into_raw()
}

struct NamedRows<'a> {
    names: Vec<String>,
    rows: RefCell<Rows<'a>>,
}

impl<'a> Serialize for NamedRows<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeSeq;

        let mut rows = self.rows.borrow_mut();
        let mut seq = serializer.serialize_seq(None)?;
        while let Some(row) = rows
            .next()
            .map_err(|err| serde::ser::Error::custom(format!("failed to get next row: {err}")))?
        {
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

impl<'a> Serialize for NamedRow<'a> {
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
unsafe extern "C" fn query_result_drop(json: *mut JsonString) {
    drop(Box::from_raw(json));
}

impl Drop for JsonString {
    fn drop(&mut self) {
        unsafe {
            String::from_raw_parts(self.ptr.as_ptr(), self.len, self.cap);
        }
    }
}
