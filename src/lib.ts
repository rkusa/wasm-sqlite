import * as Asyncify from "asyncify-wasm";
import module from "./wasm_sqlite.wasm";

export type Param = string | number | boolean | null;

export interface Vfs {
  pageCount(): number;
  getPage(ix: number): Promise<Uint8Array>;
  putPage(ix: number, page: Uint8Array): Promise<void>;
  delPage(ix: number): Promise<void>;
}

export class Sqlite {
  private readonly exports: Exports;

  private constructor(exports: Exports) {
    this.exports = exports;
  }

  public static async instantiate(vfs: Vfs): Promise<Sqlite> {
    let exports: Exports;
    const stdout = new Log(false);
    const stderr = new Log(true);
    const instance = await Asyncify.instantiate(module, {
      wasi_snapshot_preview1: {
        // "wasi_snapshot_preview1"."random_get": [I32, I32] -> [I32]
        random_get(offset: number, length: number) {
          const buffer = new Uint8Array(exports.memory.buffer, offset, length);
          crypto.getRandomValues(buffer);

          return ERRNO_SUCCESS;
        },

        // "wasi_snapshot_preview1"."clock_time_get": [I32, I64, I32] -> [I32]
        clock_time_get(id: number, _precision: number, offset: number) {
          const CLOCKID_REALTIME = 0;
          const CLOCKID_MONOTONIC = 1;
          const CLOCKID_PROCESS_CPUTIME_ID = 2;
          const CLOCKID_THREAD_CPUTIME_ID = 3;

          const memoryView = new DataView(exports.memory.buffer);

          switch (id) {
            case CLOCKID_REALTIME:

            // performance.now() would be a better fit for the following, but is not available on
            // Cloudflare Workers
            case CLOCKID_MONOTONIC:
            case CLOCKID_PROCESS_CPUTIME_ID:
            case CLOCKID_THREAD_CPUTIME_ID: {
              const time = BigInt(Date.now()) * BigInt(1e6);
              memoryView.setBigUint64(offset, time, true);
              break;
            }

            default:
              return ERRNO_INVAL;
          }

          return ERRNO_SUCCESS;
        },

        // "wasi_snapshot_preview1"."fd_write": [I32, I32, I32, I32] -> [I32]
        fd_write(
          fd: number,
          iovsOffset: number,
          iovsLength: number,
          nwrittenOffset: number
        ) {
          if (fd !== 1 && fd !== 2) {
            return ERRNO_BADF;
          }

          const decoder = new TextDecoder();
          const memoryView = new DataView(exports.memory.buffer);
          let nwritten = 0;
          for (let i = 0; i < iovsLength; i++) {
            const dataOffset = memoryView.getUint32(iovsOffset, true);
            iovsOffset += 4;

            const dataLength = memoryView.getUint32(iovsOffset, true);
            iovsOffset += 4;

            const data = new Uint8Array(
              exports.memory.buffer,
              dataOffset,
              dataLength
            );
            const s = decoder.decode(data);
            nwritten += data.byteLength;
            switch (fd) {
              case 1: // stdout
                stdout.write(s);
                break;
              case 2: // stderr
                stderr.write(s);
                break;
              default:
                return ERRNO_BADF;
            }
          }

          memoryView.setUint32(nwrittenOffset, nwritten, true);

          return ERRNO_SUCCESS;
        },

        // "wasi_snapshot_preview1"."poll_oneoff": [I32, I32, I32, I32] -> [I32]
        poll_oneoff() {
          return ERRNO_NOSYS;
        },

        // "wasi_snapshot_preview1"."environ_get": [I32, I32] -> [I32]
        environ_get() {
          throw new Error("environ_get not implemented");
        },

        // "wasi_snapshot_preview1"."environ_sizes_get": [I32, I32] -> [I32]
        environ_sizes_get(
          environcOffset: number,
          _environBufferSizeOffset: number
        ) {
          const memoryView = new DataView(exports.memory.buffer);
          memoryView.setUint32(environcOffset, 0, true);
          return ERRNO_SUCCESS;
        },

        // "wasi_snapshot_preview1"."proc_exit": [I32] -> []
        proc_exit(rval: number) {
          throw new Error(`WASM program exited with code: ${rval}`);
        },
      },

      env: {
        page_count(): number {
          return vfs.pageCount();
        },

        async get_page(ix: number, ptr: number) {
          const page = await vfs.getPage(ix);
          // console.log("got page:", ix, page);
          // console.log("write at", ptr, page.length);
          const dst = new Uint8Array(exports.memory.buffer, ptr, 4096);
          dst.set(page);
        },

        async put_page(ix: number, ptr: number) {
          const page = new Uint8Array(exports.memory.buffer, ptr, 4096);
          await vfs.putPage(ix, page);
        },

        async del_page(ix: number) {
          await vfs.delPage(ix);
        },

        async conn_sleep(ms: number) {
          // console.log("sleep", ms);
          await new Promise<void>((resolve) => setTimeout(resolve, ms));
        },
      },
    });
    exports = instance.exports as unknown as Exports;

    // increase asyncify stack size
    const STACK_SIZE = 4096;
    const DATA_ADDR = 16;
    const ptr = await exports.alloc(STACK_SIZE);
    new Int32Array(exports.memory.buffer, DATA_ADDR, 2).set([
      ptr,
      ptr + STACK_SIZE,
    ]);

    return new Sqlite(exports);
  }

  public async connect(): Promise<Connection> {
    const ptr = await this.exports.conn_new();
    return new SqliteConnection(ptr, this.exports);
  }
}

export interface Connection {
  execute(sql: string, params?: Array<Param>): Promise<void>;
  query<T>(sql: string, params?: Array<Param>): Promise<Array<T>>;
  queryRaw(sql: string, params?: Array<Param>): Promise<string>;
  drop(): Promise<void>;
}

class SqliteConnection implements Connection {
  private readonly ptr: number;
  private readonly exports: Exports;
  private readonly encoder = new TextEncoder();
  private readonly decoder = new TextDecoder();

  public constructor(ptr: number, exports: Exports) {
    this.ptr = ptr;
    this.exports = exports;
  }

  private async throwLastError(): Promise<void> {
    const m = await this.exports.conn_last_error(this.ptr);
    if (m) {
      const decoder = new TextDecoder();

      let memory = new Uint8Array(this.exports.memory.buffer);
      let len = 0;
      for (; memory[len + m] != 0; len++) {}

      const data = new Uint8Array(this.exports.memory.buffer, m, len);
      const err = decoder.decode(data);
      await this.exports.conn_last_error_drop(m);
      throw new Error(err);
    } else {
      throw new Error("unknown error");
    }
  }

  public async execute(sql: string, params?: Array<Param>): Promise<void> {
    const query = JSON.stringify({
      sql,
      params: params ?? [],
    });

    const queryOffset = await this.exports.alloc(query.length);
    this.encoder.encodeInto(
      query,
      new Uint8Array(this.exports.memory.buffer, queryOffset, query.length)
    );
    const ok = await this.exports.conn_execute(
      this.ptr,
      queryOffset,
      query.length
    );
    await this.exports.dealloc(queryOffset, query.length);
    if (!ok) {
      await this.throwLastError();
    }
  }

  public async query<T>(sql: string, params?: Array<Param>): Promise<Array<T>> {
    return JSON.parse(await this.queryRaw(sql, params));
  }

  public async queryRaw(sql: string, params?: Array<Param>): Promise<string> {
    const query = JSON.stringify({
      sql,
      params: params ?? [],
    });

    const queryOffset = await this.exports.alloc(query.length);
    this.encoder.encodeInto(
      query,
      new Uint8Array(this.exports.memory.buffer, queryOffset, query.length)
    );
    const resultPtr = await this.exports.conn_query(
      this.ptr,
      queryOffset,
      query.length
    );
    await this.exports.dealloc(queryOffset, query.length);
    if (!resultPtr) {
      await this.throwLastError();
    }

    const [resultOffset, resultLength] = new Uint32Array(
      this.exports.memory.buffer,
      resultPtr,
      2
    );
    const result = this.decoder.decode(
      new Uint8Array(this.exports.memory.buffer, resultOffset, resultLength)
    );
    await this.exports.query_result_drop(resultPtr);

    return result;
  }

  public async drop(): Promise<void> {
    await this.exports.conn_drop(this.ptr);
  }
}

const ERRNO_SUCCESS = 0;
const ERRNO_BADF = 8;
const ERRNO_INVAL = 28;
const ERRNO_NOSYS = 52;

interface Exports {
  readonly memory: WebAssembly.Memory;
  alloc(size: number): Promise<number>;
  dealloc(size: number, len: number): Promise<void>;

  conn_new(): Promise<number>;
  conn_execute(conn: number, ptr: number, len: number): Promise<number>;
  conn_query(conn: number, ptr: number, len: number): Promise<number>;
  conn_drop(conn: number): Promise<void>;
  conn_last_error(conn: number): Promise<number>;
  conn_last_error_drop(err: number): Promise<void>;

  query_result_drop(ptr: number): Promise<void>;
}

// A wrapper for console.{log,error} that tries to prevent adding unnecessary new lines.
class Log {
  private readonly isError: boolean;
  private buffer: string = "";
  private timeout: null | number = null;

  public constructor(isError: boolean) {
    this.isError = isError;
  }

  public write(s: string): void {
    if (this.timeout) {
      clearTimeout(this.timeout);
      this.timeout = null;
    }

    this.buffer += s;
    if (this.buffer.endsWith("\n")) {
      if (this.isError) {
        console.error(this.buffer);
      } else {
        console.log(this.buffer);
      }
      this.buffer = "";
    } else {
      this.timeout = setTimeout(
        () => this.write("\n"),
        500
      ) as unknown as number;
    }
  }
}
