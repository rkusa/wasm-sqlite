import * as Asyncify from "https://unpkg.com/asyncify-wasm?module";
import Context from "https://deno.land/std@0.176.0/wasi/snapshot_preview1.ts";

export type Param = string | number | boolean | null;

export interface Vfs {
  pageCount(): number;
  getPage(ix: number): Promise<Uint8Array>;
  putPage(ix: number, page: Uint8Array): Promise<void>;
  delPage(ix: number): Promise<void>;
}

const context = new Context({
  args: Deno.args,
  env: Deno.env.toObject(),
});

export class Sqlite {
  private readonly exports: Exports;

  private constructor(exports: Exports) {
    this.exports = exports;
  }

  public static async instantiate(vfs: Vfs): Promise<Sqlite> {
    let exports: Exports;

    const instance = await Asyncify.instantiateStreaming(
      fetch(new URL("../dist/wasm_sqlite.wasm", import.meta.url)),
      {
        wasi_snapshot_preview1: context.exports,

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
      }
    );
    context.initialize(instance.instance);
    exports = instance.instance.exports as unknown as Exports;

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
