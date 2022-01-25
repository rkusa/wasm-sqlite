import * as Asyncify from "asyncify-wasm";
import module from "./wasm_sqlite.wasm";

export type GetPageFn = (ix: number) => Promise<Uint8Array>;
export type PutPageFn = (ix: number, page: Uint8Array) => Promise<void>;
export type Param = string | number | boolean | null;

export default class Sqlite {
  private readonly exports: Exports;
  private readonly encoder = new TextEncoder();
  private readonly decoder = new TextDecoder();

  private constructor(exports: Exports) {
    this.exports = exports;
  }

  public static async instantiate(
    getPage: GetPageFn,
    putPage: PutPageFn
  ): Promise<Sqlite> {
    let exports: Exports;
    const instance = await Asyncify.instantiate(module, {
      wasi_snapshot_preview1: {
        // "wasi_snapshot_preview1"."random_get": [I32, I32] -> [I32]
        random_get(offset: number, length: number) {
          const buffer = new Uint8Array(exports.memory.buffer, offset, length);
          crypto.getRandomValues(buffer);

          return ERRNO_SUCCESS;
        },

        // "wasi_snapshot_preview1"."clock_time_get": [I32, I64, I32] -> [I32]
        clock_time_get() {
          throw new Error("clock_time_get not implemented");
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
                console.log(s);
                break;
              case 2: // stderr
                console.error(s);
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
          throw new Error("poll_oneoff not implemented");
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
        async get_page(ix: number): Promise<number> {
          const page = await getPage(ix);

          const offset = await exports.alloc(page.length);
          const dst = new Uint8Array(
            exports.memory.buffer,
            offset,
            page.length
          );
          dst.set(page);

          // TODO: dealloc

          return offset;
        },

        async put_page(ix: number, ptr: number) {
          const page = new Uint8Array(exports.memory.buffer, ptr, 16384);
          await putPage(ix, page);
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
    await this.exports.execute(queryOffset, query.length);
    await this.exports.dealloc(queryOffset, query.length);
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
    const resultPtr = await this.exports.query(queryOffset, query.length);
    await this.exports.dealloc(queryOffset, query.length);

    const [resultOffset, resultLength] = new Uint32Array(
      this.exports.memory.buffer,
      resultPtr,
      2
    );
    const result = this.decoder.decode(
      new Uint8Array(this.exports.memory.buffer, resultOffset, resultLength)
    );
    await this.exports.query_result_destroy(resultPtr);

    return result;
  }
}

const ERRNO_SUCCESS = 0;
const ERRNO_BADF = 8;

interface Exports {
  readonly memory: WebAssembly.Memory;
  alloc(size: number): Promise<number>;
  dealloc(size: number, len: number): Promise<void>;
  query_result_destroy(ptr: number): Promise<void>;
  execute(ptr: number, len: number): Promise<void>;
  query(ptr: number, len: number): Promise<number>;
}
