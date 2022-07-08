# `wasm-sqlite`

SQLite compiled to WASM with pluggable data storage. Useful to save SQLite in e.g. Cloudflare Durable Objects (example: https://github.com/rkusa/do-sqlite).

**Status:** This is very experimental. Don't use it for real applications yet! See the [conclusion of my blog post](https://ma.rkusa.st/store-sqlite-in-cloudflare-durable-objects#conclusion) for reasons for why not to use it.

```bash
npm install -S @rkusa/wasm-sqlite
```

## Example

```ts
const sqlite = await Sqlite.instantiate({
  pageCount(): number {
    return self.pageCount;
  },

  async getPage(ix: number): Promise<Uint8Array> {
    return (await storage.get(ix)) ?? new Uint8Array(4096);
  },

  async putPage(ix: number, page: Uint8Array): Promise<void> {
    await storage.put(ix, page);
  },

  async delPage(ix: number): Promise<void> {
    await storage.delete(ix);
    if (ix + 1 >= self.pageCount) {
      self.pageCount = ix;
    }
  },
});

const conn = await this.sqlite.connect();
await conn.execute("...", []);
const query: T = await conn.query("...", []);
```

## Build

Execute the following once:

```bash
brew install cmake ninja binaryen
./build-wasi-sdk.sh
```

After that, build it via:

```bash
npm run build
```
