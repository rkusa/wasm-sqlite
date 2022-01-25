# `wasm-sqlite`

SQLite compiled to WASM with a pluggable data storage. Useful to save SQLite in e.g. Cloudflare Durable Objects (example: https://github.com/rkusa/do-sqlite).

**Status:** This is very experimental. Don't use it for real applications yet! See the [conclusion of my blog post](https://ma.rkusa.st/store-sqlite-in-cloudflare-durable-objects#conclusion) for reasons for why not to use it.

```bash
npm install -S wasm-sqlite
```

## Example

```ts
const sqlite = await Sqlite.instantiate(
  // get page
  async (ix: number) => {
    return await storage.get(ix) ?? new Uint8Array(4096);
  },

  // put page
  async (ix: number, page: Uint8Array) => {
    await storage.put(ix, page);
  }
);

await sqlite.execute("...", []);
const query: T = await sqlite.query("...", []);
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
