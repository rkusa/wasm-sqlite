#!/bin/sh
set -e

rm -rf ./dist/*
mkdir -p dist


cd wasm
make build
cd ..

./node_modules/.bin/esbuild --format=esm --platform=neutral --external:"*.wasm" \
  --outfile=./dist/wasm-sqlite.js --bundle --main-fields=module src/lib.ts

./node_modules/.bin/tsc --emitDeclarationOnly

wasm-opt -Os --asyncify --pass-arg asyncify-imports@env.put_page,env.get_page,env.del_page,env.conn_sleep \
  wasm/target/wasm32-wasi/release/wasm_sqlite.wasm \
  -o dist/wasm_sqlite.wasm
