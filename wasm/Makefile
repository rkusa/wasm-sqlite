ifndef DEBUG
	cargo_build_flags += --release
endif

.PHONY: build
build:
	PATH="$(shell pwd)/wasi-sdk/dist/wasi-sdk-16.0/bin:${PATH}" \
	CFLAGS="--sysroot=$(shell pwd)/wasi-sdk/dist/wasi-sdk-16.0/share/wasi-sysroot" \
		cargo build --target wasm32-wasi $(cargo_build_flags)
