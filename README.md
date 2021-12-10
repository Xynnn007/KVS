# Simple KV Store

## Intro

follow the course [talent-plan](https://github.com/pingcap/talent-plan).

## Usage

```bash
cargo run set key1 value1

cargo run get key1
```

will get value1

```bash
cargo run rm key1

cargo run get key1
```

Will failed because key `key1` has been removed.