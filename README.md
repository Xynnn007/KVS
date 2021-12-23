# Simple KV Store

## Intro

follow the course [talent-plan](https://github.com/pingcap/talent-plan).

## Usage

Well, please refer to [talent-plan](https://github.com/pingcap/talent-plan) for more
information.

- [x] Project 1
- [x] Project 2
- [x] Project 3
- [x] Project 4
- [ ] Project 5

## Performance

Some strategies were applied to promote the performance, including

- Use `BufReader` and `BufWriter`, the two structs provides 
    user space buffering.

- Use `serde_json` instead of `bincode`. `json` code can be
    deserialize via a stream but not `bincode`. This good 
    code property can reduce useless memory copies. To use 
    this property, use reader and writer-like functions.