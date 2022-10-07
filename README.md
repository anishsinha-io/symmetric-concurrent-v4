### (Currently incomplete) Implementation of an RDBMS in Rust

Currently complete WITH FULL TEST COVERAGE:

- [x] Concurrency API, including implementations of Latches, RwLatches, and Binary Semaphores
- [x] DiskMgr
- [x] Disk FS API
- [x] Memory IO API (encode/decode)
- [x] Constant definitions

Incomplete:

- [ ] Buffer pool manager 
- [ ] PostgreSQL-like Lanin + Shasha/Lehman and Yao index
- [ ] Query execution engine
- [ ] Shell

...among others

### Build Instructions

- `git clone` this repo
- `cd` into the folder you cloned this into
- run `cargo test` to test everything
- start hacking!

The main program doesn't do anything yet. Once the remaining modules are built that will change.

This is FOSS. I haven't added the header to every file but this is the license: 

The author disclaims copyright to this source code. In place of a legal notice, here is a blessing:

- May you do good and not evil.
- May you find forgiveness for yourself and forgive others.
- May you share freely, never taking more than you give.

<hr/>

https://spdx.org/licenses/blessing.html