#![no_main]

use bitcask::{eval_op, Op};
use bitcask::{Db, OnDisk, ToDisk};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|methods: Vec<Op<String, u64>>| {
    let mut db = OnDisk::open("test").unwrap();
    for method in methods {
        eval_op(&mut db, method);
    }
});
