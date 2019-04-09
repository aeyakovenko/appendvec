#![cfg_attr(feature = "unstable", feature(test))]
extern crate appendvec;
extern crate rand;
extern crate test;

use appendvec::appendvec::{Account, AppendVec};
use rand::{thread_rng, Rng};
use std::sync::Arc;
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;
use test::Bencher;

fn test_account(ix: usize) -> Account {
    let data_len = ix % 256;
    Account {
        lamports: ix as u64,
        data: (0..data_len).into_iter().map(|_| data_len as u8).collect(),
    }
}

#[bench]
fn append(bencher: &mut Bencher) {
    let vec = AppendVec::new("/tmp/appendvec/bench_append", 2*1024 * 1024 * 1024);
    bencher.iter(|| {
        let val = test_account(0);
        assert!(vec.append_account(&val).is_some());
    });
}

#[bench]
fn sequential_read(bencher: &mut Bencher) {
    let vec = AppendVec::new("/tmp/appendvec/bench_ra", 128 * 1024 * 1024);
    let size = 1_000;
    let mut indexes = vec![];
    for ix in 0..size {
        let val = test_account(ix);
        let pos = vec.append_account(&val).unwrap();
        indexes.push((ix, pos))
    }
    bencher.iter(|| {
        let (ix, pos) = indexes.pop().unwrap();
        let account = vec.get_account(pos);
        let test = test_account(ix);
        assert_eq!(*account, test);
        indexes.push((ix, pos));
    });
}
#[bench]
fn random_read(bencher: &mut Bencher) {
    let vec = AppendVec::new("/tmp/appendvec/bench_rax", 128 * 1024 * 1024);
    let size = 1_000;
    let mut indexes = vec![];
    for ix in 0..size {
        let val = test_account(ix);
        let pos = vec.append_account(&val).unwrap();
        indexes.push(pos)
    }
    bencher.iter(|| {
        let random_index: usize = thread_rng().gen_range(0, indexes.len());
        let ix = &indexes[random_index];
        let account = vec.get_account(*ix);
        let test = test_account(*ix);
        assert_eq!(*account, test);
    });
}

#[bench]
fn concurrent_lock_append_read(bencher: &mut Bencher) {
    let vec = Arc::new(AppendVec::new(
        "/tmp/appendvec/bench_lock_append_read",
        1024 * 1024 * 1024,
    ));
    let vec1 = vec.clone();
    spawn(move || loop {
        let account = test_account(0);
        if vec1.append_account(&account).is_none() {
            break;
        }
    });
    while vec.len() == 0 {
        sleep(Duration::from_millis(100));
    }
    bencher.iter(|| {
        for acc in vec.accounts(0) {
            assert_eq!(acc.data.len(), 0);
        }
    });
}
