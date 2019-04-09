#![cfg_attr(feature = "unstable", feature(test))]
extern crate appendvec;
extern crate rand;
extern crate test;

use appendvec::appendvec::{Account, AppendVec};
use rand::{thread_rng, Rng};
use std::sync::{Arc, RwLock};
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
    let vec = AppendVec::new("/tmp/appendvec/bench_append");
    bencher.iter(|| {
        let val = test_account(0);
        assert!(vec.append_account(&val).is_some());
    });
}

#[bench]
fn sequential_read(bencher: &mut Bencher) {
    let vec = AppendVec::new("/tmp/appendvec/bench_ra");
    let size = 1_000;
    let mut indexes = vec![];
    for ix in 0..size {
        let val = test_account(ix);
        let ix = vec.append_account(&val).unwrap();
        indexes.push(ix)
    }
    bencher.iter(|| {
        let ix = indexes.pop().unwrap();
        let account = vec.get_account(ix);
        let test = test_account(ix);
        assert_eq!(*account, test);
        indexes.push(ix);
    });
}
#[bench]
fn random_read(bencher: &mut Bencher) {
    let vec = AppendVec::new("/tmp/appendvec/bench_rax");
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
    let vec = Arc::new(RwLock::new(AppendVec::new(
        "/tmp/appendvec/bench_lock_append_read",
    )));
    let vec1 = vec.clone();
    let size = 1_000;
    spawn(move || loop {
        {
            let rlock = vec1.read().unwrap();
            loop {
                let account = test_account(0);
                if rlock.append_account(&account).is_none() {
                    break;
                }
            }
            if rlock.len() >= size {
                break;
            }
        }
    });
    while vec.read().unwrap().len() == 0 {
        sleep(Duration::from_millis(100));
    }
    bencher.iter(|| {
        let rlock = vec.read().unwrap();
        for acc in rlock.accounts(0) {
            assert_eq!(acc.data.len(), 0);
        }
    });
}
