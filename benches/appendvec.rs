#![cfg_attr(feature = "unstable", feature(test))]
extern crate appendvec;
extern crate test;

use appendvec::appendvec::AppendVec;
use std::sync::atomic::AtomicUsize;
use test::Bencher;

#[bench]
fn atomic_vec(bencher: &mut Bencher) {
    let mut vec = AppendVec::<AtomicUsize>::new();
    bencher.iter(|| {
        if vec.append(AtomicUsize::new(0)).is_none() {
            assert!(vec.grow_file().is_ok());
        }
    });
}
