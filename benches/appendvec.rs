#![cfg_attr(feature = "unstable", feature(test))]
extern crate appendvec;
extern crate rand;
extern crate test;

use appendvec::appendvec::AppendVec;
use rand::{thread_rng, Rng};
use std::sync::atomic::{AtomicUsize, Ordering};
use test::Bencher;

#[bench]
fn atomic_vec(bencher: &mut Bencher) {
    let mut vec = AppendVec::<AtomicUsize>::new("/tmp/appendvec/bench_append");
    bencher.iter(|| {
        if vec.append(AtomicUsize::new(0)).is_none() {
            assert!(vec.grow_file().is_ok());
        }
    });
}
#[bench]
fn atomic_random_access(bencher: &mut Bencher) {
    let mut vec = AppendVec::<AtomicUsize>::new("/tmp/appendvec/bench_ra");
    let size = 10_000_000;
    for _ in 0..size {
        if vec.append(AtomicUsize::new(0)).is_none() {
            assert!(vec.grow_file().is_ok());
            assert!(vec.append(AtomicUsize::new(0)).is_some());
        }
    }
    bencher.iter(|| {
        let index = thread_rng().gen_range(0, size as u64);
        vec.get(index);
    });
}
#[bench]
fn atomic_random_atomic_change(bencher: &mut Bencher) {
    let mut vec = AppendVec::<AtomicUsize>::new("/tmp/appendvec/bench_rax");
    let size = 10_000_000;
    for _ in 0..size {
        if vec.append(AtomicUsize::new(0)).is_none() {
            assert!(vec.grow_file().is_ok());
            assert!(vec.append(AtomicUsize::new(0)).is_some());
        }
    }
    bencher.iter(|| {
        let index = thread_rng().gen_range(0, size as u64);
        let atomic1 = vec.get(index);
        let current1 = atomic1.load(Ordering::Relaxed);
        let next = current1 + 1;
        atomic1.store(next, Ordering::Relaxed);
        let atomic2 = vec.get(index);
        let current2 = atomic2.load(Ordering::Relaxed);
        assert_eq!(current2, next);
    });
}
