#![cfg_attr(feature = "unstable", feature(test))]
extern crate appendvec;
extern crate rand;
extern crate test;

use appendvec::appendvec::AppendVec;
use rand::{thread_rng, Rng};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;
use test::Bencher;

#[bench]
fn atomic_append(bencher: &mut Bencher) {
    let mut vec = AppendVec::<AtomicUsize>::new("/tmp/appendvec/bench_append");
    bencher.iter(|| {
        if vec.append(AtomicUsize::new(0)).is_none() {
            assert!(vec.grow_file().is_ok());
            assert!(vec.append(AtomicUsize::new(0)).is_some());
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
fn atomic_random_change(bencher: &mut Bencher) {
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
#[bench]
fn atomic_random_read(bencher: &mut Bencher) {
    let mut vec = AppendVec::<AtomicUsize>::new("/tmp/appendvec/bench_read");
    let size = 100_000_000;
    for _ in 0..size {
        if vec.append(AtomicUsize::new(0)).is_none() {
            assert!(vec.grow_file().is_ok());
            assert!(vec.append(AtomicUsize::new(0)).is_some());
        }
    }
    bencher.iter(|| {
        let index = thread_rng().gen_range(0, vec.len());
        let atomic1 = vec.get(index);
        let current1 = atomic1.load(Ordering::Relaxed);
        assert_eq!(current1, 0);
    });
}

#[bench]
fn concurrent_lock_append(bencher: &mut Bencher) {
    let vec = Arc::new(RwLock::new(AppendVec::<AtomicUsize>::new(
        "/tmp/appendvec/bench_lock_append",
    )));
    let vec1 = vec.clone();
    let size = 1_000_000_000;
    spawn(move || loop {
        {
            let rlock = vec1.read().unwrap();
            loop {
                if rlock.append(AtomicUsize::new(0)).is_none() {
                    break;
                }
            }
            if rlock.len() >= size {
                break;
            }
        }
        {
            let mut wlock = vec1.write().unwrap();
            if wlock.len() >= size {
                break;
            }
            assert!(wlock.grow_file().is_ok());
        }
    });
    while vec.read().unwrap().len() == 0 {
        sleep(Duration::from_millis(100));
    }
    bencher.iter(|| {
        let rlock = vec.read().unwrap();
        assert!(rlock.len() < size * 2);
    });
}

#[bench]
fn concurrent_get_append(bencher: &mut Bencher) {
    let vec = Arc::new(RwLock::new(AppendVec::<AtomicUsize>::new(
        "/tmp/appendvec/bench_get_append",
    )));
    let vec1 = vec.clone();
    let size = 1_000_000_000;
    spawn(move || loop {
        {
            let rlock = vec1.read().unwrap();
            loop {
                if rlock.append(AtomicUsize::new(0)).is_none() {
                    break;
                }
            }
            if rlock.len() >= size {
                break;
            }
        }
        {
            let mut wlock = vec1.write().unwrap();
            if wlock.len() >= size {
                break;
            }
            assert!(wlock.grow_file().is_ok());
        }
    });
    while vec.read().unwrap().len() == 0 {
        sleep(Duration::from_millis(100));
    }
    bencher.iter(|| {
        let rlock = vec.read().unwrap();
        let index = thread_rng().gen_range(0, rlock.len());
        rlock.get(index);
    });
}
