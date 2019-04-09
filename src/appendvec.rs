use memmap::MmapMut;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

macro_rules! align_up {
    ($addr: expr, $align: expr) => {
        ($addr + ($align - 1)) & !($align - 1)
    };
}

#[derive(PartialEq, Default, Debug)]
pub struct Account {
    pub lamports: u64,
    pub data: Vec<u8>,
}

pub struct AppendVec {
    map: MmapMut,
    append_offset: Mutex<usize>,
    current_len: AtomicUsize,
    file_size: u64,
}

impl AppendVec {
    pub fn new(file: &str, size: usize) -> Self {
        let mut data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file)
            .expect("Unable to open data file");

        data.seek(SeekFrom::Start(size as u64)).unwrap();
        data.write_all(&[0]).unwrap();
        data.seek(SeekFrom::Start(0)).unwrap();
        data.flush().unwrap();
        let map = unsafe { MmapMut::map_mut(&data).expect("failed to map the data file") };

        AppendVec {
            map,
            append_offset: Mutex::new(0),
            current_len: AtomicUsize::new(0),
            file_size: size as u64,
        }
    }

    pub fn len(&self) -> usize {
        self.current_len.load(Ordering::Relaxed)
    }

    pub fn capacity(&self) -> u64 {
        self.file_size
    }

    fn get_slice(&self, offset: usize, size: usize) -> &mut [u8] {
        let len = self.len();
        assert!(len >= offset + size);
        let data = &self.map[offset..offset + size];
        unsafe {
            let dst = std::mem::transmute::<*const u8, *mut u8>(data.as_ptr());
            std::slice::from_raw_parts_mut(dst, size)
        }
    }

    fn append_ptr(&self, offset: &mut usize, src: *const u8, len: usize) {
        let pos = align_up!(*offset as usize, mem::size_of::<u64>());
        let data = &self.map[pos..(pos + len)];
        unsafe {
            let dst = std::mem::transmute::<*const u8, *mut u8>(data.as_ptr());
            std::ptr::copy(src, dst, len);
        };
        *offset = pos + len;
    }
    fn append_ptrs(&self, vals: &[(*const u8, usize)]) -> Option<usize> {
        let mut offset = self.append_offset.lock().unwrap();
        let mut end = *offset;
        for val in vals {
            end = align_up!(end, mem::size_of::<u64>());
            end += val.1;
        }

        if (self.file_size as usize) <= end {
            return None;
        }

        let pos = align_up!(*offset, mem::size_of::<u64>());
        for val in vals {
            self.append_ptr(&mut offset, val.0, val.1)
        }
        self.current_len.store(*offset, Ordering::Relaxed);
        Some(pos)
    }

    pub fn get_account(&self, offset: usize) -> &Account {
        let account: *mut Account = {
            let data = self.get_slice(offset, mem::size_of::<Account>());
            unsafe { std::mem::transmute::<*const u8, *mut Account>(data.as_ptr()) }
        };
        let data_at = align_up!(offset + mem::size_of::<Account>(), mem::size_of::<u64>());
        let account_ref: &mut Account =
            unsafe { std::mem::transmute::<*mut Account, &mut Account>(account) };
        let data = self.get_slice(data_at, account_ref.data.len());
        unsafe {
            let mut new_data = Vec::from_raw_parts(data.as_mut_ptr(), data.len(), data.len());
            std::mem::swap(&mut account_ref.data, &mut new_data);
            std::mem::forget(new_data);
        };
        account_ref
    }

    pub fn accounts(&self, mut start: usize) -> Vec<&Account> {
        let mut accounts = vec![];
        loop {
            let end = align_up!(start + mem::size_of::<Account>(), mem::size_of::<u64>());
            if end > self.len() {
                break;
            }
            let first = self.get_account(start);
            accounts.push(first);
            let data_at = align_up!(start + mem::size_of::<Account>(), mem::size_of::<u64>());
            let next = align_up!(data_at + first.data.len(), mem::size_of::<u64>());
            start = next;
        }
        accounts
    }

    pub fn append_account(&self, account: &Account) -> Option<usize> {
        unsafe {
            let acc_ptr = account as *const Account;
            let data_len = account.data.len();
            let data_ptr = account.data.as_ptr();
            let ptrs = [
                (
                    std::mem::transmute::<*const Account, *const u8>(acc_ptr),
                    mem::size_of::<Account>(),
                ),
                (data_ptr, data_len),
            ];
            self.append_ptrs(&ptrs)
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use rand::{thread_rng, Rng};
    use std::time::Instant;
    use timing::duration_as_ms;

    #[test]
    fn test_append_vec() {
        let av = AppendVec::new("/tmp/appendvec/test_append", 1024 * 1024);
        let val = Account {
            lamports: 5,
            data: vec![],
        };
        let index = av.append_account(&val).unwrap();
        assert_eq!(*av.get_account(index), val);
        let val1 = Account {
            lamports: 6,
            data: vec![],
        };
        let index1 = av.append_account(&val1).unwrap();
        assert_eq!(*av.get_account(index), val);
        assert_eq!(*av.get_account(index1), val1);
    }

    #[test]
    fn test_append_vec_data() {
        let av = AppendVec::new("/tmp/appendvec/test_append2", 1024 * 1024);
        let val = Account {
            lamports: 5,
            data: vec![1, 2, 3],
        };
        let index = av.append_account(&val).unwrap();
        let account = av.get_account(index);
        assert_eq!(*account, val);
        let val1 = Account {
            lamports: 6,
            data: vec![4, 5, 6],
        };
        let index1 = av.append_account(&val1).unwrap();
        assert_eq!(*av.get_account(index), val);
        assert_eq!(*av.get_account(index1), val1);
    }
    fn test_account(ix: usize) -> Account {
        let data_len = ix % 256;
        Account {
            lamports: ix as u64,
            data: (0..data_len).into_iter().map(|_| data_len as u8).collect(),
        }
    }
    #[test]
    fn test_grow_append_vec() {
        let av = AppendVec::new("/tmp/appendvec/test_grow", 1024 * 1024);

        let size = 1000;
        let mut indexes = vec![];
        let now = Instant::now();
        for ix in 0..size {
            let val = test_account(ix);
            let pos = av.append_account(&val).unwrap();
            assert_eq!(*av.get_account(pos), val);
            indexes.push(pos)
        }
        println!("append time: {} ms", duration_as_ms(&now.elapsed()),);

        let now = Instant::now();
        for _ in 0..size {
            let ix = thread_rng().gen_range(0, indexes.len());
            let val = test_account(ix);
            assert_eq!(*av.get_account(indexes[ix]), val);
        }
        println!("random read time: {} ms", duration_as_ms(&now.elapsed()),);

        let now = Instant::now();
        assert_eq!(indexes.len(), size);
        assert_eq!(indexes[0], 0);
        let accounts = av.accounts(indexes[0]);
        assert_eq!(accounts.len(), size);
        for (ix, v) in accounts.iter().enumerate() {
            let val = test_account(ix);
            assert_eq!(**v, val)
        }
        println!(
            "sequential read time: {} ms",
            duration_as_ms(&now.elapsed()),
        );
    }
}
