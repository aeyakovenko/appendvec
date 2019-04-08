use memmap::MmapMut;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek, SeekFrom, Write};
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

macro_rules! align_up {
    ($addr: expr, $align: expr) => {
        ($addr + ($align - 1)) & !($align - 1)
    };
}

pub struct Account {
    pub lamports: u64,
    pub data: Vec<u8>,
}

pub struct AppendVec {
    data: File,
    map: MmapMut,
    append_offset: Mutex<usize>,
    current_len: AtomicUsize,
    file_size: u64,
}

const DATA_FILE_INC_SIZE: u64 = 4 * 1024 * 1024;

impl AppendVec {
    pub fn new(file: &str) -> Self {
        const DATA_FILE_START_SIZE: u64 = 16 * 1024 * 1024;
        let mut data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file)
            .expect("Unable to open data file");

        data.seek(SeekFrom::Start(DATA_FILE_START_SIZE)).unwrap();
        data.write_all(&[0]).unwrap();
        data.seek(SeekFrom::Start(0)).unwrap();
        data.flush().unwrap();
        let map = unsafe { MmapMut::map_mut(&data).expect("failed to map the data file") };

        AppendVec {
            data,
            map,
            append_offset: Mutex::new(0),
            current_len: AtomicUsize::new(0),
            file_size: DATA_FILE_START_SIZE,
        }
    }

    pub fn len(&self) -> usize {
        self.current_len.load(Ordering::Relaxed)
    }

    pub fn capacity(&self) -> u64 {
        self.file_size
    }

    fn get_slice(&self, offset: usize, size: usize) -> &mut [u8] {
        let len = self.current_len.load(Ordering::Relaxed);
        assert!(len >= offset + size);
        let data = &self.map[offset..offset + size];
        unsafe {
            let dst = std::mem::transmute::<*const u8, *mut u8>(data.as_ptr());
            std::slice::from_raw_parts_mut(dst, size)
        }
    }

    // grow the file
    // must be exclusive to read and append and itself
    pub fn grow_file(&mut self, size: usize) -> io::Result<()> {
        let append_offset = self.append_offset.lock().unwrap();
        let offset = *append_offset + size;
        if offset as u64 + DATA_FILE_INC_SIZE < self.file_size {
            // grow was already called
            return Ok(());
        }
        let end = self.file_size + DATA_FILE_INC_SIZE;
        drop(&self.map);
        self.data.seek(SeekFrom::Start(end))?;
        self.data.write_all(&[0])?;
        self.data.seek(SeekFrom::Start(0))?;
        self.data.flush()?;
        self.map = unsafe { MmapMut::map_mut(&self.data)? };
        self.file_size = end;
        Ok(())
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

        if (self.file_size as usize) < end {
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
            account_ref.data = Vec::from_raw_parts(data.as_mut_ptr(), data.len(), data.len());
        };
        account_ref
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
    use timing::{duration_as_ms, duration_as_s};

    #[test]
    fn test_append_vec() {
        let av = AppendVec::new("/tmp/appendvec/test_append");
        let val: u64 = 5;
        let index = av.append(val).unwrap();
        assert_eq!(*av.get(index), val);
        let val1 = val + 1;
        let index1 = av.append(val1).unwrap();
        assert_eq!(*av.get(index), val);
        assert_eq!(*av.get(index1), val1);
    }

    #[test]
    fn test_grow_append_vec() {
        let mut av = AppendVec::new("/tmp/appendvec/test_grow");
        //let mut val: u64 = 5;
        let mut val = [5u64; 32];
        let size = 100_000;

        let now = Instant::now();
        for _ in 0..size {
            if av.append(val).is_none() {
                assert!(av.grow_file().is_ok());
                assert!(av.append(val).is_some());
            }
            val[0] += 1;
        }
        println!(
            "time: {} ms {} / s",
            duration_as_ms(&now.elapsed()),
            ((mem::size_of::<[u64; 32]>() * size) as f32) / duration_as_s(&now.elapsed()),
        );

        let now = Instant::now();
        let num_reads = 100_000;
        for _ in 0..num_reads {
            let index = thread_rng().gen_range(0, size as u64);
            assert_eq!(av.get(index)[0], index + 5);
        }
        println!(
            "time: {} ms {} / s",
            duration_as_ms(&now.elapsed()),
            (num_reads as f32) / duration_as_s(&now.elapsed()),
        );
    }
    #[test]
    fn random_atomic_change() {
        let mut vec = AppendVec::<AtomicUsize>::new("/tmp/appendvec/test_rax");
        let size = 1_000;
        for _ in 0..size {
            if vec.append(AtomicUsize::new(0)).is_none() {
                assert!(vec.grow_file().is_ok());
                assert!(vec.append(AtomicUsize::new(0)).is_some());
            }
        }
        let index = thread_rng().gen_range(0, size as u64);
        let atomic1 = vec.get(index);
        let current1 = atomic1.load(Ordering::Relaxed);
        let next = current1 + 1;
        atomic1.store(next, Ordering::Relaxed);
        let atomic2 = vec.get(index);
        let current2 = atomic2.load(Ordering::Relaxed);
        assert_eq!(current2, next);
    }
}
