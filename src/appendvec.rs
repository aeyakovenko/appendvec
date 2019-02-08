use memmap::MmapMut;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem;

pub struct AppendVec<T> {
    data: File,
    map: MmapMut,
    current_offset: u64,
    file_size: u64,
    _dummy: PhantomData<T>,
}

const DATA_FILE_INC_SIZE: u64 = 4 * 1024 * 1024;

impl<T> AppendVec<T>
where
    T: Default,
{
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
            current_offset: 0,
            file_size: DATA_FILE_START_SIZE,
            _dummy: Default::default(),
        }
    }

    pub fn get(&self, index: u64) -> &T {
        assert!(self.current_offset > index);
        let index = (index as usize) * mem::size_of::<T>();
        let data = &self.map[index..(index + mem::size_of::<T>())];
        let ptr = data.as_ptr() as *const _;
        unsafe { ptr.as_ref().unwrap() }
    }

    pub fn grow_file(&mut self) -> io::Result<()> {
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

    pub fn append(&mut self, val: T) -> Option<u64> {
        let index = (self.current_offset as usize) * mem::size_of::<T>();

        if (self.file_size as usize) < index + mem::size_of::<T>() {
            return None;
        }

        //info!("appending to {}", index);
        let data = &mut self.map[index..(index + mem::size_of::<T>())];
        unsafe { std::ptr::write(data.as_mut_ptr() as *mut _, val) };
        let ret = self.current_offset;
        self.current_offset += 1;
        Some(ret)
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
        let mut av = AppendVec::new("/tmp/appendvec/test_append");
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
}
