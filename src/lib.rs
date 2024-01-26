#![feature(let_chains)]
#![feature(fs_try_exists)]

use crc::{self, Crc, CRC_32_CKSUM};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::fs;
use std::fs::OpenOptions;
use std::hash::Hash;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::marker::PhantomData;
use std::{collections::BTreeMap, fs::File};

use anyhow::Result;

pub trait Db<K, V> {
    fn get(&self, key: &K) -> Option<V>;
    fn put(&mut self, key: K, value: V) -> Result<V>;
    fn delete(&mut self, key: &K) -> Result<()>;
    fn keys(&mut self) -> Vec<&K>;
    fn values(&mut self) -> Vec<V>;
    fn items(&mut self) -> Vec<(&K, V)>;
}

pub trait ToDisk<K, V>: Db<K, V>
where
    K: PartialOrd + Ord + PartialEq + Eq + Hash + Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned,
{
    fn open(file_name: &str) -> Result<OnDisk<K, V>>;
    fn sync(&mut self) -> Result<()>;
    fn prune(&mut self) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Slot {
    file_id: u64,
    start: u64,
    end: u64,
}

pub struct OnDisk<K, V>
where
    K: PartialOrd + Ord + PartialEq + Eq + Hash + Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned,
{
    key_dir: BTreeMap<K, (u64, usize, u64, Slot)>,
    delete_map: BTreeMap<K, (u64, usize, u64, Slot)>,
    prefix: String,
    file_id: u64,
    file_position: u64,
    crc_hasher: Crc<u32>,
    is_dirty: bool,
    phantom_data: PhantomData<V>,
    free_slots: BTreeMap<u64, Vec<Slot>>,
}

impl<K, V> OnDisk<K, V>
where
    K: PartialOrd + Ord + PartialEq + Eq + Hash + Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned,
{
    fn get_file_by_id(&self, file_id: u64) -> Result<File> {
        let file_name = format!("{}.{}.db", self.prefix, file_id);
        let file = OpenOptions::new().read(true).write(true).open(file_name)?;
        Ok(file)
    }

    fn get_tempfile_by_id(&self, file_id: u64) -> Result<File> {
        let file_name = format!("{}.{}.temp.db", self.prefix, file_id);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_name)?;
        Ok(file)
    }

    fn curr_file(&self) -> Result<File> {
        let file_name = format!("{}.{}.db", self.prefix, self.file_id);
        let file = OpenOptions::new().read(true).write(true).open(file_name)?;
        Ok(file)
    }

    fn serialize_to_file(&self, key: &K, value: V, file: File) -> Result<(u64, usize, u64, Slot)> {
        let serialized_key = bincode::serialize(&key)?;
        let serialized_value = bincode::serialize(&value)?;
        let serialized_key_len = bincode::serialize(&serialized_key.len())?;
        let serialized_value_len = bincode::serialize(&serialized_value.len())?;

        let mut digest = self.crc_hasher.digest();

        digest.update(&serialized_key_len);
        digest.update(&serialized_value_len);
        digest.update(&serialized_key);
        digest.update(&serialized_value);

        let checksum = digest.finalize();
        let serialized_checksum = bincode::serialize(&checksum)?;

        let mut writer = BufWriter::new(file);
        writer.seek(SeekFrom::End(0))?;
        let start_pos = writer.stream_position()?;

        writer.write_all(&serialized_checksum)?;
        writer.write_all(&serialized_key_len)?;
        writer.write_all(&serialized_value_len)?;
        writer.write_all(&serialized_key)?;
        let value_pos = writer.stream_position()?;
        writer.write_all(&serialized_value)?;

        let end_pos = writer.stream_position()?;
        let free_slot = Slot {
            file_id: self.file_id,
            start: start_pos,
            end: end_pos,
        };
        Ok((self.file_id, serialized_value.len(), value_pos, free_slot))
    }
}

impl<K, V> Drop for OnDisk<K, V>
where
    K: PartialOrd + Ord + PartialEq + Eq + Hash + Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned,
{
    fn drop(&mut self) {
        let _ = self.sync();
    }
}

impl<K, V> Db<K, V> for OnDisk<K, V>
where
    K: PartialOrd + Ord + PartialEq + Eq + Hash + Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned,
{
    fn get(&self, key: &K) -> Option<V> {
        if let Some((file_id, value_len, value_pos, _)) = self.key_dir.get(key) {
            let mut reader = self
                .get_file_by_id(*file_id)
                .expect("failed to get file_id");
            reader
                .seek(SeekFrom::Start(*value_pos))
                .expect("failed to seek");

            let mut value_buf = vec![0u8; *value_len];
            reader
                .read_exact(&mut value_buf)
                .expect("failed to read value");
            let value: V = bincode::deserialize(&value_buf).expect("Failed to deserialize value");

            Some(value)
        } else {
            None
        }
    }

    fn put(&mut self, key: K, value: V) -> Result<V> {
        if self.key_dir.contains_key(&key) {
            self.delete(&key)?;
        }
        let serialized_key = bincode::serialize(&key)?;
        let serialized_value = bincode::serialize(&value)?;
        let serialized_key_len = bincode::serialize(&serialized_key.len())?;
        let serialized_value_len = bincode::serialize(&serialized_value.len())?;

        let mut digest = self.crc_hasher.digest();

        digest.update(&serialized_key_len);
        digest.update(&serialized_value_len);
        digest.update(&serialized_key);
        digest.update(&serialized_value);

        let checksum = digest.finalize();
        let serialized_checksum = bincode::serialize(&checksum)?;

        let total_len = (serialized_key.len()
            + serialized_value.len()
            + serialized_key_len.len()
            + serialized_value_len.len()
            + serialized_checksum.len()) as u64;

        let mut items = self.free_slots.range(total_len..);

        if let Some((length, free_slots)) = items.next()
            && let Some(free_slot) = free_slots.last()
        {
            let file = self.get_file_by_id(free_slot.file_id)?;
            let mut writer = BufWriter::new(file);
            writer.seek(SeekFrom::Start(free_slot.start))?;

            writer.write_all(&serialized_checksum)?;
            writer.write_all(&serialized_key_len)?;
            writer.write_all(&serialized_value_len)?;
            writer.write_all(&serialized_key)?;
            let value_pos = writer.stream_position()?;
            writer.write_all(&serialized_value)?;

            let end_pos = writer.stream_position()?;
            let free_slot = Slot {
                file_id: free_slot.file_id,
                start: free_slot.start,
                end: end_pos,
            };
            self.key_dir.insert(
                key,
                (
                    free_slot.file_id,
                    serialized_value.len(),
                    value_pos,
                    free_slot.clone(),
                ),
            );
            let mut free_slots = free_slots.clone();
            free_slots.pop();
            self.free_slots.insert(*length, free_slots);
            self.file_position = end_pos;
            self.is_dirty = true;
        } else {
            let file = self.curr_file()?;
            let mut writer = BufWriter::new(file);
            writer.seek(SeekFrom::Start(self.file_position))?;

            writer.write_all(&serialized_checksum)?;
            writer.write_all(&serialized_key_len)?;
            writer.write_all(&serialized_value_len)?;
            writer.write_all(&serialized_key)?;
            let value_pos = writer.stream_position()?;
            writer.write_all(&serialized_value)?;

            let end_pos = writer.stream_position()?;
            let free_slot = Slot {
                file_id: self.file_id,
                start: self.file_position,
                end: end_pos,
            };
            self.key_dir.insert(
                key,
                (
                    self.file_id,
                    serialized_value.len(),
                    value_pos,
                    free_slot.clone(),
                ),
            );
            self.file_position = end_pos;
            self.is_dirty = true;
        }

        Ok(value)
    }

    fn delete(&mut self, key: &K) -> Result<()> {
        if let Some((file_id, value_len, value_pos, free_slot)) = self.key_dir.remove(key) {
            let distance = free_slot.end - free_slot.start;
            self.free_slots
                .entry(distance)
                .or_default()
                .push(free_slot.clone());
            self.delete_map
                .insert(key.clone(), (file_id, value_len, value_pos, free_slot));
        }
        Ok(())
    }

    fn keys(&mut self) -> Vec<&K> {
        let keys: Vec<_> = self.key_dir.keys().collect();
        keys
    }

    fn values(&mut self) -> Vec<V> {
        let mut values = vec![];
        for value in self.key_dir.keys() {
            if let Some(v) = self.get(value) {
                values.push(v);
            }
        }
        values
    }

    fn items(&mut self) -> Vec<(&K, V)> {
        let mut items = vec![];
        self.key_dir.keys().for_each(|k| {
            items.push((k, self.get(k).expect("Could not find key")));
        });
        items
    }
}

impl<K, V> ToDisk<K, V> for OnDisk<K, V>
where
    K: PartialOrd + Ord + PartialEq + Eq + Hash + Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned,
{
    fn open(file_name: &str) -> Result<OnDisk<K, V>> {
        let db_name = format!("{}.{}.db", file_name, 1);
        let _ = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_name)?;
        Ok(Self {
            key_dir: BTreeMap::default(),
            prefix: file_name.to_string(),
            file_id: 1,
            crc_hasher: Crc::<u32>::new(&CRC_32_CKSUM),
            phantom_data: PhantomData,
            file_position: 0,
            is_dirty: false,
            free_slots: BTreeMap::default(),
            delete_map: BTreeMap::default(),
        })
    }

    fn sync(&mut self) -> Result<()> {
        if self.is_dirty {
            self.file_id += 1;
            let db_name = format!("{}.{}.db", self.prefix, self.file_id);
            let _ = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(db_name)?;
            self.file_position = 0;
            self.is_dirty = false;
        }
        Ok(())
    }

    fn prune(&mut self) -> Result<()> {
        // for every file in 1..self.file_id
        // we want to iterate through and copy
        if self.is_dirty {
            let mut files_to_swap = BTreeSet::new();
            let mut new_key_dir = BTreeMap::new();
            if self.key_dir.is_empty() {
                for f_id in 2..=self.file_id {
                    fs::remove_file(format!("{}.{}.db", self.prefix, f_id))?;
                }
            }
            for (key, (file_id, value_len, value_pos, Slot { .. })) in &self.key_dir {
                let tempfile = self.get_tempfile_by_id(*file_id)?;
                let file = self.get_file_by_id(*file_id)?;

                let mut reader = BufReader::new(file);
                reader.seek(SeekFrom::Start(*value_pos))?;

                let mut value_buf = vec![0u8; *value_len];
                reader.read_exact(&mut value_buf)?;

                let value: V = bincode::deserialize(&value_buf)?;

                // then write it to tempfile
                let (file_id, value_len, value_pos, new_slot) =
                    self.serialize_to_file(key, value, tempfile)?;
                new_key_dir.insert(key.clone(), (file_id, value_len, value_pos, new_slot));

                // Finally, swap tempfile and file
                files_to_swap.insert(file_id);
            }

            for file_id in files_to_swap {
                let temp_file_path = format!("{}.{}.temp.db", self.prefix, file_id);
                let file_path = format!("{}.{}.db", self.prefix, file_id);
                if fs::try_exists(&temp_file_path).is_ok() {
                    fs::rename(temp_file_path, file_path)?;
                }
            }

            self.delete_map = BTreeMap::new();
            self.key_dir = new_key_dir;
            self.is_dirty = false;
        }

        Ok(())
    }
}

use Op::*;
#[derive(Debug, Clone)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Op<K, V> {
    Put { key: K, value: V },
    Delete { key: K },
    Update { key: K, value: V },
    Prune,
    Sync,
}

pub fn eval_op<K, V>(db: &mut OnDisk<K, V>, op: Op<K, V>)
where
    K: PartialOrd + Ord + PartialEq + Eq + Hash + Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned,
{
    match op {
        Put { key, value } => {
            db.put(key, value).unwrap();
        }
        Delete { key } => {
            db.delete(&key).unwrap();
        }
        Update { key, value } => {
            db.put(key, value).unwrap();
        }
        Prune => {
            db.prune().unwrap();
        }
        Sync => db.sync().unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crash_1() {
        let mut db: OnDisk<String, u64> = OnDisk::open("crash_1").unwrap();

        let instructions = vec![
            Delete {
                key: "".to_string(),
            },
            Delete {
                key: "".to_string(),
            },
            Put {
                key: "\0".to_string(),
                value: 10344644575756526533u64,
            },
            Put {
                key: "\u{1}".to_string(),
                value: 72339073326448897u64,
            },
            Put {
                key: "\u{1}".to_string(),
                value: 72057594037928193u64,
            },
            Put {
                key: "".to_string(),
                value: 0,
            },
        ];

        for instruction in instructions {
            eval_op(&mut db, instruction);
        }

        assert!(db.sync().is_ok());
    }

    #[test]
    fn crash_2() {
        let mut db: OnDisk<String, u64> = OnDisk::open("crash_2").unwrap();

        let instructions = vec![
            Put {
                key: "\0".to_string(),
                value: 16969173279757565696,
            },
            Sync,
            Prune,
        ];

        for instruction in instructions {
            eval_op(&mut db, instruction);
        }

        assert!(db.sync().is_ok());
    }
}
