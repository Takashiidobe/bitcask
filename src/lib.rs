use crc::{self, Crc, CRC_32_CKSUM};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::fs::OpenOptions;
use std::hash::Hash;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::io::{BufReader, BufWriter};
use std::{collections::BTreeMap, fs::File};

use anyhow::Result;

pub trait Db<K, V> {
    fn get(&self, key: &K) -> Option<&V>;
    fn put(&mut self, key: K, value: V) -> Option<V>;
    fn delete(&mut self, key: &K) -> Option<V>;
}

pub trait ToDisk<K, V>: Db<K, V>
where
    K: PartialOrd + Ord + PartialEq + Eq + Hash + Debug + Serialize,
    V: Serialize + Debug,
{
    fn open(file_name: &str) -> Result<OnDisk<K, V>>;
    fn sync(&mut self) -> Result<()>;
}

pub trait FromDisk<K, V>: ToDisk<K, V>
where
    K: PartialOrd + Ord + PartialEq + Eq + Hash + Debug + Serialize,
    V: Serialize + Debug,
{
    fn hydrate(&mut self) -> Result<()>;
}

pub struct OnDisk<K, V>
where
    K: PartialOrd + Ord + PartialEq + Eq + Hash + Debug + Serialize,
    V: Serialize + Debug,
{
    data: BTreeMap<K, V>,
    file: File,
    crc_hasher: Crc<u32>,
}

impl<K, V> Drop for OnDisk<K, V>
where
    K: PartialOrd + Ord + PartialEq + Eq + Hash + Debug + Serialize,
    V: Serialize + Debug,
{
    fn drop(&mut self) {
        let _ = self.sync();
    }
}

impl<K, V> Db<K, V> for OnDisk<K, V>
where
    K: PartialOrd + Ord + PartialEq + Eq + Hash + Debug + Serialize,
    V: Serialize + Debug,
{
    fn get(&self, key: &K) -> Option<&V> {
        self.data.get(key)
    }

    fn put(&mut self, key: K, value: V) -> Option<V> {
        self.data.insert(key, value)
    }

    fn delete(&mut self, key: &K) -> Option<V> {
        self.data.remove(key)
    }
}

impl<K, V> FromDisk<K, V> for OnDisk<K, V>
where
    K: PartialOrd + Ord + PartialEq + Eq + Hash + Debug + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned + Debug,
{
    fn hydrate(&mut self) -> Result<()> {
        let mut reader = BufReader::new(&self.file);
        reader.rewind()?;

        loop {
            let mut checksum_buf = [0u8; 4];
            if reader.read_exact(&mut checksum_buf).is_err() {
                break;
            }

            let mut key_len_buf = [0u8; 8];
            reader.read_exact(&mut key_len_buf)?;

            let mut value_len_buf = [0u8; 8];
            reader.read_exact(&mut value_len_buf)?;

            let checksum: u32 = bincode::deserialize(&checksum_buf)?;
            let key_len: usize = bincode::deserialize(&key_len_buf)?;
            let value_len: usize = bincode::deserialize(&value_len_buf)?;

            let mut key_buf = vec![0u8; key_len];
            reader.read_exact(&mut key_buf)?;

            let mut value_buf = vec![0u8; value_len];
            reader.read_exact(&mut value_buf)?;

            let key: K = bincode::deserialize(&key_buf)?;
            let value: V = bincode::deserialize(&value_buf)?;

            dbg!(checksum, key_len, value_len, key, value);
        }

        Ok(())
    }
}

impl<K, V> ToDisk<K, V> for OnDisk<K, V>
where
    K: PartialOrd + Ord + PartialEq + Eq + Hash + Debug + Serialize,
    V: Serialize + Debug,
{
    fn open(file_name: &str) -> Result<OnDisk<K, V>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(file_name)?;
        Ok(Self {
            data: BTreeMap::default(),
            file,
            crc_hasher: Crc::<u32>::new(&CRC_32_CKSUM),
        })
    }

    fn sync(&mut self) -> Result<()> {
        let mut writer = BufWriter::new(&self.file);
        for (key, value) in &self.data {
            let serialized_key = bincode::serialize(key)?;
            let serialized_value = bincode::serialize(value)?;

            let mut digest = self.crc_hasher.digest();

            digest.update(&serialized_key);
            digest.update(&serialized_value);

            let checksum = digest.finalize();
            let serialized_checksum = bincode::serialize(&checksum)?;
            let serialized_key_len = bincode::serialize(&serialized_key.len())?;
            let serialized_value_len = bincode::serialize(&serialized_value.len())?;

            dbg!(
                checksum,
                serialized_key.len(),
                serialized_value.len(),
                &key,
                &value
            );

            writer.write_all(&serialized_checksum)?;
            writer.write_all(&serialized_key_len)?;
            writer.write_all(&serialized_value_len)?;
            writer.write_all(&serialized_key)?;
            writer.write_all(&serialized_value)?;
        }

        self.file.sync_all()?;

        self.data = BTreeMap::default();

        Ok(())
    }
}

pub struct InMemory<K, V> {
    data: BTreeMap<K, V>,
}

impl<K, V> Default for InMemory<K, V> {
    fn default() -> Self {
        Self {
            data: BTreeMap::default(),
        }
    }
}

impl<K, V> Db<K, V> for InMemory<K, V>
where
    K: PartialOrd + Ord + PartialEq + Eq + Hash,
{
    fn get(&self, key: &K) -> Option<&V> {
        self.data.get(key)
    }

    fn put(&mut self, key: K, value: V) -> Option<V> {
        self.data.insert(key, value)
    }

    fn delete(&mut self, key: &K) -> Option<V> {
        self.data.remove(key)
    }
}
