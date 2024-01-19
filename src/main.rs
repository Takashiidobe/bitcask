use anyhow::Result;
use bitcask::{Db, OnDisk, ToDisk};

fn main() -> Result<()> {
    let mut on_disk = OnDisk::open("hello")?;
    {
        on_disk.put("a".to_string(), 1)?;
        on_disk.put("b".to_string(), 2)?;
        on_disk.put("c".to_string(), 3)?;
        on_disk.put("d".to_string(), 4)?;
        on_disk.put("e".to_string(), 5)?;
        on_disk.put("f".to_string(), 6)?;
        on_disk.put("g".to_string(), 7)?;
        on_disk.delete(&"a".to_string())?;
        on_disk.delete(&"b".to_string())?;
        on_disk.delete(&"c".to_string())?;
        on_disk.delete(&"d".to_string())?;
        on_disk.delete(&"e".to_string())?;
        on_disk.delete(&"f".to_string())?;
        on_disk.delete(&"g".to_string())?;
        on_disk.prune()?;
        dbg!(on_disk.keys());
        dbg!(on_disk.values());
        dbg!(on_disk.items());
    }

    Ok(())
}
