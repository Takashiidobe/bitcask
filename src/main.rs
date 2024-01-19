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
        on_disk.put("xddd".to_string(), 6)?;
        on_disk.put("boblol".to_string(), 7)?;
        on_disk.delete(&"a".to_string())?;
        on_disk.delete(&"b".to_string())?;
        on_disk.put("thing".to_string(), 10)?;
        on_disk.put("some".to_string(), 20)?;
        dbg!(on_disk.keys());
        dbg!(on_disk.values());
        dbg!(on_disk.items());
    }

    Ok(())
}
