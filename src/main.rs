use anyhow::Result;
use bitcask::{Db, FromDisk, OnDisk, ToDisk};

fn main() -> Result<()> {
    let mut on_disk = OnDisk::open("hello.db")?;
    on_disk.put("a".to_string(), 1);
    on_disk.put("b".to_string(), 2);
    on_disk.sync()?;

    on_disk.put("c".to_string(), 3);
    on_disk.put("d".to_string(), 4);

    on_disk.sync()?;
    on_disk.hydrate()?;

    Ok(())
}
