use anyhow::Result;
use bitcask::{Db, OnDisk, ToDisk};
use rustyline::{error::ReadlineError, DefaultEditor};

fn main() -> Result<()> {
    let mut db = OnDisk::open("test")?;
    let mut rl = DefaultEditor::new()?;

    rl.load_history("history.txt").unwrap_or(());

    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str())?;

                if line.trim().starts_with(".exit") {
                    println!("exiting");
                    break;
                } else if line.trim().starts_with("items") {
                    let items = db.items();

                    for item in items {
                        println!("{:?}", item);
                    }
                } else if line.trim().starts_with("keys") {
                    let keys = db.keys();

                    for key in keys {
                        println!("{:?}", key);
                    }
                } else if line.trim().starts_with("values") {
                    let values = db.values();

                    for value in values {
                        println!("{:?}", value);
                    }
                } else if line.trim().starts_with("prune") {
                    println!("Pruning database");
                    db.prune()?;
                } else if line.trim().starts_with("put") {
                    let parsed_line = line.trim().strip_prefix("put").unwrap().to_string();
                    let split: Vec<_> = parsed_line
                        .split_ascii_whitespace()
                        .map(|x| x.to_string())
                        .collect();
                    let key = split[0].clone();
                    let value = split[1].clone();
                    db.put(key, value)?;
                } else if line.trim().starts_with("get") {
                    let parsed_line = line.trim().strip_prefix("get").unwrap().to_string();
                    let split: Vec<_> = parsed_line
                        .split_ascii_whitespace()
                        .map(|x| x.to_string())
                        .collect();
                    let key = &split[0];
                    let value = db.get(key).unwrap_or("None".to_string());
                    println!("{}={}", key, value);
                } else if line.trim().starts_with("delete") {
                    let parsed_line = line.trim().strip_prefix("delete").unwrap().to_string();
                    let split: Vec<_> = parsed_line
                        .split_ascii_whitespace()
                        .map(|x| x.to_string())
                        .collect();
                    let key = &split[0];
                    db.delete(key)?;
                    println!("deleting {}", key);
                } else {
                    eprintln!("Unknown command: {}", line);
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history("history.txt")?;
    Ok(())
}
