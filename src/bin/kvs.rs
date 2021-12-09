#[macro_use]
extern crate clap;

use std::process;
use std::str::FromStr;

use clap::{App, Arg, SubCommand};
use kvs::err::*;
use kvs::*;

fn main() -> Result<()> {
    let matches = App::new("kvs")
        .version(crate_version!())
        .author("Xynnn_ <xynnn_@sjtu.edu.cn>")
        .about("A Simple Memory KV-Store")
        .subcommand(
            SubCommand::with_name("set")
            .about("Set a key-value")
            .arg(
                Arg::with_name("key")
                .help("key")
                .index(1)
                .required(true)
            )
            .arg(
                Arg::with_name("value")
                .help("value")
                .index(2)
                .required(true)
            )
        )
        .subcommand(
            SubCommand::with_name("rm")
            .about("Remove a key-value")
            .arg(
                Arg::with_name("key")
                .help("key")
                .index(1)
                .required(true)
            )
        )
        .subcommand(
            SubCommand::with_name("get")
            .about("Get a key-value")
            .arg(
                Arg::with_name("key")
                .help("key")
                .index(1)
                .required(true)
            )
        )
        .get_matches();
    
    match matches.subcommand() {
        ("set", Some(_matches)) => {
            let mut kv = KvStore::new()?;

            let k = String::from_str(_matches.values_of("key").unwrap().last().unwrap()).unwrap();
            let v = String::from_str(_matches.values_of("value").unwrap().last().unwrap()).unwrap();

            kv.set(k, v)?;
        },
        ("rm", Some(_matches)) => {
            let mut kv = KvStore::new()?;
            
            let k = String::from_str(_matches.values_of("key").unwrap().last().unwrap()).unwrap();

            match kv.remove(k) {
                Ok(_) => {},
                Err(e) => match e.kind() {
                    ErrorKind::NoEntryError => {
                        println!("key not found");
                        process::exit(-1);
                    },
                    _ => Err(e)?,
                }
            };
        },
        ("get", Some(_matches)) => {
            let mut kv = KvStore::new()?;
            
            let k = String::from_str(_matches.values_of("key").unwrap().last().unwrap()).unwrap();

            let v = kv.get(k)?;
            match v {
                Some(v) => println!("{}", v),
                None => println!("key not found")
            }
        },
        _ => Err(ErrorKind::SubCmdError)?,
    }
    Ok(())
}
