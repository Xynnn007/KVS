#[macro_use]
extern crate clap;

use std::process;
use std::str::FromStr;

use clap::AppSettings;
use clap::{App, Arg, SubCommand};
use kvs::client::*;
use kvs::err::*;
use log::LevelFilter;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .init();

    let matches = App::new("kvs-client")
        .settings(&[AppSettings::UnifiedHelpMessage,
            AppSettings::DeriveDisplayOrder,
            AppSettings::VersionlessSubcommands,
            AppSettings::AllowExternalSubcommands])
        .version(crate_version!())
        .author("Xynnn_ <xynnn_@sjtu.edu.cn>")
        .about("A Simple Memory KV-Store Client")
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
            .arg(Arg::with_name("server address")
                               .short("s")
                               .long("addr")
                               .value_name("server_address")
                               .help("Sets a server to connect")
                               .default_value("127.0.0.1:4000"))
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
            .arg(Arg::with_name("server address")
                               .short("s")
                               .long("addr")
                               .value_name("server_address")
                               .help("Sets a server to connect")
                               .default_value("127.0.0.1:4000"))
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
            .arg(Arg::with_name("server address")
                               .short("s")
                               .long("addr")
                               .value_name("server_address")
                               .help("Sets a server to connect")
                               .default_value("127.0.0.1:4000"))
        )
        .get_matches();

    match matches.subcommand() {
        ("set", Some(_matches)) => {
            let address = _matches.value_of("server address")
                .unwrap()
                .parse()?;
            let k = String::from_str(_matches.values_of("key").unwrap().last().unwrap()).unwrap();
            let v = String::from_str(_matches.values_of("value").unwrap().last().unwrap()).unwrap();

            let mut kv = KvsClient::new(address).await?;
            kv.set(k, v).await?;
        },
        ("rm", Some(_matches)) => {
            let address = _matches.value_of("server address")
                .unwrap()
                .parse()?;
            let k = String::from_str(_matches.values_of("key").unwrap().last().unwrap()).unwrap();

            let mut kv = KvsClient::new(address).await?;

            match kv.remove(k).await {
                Ok(()) => {},
                Err(e) => match e {
                    KvsError::NoEntryError => {
                        eprintln!("Key not found");
                        process::exit(-1);
                    },
                    _ => Err(e)?,
                }
            };
        },
        ("get", Some(_matches)) => {
            let address = _matches.value_of("server address")
                .unwrap()
                .parse()?;
            let k = String::from_str(_matches.values_of("key").unwrap().last().unwrap()).unwrap();
            let mut kv = KvsClient::new(address).await?;

            let v = kv.get(k).await?;
            match v {
                Some(v) => println!("{}", v),
                None => println!("Key not found")
            }
        },
        _ => Err(KvsError::SubCmdError)?,
    }
    Ok(())
}
