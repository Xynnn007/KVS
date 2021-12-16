#[macro_use]
extern crate clap;

use std::process;
use std::str::FromStr;

use clap::AppSettings;
use clap::{App, Arg, SubCommand};
use kvs::client::*;
use kvs::err::*;

fn main() -> Result<()> {
    stderrlog::new().module(module_path!())
                    // .verbosity(3)
                    .init()
                    .unwrap();

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
            let address = _matches.value_of("server address").unwrap();
            let k = String::from_str(_matches.values_of("key").unwrap().last().unwrap()).unwrap();
            let v = String::from_str(_matches.values_of("value").unwrap().last().unwrap()).unwrap();

            let client_config = KvsClientConfig{ address };
            let mut kv = KvsClient::new(&client_config)?;
            kv.set(k, v)?;
        },
        ("rm", Some(_matches)) => {
            let address = _matches.value_of("server address").unwrap();
            let k = String::from_str(_matches.values_of("key").unwrap().last().unwrap()).unwrap();

            let client_config = KvsClientConfig{ address };
            let mut kv = KvsClient::new(&client_config)?;

            match kv.remove(k) {
                Ok(_) => {},
                Err(e) => match e.kind() {
                    ErrorKind::NoEntryError => {
                        eprintln!("Key not found");
                        process::exit(-1);
                    },
                    _ => Err(e)?,
                }
            };
        },
        ("get", Some(_matches)) => {
            let address = _matches.value_of("server address").unwrap();
            let k = String::from_str(_matches.values_of("key").unwrap().last().unwrap()).unwrap();

            let client_config = KvsClientConfig{ address };
            let mut kv = KvsClient::new(&client_config)?;

            let v = kv.get(k)?;
            match v {
                Some(v) => println!("{}", v),
                None => println!("Key not found")
            }
        },
        _ => Err(ErrorKind::SubCmdError)?,
    }
    Ok(())
}
