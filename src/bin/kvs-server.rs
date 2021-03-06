#[macro_use]
extern crate clap;

use std::env::current_dir;
use std::{path::Path, fs};

use clap::{App, Arg, AppSettings};
use kvs::engine::SledKvsEngine;
use kvs::server::KvsServer;
use kvs::thread_pool::{NaiveThreadPool, ThreadPool};
use kvs::{KvStore, KvsEngine};
use log::*;

use kvs::err::*;

const ENGINE_FLAG_FILE: &str = ".engine_flag";

fn judge_engine_flag(name: &str) -> Result<bool> {
    if !Path::new(ENGINE_FLAG_FILE).exists() {
        fs::write(ENGINE_FLAG_FILE, name)?;
        Ok(true)
    } else {
        let exist = fs::read(ENGINE_FLAG_FILE)?;
        Ok(exist.eq(name.as_bytes()))
    }
}

fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .init();

    info!("Version: {}", env!("CARGO_PKG_VERSION"));
    
    let matches = App::new("kvs-server")
        .version(crate_version!())
        .author("Xynnn_ <xynnn_@sjtu.edu.cn>")
        .about("A Simple Memory KV-Store Server")
        .setting(AppSettings::AllowExternalSubcommands)
        .arg(Arg::with_name("server address")
                               .short("s")
                               .long("addr")
                               .value_name("server_address")
                               .help("Sets a server to connect")
                               .default_value("127.0.0.1:4000"))
        .arg(Arg::with_name("engine name")
                               .short("e")
                               .long("engine")
                               .value_name("engine_name")
                               .help("Sets an engine type of storage")
                               .default_value("kvs"))
        .get_matches();
    
    let engine_name  = matches.value_of("engine name")
                .unwrap();

    let address   = matches.value_of("server address")
                .unwrap();

    if !judge_engine_flag(engine_name)? {
        Err(KvsError::EngineError)?
    }    

    info!("ENGINE: {}", engine_name);
    info!("Serve {}", address);

    run_with_name(address, engine_name)
}

fn run_with_name(address: &str, engine_name: &str) -> Result<()> {
    match engine_name {
        "kvs" => run(address, KvStore::new()?),
        "sled" => run(address, SledKvsEngine::new(
            sled::open(
                current_dir()?
            )?
        )?),
        _ => run(address, KvStore::new()?),
    }
}

fn run<E: KvsEngine>(address: &str, e: E) -> Result<()> {
    let pool = NaiveThreadPool::new(0)?;

    let server = KvsServer::new(address,e, pool)?;
    server.run()
}