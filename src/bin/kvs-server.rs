#[macro_use]
extern crate clap;

use std::{path::Path, fs};

use clap::{App, Arg, AppSettings};
use failure::ResultExt;
// use kvs::server::{KvsServer, KvsServerConfig};
use log::*;

use kvs::err::*;

const ENGINE_FLAG_FILE: &str = ".engine_flag";

fn judge_engine_flag(name: &str) -> Result<bool> {
    if !Path::new(ENGINE_FLAG_FILE).exists() {
        fs::write(ENGINE_FLAG_FILE, name).context(ErrorKind::IOError)?;
        Ok(true)
    } else {
        let exist = fs::read(ENGINE_FLAG_FILE).context(ErrorKind::IOError)?;
        Ok(exist.eq(name.as_bytes()))
    }
}

fn main() -> Result<()> {
    stderrlog::new().module(module_path!())
                    .verbosity(2)
                    .init()
                    .unwrap();

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
    
    let engine  = matches.value_of("engine name")
                .unwrap();

    let address   = matches.value_of("server address")
                .unwrap();

    if !judge_engine_flag(engine)? {
        Err(ErrorKind::EngineError)?
    }    

    info!("ENGINE: {}", engine);
    info!("Serve {}", address);
    // let mut server = KvsServer::new(&KvsServerConfig {address, engine})?;
    // server.run()?;
    
    Ok(())
}