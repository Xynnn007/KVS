#[macro_use]
extern crate clap;

use clap::{App, Arg, AppSettings};
use kvs::server::{KvsServer, KvsServerConfig};
use log::*;

use kvs::err::*;

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

    info!("ENGINE: {}", engine);
    info!("Serve {}", address);
    let mut server = KvsServer::new(&KvsServerConfig {address, engine})?;
    server.run()?;
    
    Ok(())
}
