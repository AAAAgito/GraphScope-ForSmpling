use graph_store::config::GraphDBConfig;
use graph_store::graph_db::GlobalStoreTrait;
use graph_store::graph_db_impl::LargeGraphDB;
use gremlin_core::graph_proxy::{create_demo_graph, GRAPH};
use gremlin_core::structure::GraphElement;
use pegasus::api::{Count, Filter, Fold, KeyBy, Map, PartitionByKey, Sink, Join, Dedup};
use pegasus::result::ResultStream;
use pegasus::{Configuration, JobConf, flat_map, ServerConf};
use strum_macros::ToString;
use core::time;
use std::path::PathBuf;
use std::slice::SliceIndex;
use std::sync::Arc;
use std::vec;
use std::collections::HashSet;
use structopt::StructOpt;

use std::fmt::Debug;
use pegasus::errors::{BuildJobError, JobSubmitError, SpawnJobError, StartupError};
use graph_store::graph_db::Direction;

use std::time::Instant;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "s", long = "servers")]
    servers: Option<PathBuf>,
    #[structopt(short = "w", long = "workers", default_value = "1")]
    workers: u32,
    #[structopt(short = "b", long = "benchmark", default_value = "t")]
    benchmark_type: String
}

fn main() {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();
    let server_conf = if let Some(ref servers) = config.servers {
        let servers = std::fs::read_to_string(servers).unwrap();
        Configuration::parse(&servers).unwrap()
    } else {
        Configuration::singleton()
    };
    pegasus::startup(server_conf).unwrap();
    let mut conf = JobConf::new("example");
    conf.set_workers(config.workers);

    if config.servers.is_some() {
        conf.reset_servers(ServerConf::All);
    }

    pegasus::wait_servers_ready(conf.servers());

    create_demo_graph();

    let mut result = _teach_example1(conf).expect("Run Job Error!");
    
    while let Some(Ok(data)) = result.next() {
        println!("{:?}", data);
    }
    
    pegasus::shutdown_all();
}

fn _teach_example1(conf: JobConf) -> Result<ResultStream<u64>, JobSubmitError> {
    pegasus::run(conf, move || {
        move |input, output| {
            input.input_from(GRAPH
                .get_all_vertices(None)
                .map(|v| (v.get_id() as u64)))?
            .sink_into(output)
        }
    })
}

