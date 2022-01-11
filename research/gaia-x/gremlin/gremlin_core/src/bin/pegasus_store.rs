use graph_store::config::GraphDBConfig;
use graph_store::graph_db::GlobalStoreTrait;
use graph_store::graph_db_impl::LargeGraphDB;
use gremlin_core::graph_proxy::{create_demo_graph, GRAPH};
use pegasus::api::{Count, Filter, Sink};
use pegasus::{Configuration, JobConf};
use std::path::PathBuf;
use std::sync::Arc;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "s", long = "servers")]
    servers: Option<PathBuf>,
    #[structopt(short = "w", long = "workers", default_value = "1")]
    workers: u32,
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
    pegasus::startup(server_conf).ok();
    let mut conf = JobConf::new("example");
    conf.set_workers(config.workers);

    create_demo_graph();

    let mut _result = pegasus::run(conf, move || {
        let id = pegasus::get_current_worker().index;
        move |input, output| {
            input
                // 如果这里图没有分区，也就是每台机器都会读入一个完整的图的话，那么还要注意分区
                .input_from(GRAPH.get_all_vertices(None).map(|v| (v.get_label()[0])))?
                .filter(|lab| Ok(*lab == 1))?
                .count()?
                .sink_into(output)
        }
    })
    .expect("run job failure;");
    let count = _result.next().unwrap().unwrap();
    println!("{}", count);

    pegasus::shutdown_all();
}
