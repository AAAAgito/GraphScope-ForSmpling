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

#[global_allocator]
static G : snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "s", long = "servers")]
    servers: Option<PathBuf>,
    #[structopt(short = "w", long = "workers", default_value = "1")]
    workers: u32,
    #[structopt(short = "b", long = "benchmark", default_value = "t")]
    benchmark_type: String
}

fn get_partition(id: u64, num_servers: usize, worker_num: usize) -> u64 {
    let magic_num = id / (num_servers as u64);
    let num_servers = num_servers as u64;
    let worker_num = worker_num as u64;
    (id - magic_num * num_servers)* worker_num + magic_num % worker_num
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

    let start = Instant::now();

    let mut result = match config.benchmark_type.as_str() {
        "g" =>  {benchmark1_getadj(conf).expect("run job failure")},
        "n" =>  {benchmark1_no_getadj(conf).expect("run job failure")},
        "i" =>  {benchmark1_intersection(conf).expect("run job failure")},
        _ =>  {benchmark1_getadj(conf).expect("run job failure")}
    };
    
    while let Some(Ok(data)) = result.next() {
        println!("{:?}", data);
    }

    let time_cost = start.elapsed().as_millis();
    println!("Time Cost: {} ms", time_cost);
    
    pegasus::shutdown_all();
}

fn benchmark1_getadj(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    let num_servers = if conf.servers().len() == 0 {1} else {conf.servers().len()};
    let worker_num = conf.workers as usize;
    pegasus::run(conf, move || {
        move |input, output| {
            let v_label_ids = vec![0];
            input.input_from(GRAPH.get_all_vertices(Some(&v_label_ids)).map(|v| (v.get_id() as u64))
            .filter(move |v_id| {
                let worker_index = pegasus::get_current_worker().index as u64;
                get_partition(*v_id, num_servers, worker_num) == worker_index
            }))?
            .repartition(move |v_id| Ok(get_partition(*v_id, num_servers, worker_num)))
            .flat_map(|v_id| {
                let e_label_ids = vec![0];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    // path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

fn benchmark1_no_getadj(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    let num_servers = if conf.servers().len() == 0 {1} else {conf.servers().len()};
    let worker_num = conf.workers as usize;
    pegasus::run(conf, move || {
        move |input, output| {
            let v_label_ids = vec![0];
            input.input_from(GRAPH.get_all_vertices(Some(&v_label_ids)).map(|v| (v.get_id() as u64))
            .filter(move |v_id| {
                let worker_index = pegasus::get_current_worker().index as u64;
                get_partition(*v_id, num_servers, worker_num) == worker_index
            }))?
            .repartition(move |v_id| Ok(get_partition(*v_id, num_servers, worker_num)))
            .flat_map(|v_id| {
                let adj_vertices = 0..200;
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    // path.push(v_id);
                    path.push(v as u64);
                    path
                }))
            })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

fn benchmark1_intersection(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    let num_servers = if conf.servers().len() == 0 {1} else {conf.servers().len()};
    let worker_num = conf.workers as usize;
    pegasus::run(conf, move || {
        move |input, output| {
            let v_label_ids = vec![0];
            input.input_from(GRAPH.get_all_vertices(Some(&v_label_ids)).map(|v| (v.get_id() as u64))
            .filter(move |v_id| {
                let worker_index = pegasus::get_current_worker().index as u64;
                get_partition(*v_id, num_servers, worker_num) == worker_index
            }))?
            .repartition(move |v_id| Ok(get_partition(*v_id, num_servers, worker_num)))
            .flat_map(|v_id| {
                let adj_vertices1 = 0..400;
                let vertices_set1: HashSet<i32> = adj_vertices1.collect();
                let adj_vertices0 = (0..200).filter(move |v| vertices_set1.contains(v));
                Ok(adj_vertices0.map(move |v| {
                    let mut path = vec![];
                    // path.push(v_id);
                    path.push(v as u64);
                    path
                }))
            })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

