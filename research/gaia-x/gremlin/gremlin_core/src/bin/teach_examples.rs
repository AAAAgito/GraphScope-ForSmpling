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

    let mut result = _teach_example7(conf).expect("Run Job Error!");
    
    while let Some(Ok(data)) = result.next() {
        println!("{:?}", data);
    }
    
    pegasus::shutdown_all();
}

fn _teach_example1(conf: JobConf) -> Result<ResultStream<u64>, JobSubmitError> {
    pegasus::run(conf, move || {
        move |input, output| {
            input.input_from(GRAPH
                .get_all_vertices(Some(&vec![0, 1]))
                .map(|v| (v.get_id() as u64))
                .filter(|v_id| *v_id == 1))?
            .sink_into(output)
        }
    })
}

fn _teach_example2(conf: JobConf) -> Result<ResultStream<u64>, JobSubmitError> {
    let worker_num = conf.workers as u64;
    pegasus::run(conf, move || {
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(None)
            .map(|v| (v.get_id() as u64))
            .filter(move |v_id| {
                let worker_index = pegasus::get_current_worker().index as u64;
                v_id % worker_num == worker_index
            })
        )?
            .flat_map(|v| {
                let adj_vertices = GRAPH
                    .get_adj_vertices(v as usize, None, Direction::Outgoing)
                    .map(|vertex| vertex.get_id() as u64);
                Ok(adj_vertices)
            })?
            .sink_into(output)
        }
    })
}

fn _teach_example3(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(None).map(|v| (v.get_id() as u64)))?
            .flat_map(|v_id| {
                let label_ids = vec![1];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            .flat_map(|path| {
                let extend_item_id = path[1];
                let e_label_ids = vec![1];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Incoming);
                Ok(adj_vectices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?
            .sink_into(output)
        }
    })
}

fn _teach_example3s(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(None).map(|v| (v.get_id() as u64)))?
            .flat_map(|v_id| {
                let label_ids = vec![0];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&label_ids), Direction::Incoming);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            .flat_map(|path| {
                let extend_item_id = path[1];
                let e_label_ids = vec![1];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vectices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?
            .sink_into(output)
        }
    })
}

fn _teach_example4(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        move |input, output| {
            let left_partition = input.input_from(GRAPH.get_all_vertices(None).map(|v| (v.get_id() as u64)))?
            .flat_map(|v_id| {
                let label_ids = vec![1];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?;
            let (left_partition, right_partition) = left_partition.copied()?;
            left_partition.key_by(|path| Ok((path[1], path)))?
            .partition_by_key()
            .inner_join(right_partition.key_by(|path| Ok((path[1], path)))?.partition_by_key())?
            .map(|(d1, d2)| {
                let mut new_path = d1.value;
                new_path.extend(&d2.value[0..1]);
                Ok(new_path)
            })?
            .sink_into(output)
        }
    })
}

fn _teach_example5(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(None).map(|v| (v.get_id() as u64)))?
            .flat_map(|v_id| {
                let label_ids = vec![1];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            .flat_map(|path| {
                let extend_item_id = path[1];
                let e_label_ids = vec![1];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Incoming);
                Ok(adj_vectices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?
            .filter(|path| Ok(path[0] != path[2]))?
            .sink_into(output)
        }
    })
}

fn _teach_example6(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(None).map(|v| (v.get_id() as u64)))?
            .flat_map(|v_id| {
                let label_ids = vec![1];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            .flat_map(|path| {
                let extend_item_id = path[1];
                let e_label_ids = vec![1];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Incoming);
                Ok(adj_vectices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?
            .filter(|path| {
                let extend_item_id = path[0];
                let e_label_ids = vec![0];
                let check_item_id = path[2];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing);
                let mut connected = false;
                for v in adj_vectices {
                    if v.get_id() as u64 == check_item_id {
                        connected = true;
                        break;
                    }
                }
                Ok(connected)
            })?
            .sink_into(output)
        }
    })
}

fn _teach_example7(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    let num_servers = if conf.servers().len() == 0 {1} else {conf.servers().len()};
    let worker_num = conf.workers as usize;
    pegasus::run(conf, move || {
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(None).map(|v| (v.get_id() as u64))
            .filter(move |v_id| {
                let worker_index = pegasus::get_current_worker().index as u64;
                get_partition(*v_id, num_servers, worker_num) == worker_index
            }))?
            .repartition(move |id| Ok(get_partition(*id, num_servers, worker_num)))            
            .flat_map(|v_id| {
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, None, Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            .repartition(move |path| Ok(get_partition(path[0], num_servers, worker_num)))
            .sink_into(output)
        }
    })
}