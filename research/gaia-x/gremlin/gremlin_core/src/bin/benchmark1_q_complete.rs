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
        "q_c1" => {q_benchmark_composition1(conf).expect("run job failure")},
        "q_j1" => {q_benchmark_join1(conf).expect("run job failure")},
        "q_c2" => {q_benchmark_composition2(conf).expect("run job failure")},
        "q_j2" => {q_benchmark_join2(conf).expect("run job failure")},
        "q_c3" => {q_benchmark_composition3(conf).expect("run job failure")},
        "q_j3" => {q_benchmark_join3(conf).expect("run job failure")},
        "q_c4" => {q_benchmark_composition4(conf).expect("run job failure")},
        "q_j4" => {q_benchmark_join4(conf).expect("run job failure")},
        _ =>  {q_benchmark_composition1(conf).expect("run job failure")}
    };
    
    while let Some(Ok(data)) = result.next() {
        println!("{:?}", data);
    }

    let time_cost = start.elapsed().as_millis();
    println!("Time Cost: {} ms", time_cost);
    
    pegasus::shutdown_all();
}

/* Use LDBC Data, test first flap map's time */
fn q_benchmark_composition1(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    let num_servers = if conf.servers().len() == 0 {1} else {conf.servers().len()};
    let worker_num = conf.workers as usize;
    pegasus::run(conf, move || {
        move |input, output| {
            let v_label_ids = vec![1];
            input.input_from(GRAPH.get_all_vertices(Some(&v_label_ids)).map(|v| (v.get_id() as u64))
            .filter(move |v_id| {
                let worker_index = pegasus::get_current_worker().index as u64;
                get_partition(*v_id, num_servers, worker_num) == worker_index
            }))?
            .repartition(move |id| Ok(get_partition(*id, num_servers, worker_num)))
            .flat_map(|v_id| {
                let e_label_ids = vec![12];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            // .repartition(move |path| Ok(get_partition(path[1], num_servers, worker_num)))
            // .flat_map(|path| {
            //     let extend_item_id = path[1];
            //     let e_label_ids = vec![0];
            //     let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing);
            //     Ok(adj_vectices.map(move |v| {
            //         let mut new_path = path.clone();
            //         new_path.push(v.get_id() as u64);
            //         new_path
            //     }))
            // })?
            // .repartition(move |path| Ok(get_partition(path[0], num_servers, worker_num)))
            // .flat_map(|path| {
            //     let extend_item_id = path[0];
            //     let e_label_ids = vec![0];
            //     let check_item_id = path[2];
            //     let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
            //                                                             .filter(move |v| (v.get_id() as u64) == check_item_id);
            //     Ok(adj_vectices.map(move |_v| {
            //         path.clone()
            //     }))
            // })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

/* Use LDBC Data, test flap map's time */
fn q_benchmark_join1(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    let num_servers = if conf.servers().len() == 0 {1} else {conf.servers().len()};
    let worker_num = conf.workers as usize;
    pegasus::run(conf, move || {
        move |input, output| {
            let v_label_ids = vec![1];
            let edge_unit1 = input.input_from(GRAPH.get_all_vertices(Some(&v_label_ids)).map(|v| (v.get_id() as u64))
            .filter(move |v_id| {
                let worker_index = pegasus::get_current_worker().index as u64;
                get_partition(*v_id, num_servers, worker_num) == worker_index
            }))?
            .repartition(move |id| Ok(get_partition(*id, num_servers, worker_num)))
            .flat_map(|v_id| {
                let e_label_ids = vec![12];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?;
            // let (edge_unit1, edge_unit2) = edge_unit1.copied()?;
            // let (edge_unit2, edge_unit3) = edge_unit2.copied()?;
            // edge_unit1.key_by(|path| Ok((path[1], path)))?
            // .partition_by_key()
            // .inner_join(edge_unit2.key_by(|path| Ok((path[0], path)))?.partition_by_key())?
            // .map(|(d1, d2)| {
            //     let mut new_path = d1.value;
            //     new_path.extend(&d2.value[1..]);
            //     Ok(new_path)
            // })?
            // .key_by(|path| Ok(((path[0],path[2]), path)))?
            // .partition_by_key()
            // .inner_join(edge_unit3.key_by(|path| Ok(((path[0],path[1]), path)))?.partition_by_key())?
            // .map(|(d1, _d2)| {
            //     Ok(d1.value)
            // })?
            edge_unit1
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}


/* Use LDBC Data, test second flap map's time */
fn q_benchmark_composition2(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    let num_servers = if conf.servers().len() == 0 {1} else {conf.servers().len()};
    let worker_num = conf.workers as usize;
    pegasus::run(conf, move || {
        move |input, output| {
            let v_label_ids = vec![1];
            input.input_from(GRAPH.get_all_vertices(Some(&v_label_ids)).map(|v| (v.get_id() as u64))
            .filter(move |v_id| {
                let worker_index = pegasus::get_current_worker().index as u64;
                get_partition(*v_id, num_servers, worker_num) == worker_index
            }))?
            .repartition(move |id| Ok(get_partition(*id, num_servers, worker_num)))
            .flat_map(|v_id| {
                let e_label_ids = vec![12];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            .repartition(move |path| Ok(get_partition(path[1], num_servers, worker_num)))
            .flat_map(|path| {
                let extend_item_id = path[1];
                let e_label_ids = vec![12];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vectices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?
            // .repartition(move |path| Ok(get_partition(path[0], num_servers, worker_num)))
            // .flat_map(|path| {
            //     let extend_item_id = path[0];
            //     let e_label_ids = vec![0];
            //     let check_item_id = path[2];
            //     let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
            //                                                             .filter(move |v| (v.get_id() as u64) == check_item_id);
            //     // match adj_vectices.next() {
            //     //     Some(v) => Ok(vec![vec![v.get_id() as u64]].into_iter()),
            //     //     _ => Ok(vec![vec![]].into_iter()),
            //     // }
            //     Ok(adj_vectices.map(move |_v| {
            //         path.clone()
            //     }))
            // })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

/* Use LDBC Data, test copy stream's time */
fn q_benchmark_join2(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    let num_servers = if conf.servers().len() == 0 {1} else {conf.servers().len()};
    let worker_num = conf.workers as usize;
    pegasus::run(conf, move || {
        move |input, output| {
            let v_label_ids = vec![1];
            let edge_unit1 = input.input_from(GRAPH.get_all_vertices(Some(&v_label_ids)).map(|v| (v.get_id() as u64))
            .filter(move |v_id| {
                let worker_index = pegasus::get_current_worker().index as u64;
                get_partition(*v_id, num_servers, worker_num) == worker_index
            }))?
            .repartition(move |id| Ok(get_partition(*id, num_servers, worker_num)))
            .flat_map(|v_id| {
                let e_label_ids = vec![12];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?;
            let (edge_unit1, edge_unit2) = edge_unit1.copied()?;
            let (edge_unit2, edge_unit3) = edge_unit2.copied()?;
            // edge_unit1.key_by(|path| Ok((path[1], path)))?
            // .partition_by_key()
            // .inner_join(edge_unit2.key_by(|path| Ok((path[0], path)))?.partition_by_key())?
            // .map(|(d1, d2)| {
            //     let mut new_path = d1.value;
            //     new_path.extend(&d2.value[1..]);
            //     Ok(new_path)
            // })?
            // .key_by(|path| Ok(((path[0],path[2]), path)))?
            // .partition_by_key()
            // .inner_join(edge_unit3.key_by(|path| Ok(((path[0],path[1]), path)))?.partition_by_key())?
            // .map(|(d1, _d2)| {
            //     Ok(d1.value)
            // })?
            edge_unit3
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

/* Use LDBC Data, test third flap map's time */
fn q_benchmark_composition3(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    let num_servers = if conf.servers().len() == 0 {1} else {conf.servers().len()};
    let worker_num = conf.workers as usize;
    pegasus::run(conf, move || {
        move |input, output| {
            let v_label_ids = vec![1];
            input.input_from(GRAPH.get_all_vertices(Some(&v_label_ids)).map(|v| (v.get_id() as u64))
            .filter(move |v_id| {
                let worker_index = pegasus::get_current_worker().index as u64;
                get_partition(*v_id, num_servers, worker_num) == worker_index
            }))?
            .repartition(move |id| Ok(get_partition(*id, num_servers, worker_num)))
            .flat_map(|v_id| {
                let e_label_ids = vec![12];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            .repartition(move |path| Ok(get_partition(path[1], num_servers, worker_num)))
            .flat_map(|path| {
                let extend_item_id = path[1];
                let e_label_ids = vec![12];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vectices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?
            .repartition(move |path| Ok(get_partition(path[0], num_servers, worker_num)))
            .flat_map(|path| {
                let extend_item_id = path[0];
                let e_label_ids = vec![12];
                let check_item_id = path[2];
                let mut adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
                                                                        .filter(move |v| (v.get_id() as u64) == check_item_id);
                match adj_vectices.next() {
                    Some(v) => Ok(vec![vec![v.get_id() as u64]].into_iter()),
                    _ => Ok(vec![vec![]].into_iter()),
                }
                // Ok(adj_vectices.map(move |_v| {
                //     path.clone()
                // }))
            })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

/* Use LDBC Data, test first join's time */
fn q_benchmark_join3(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    let num_servers = if conf.servers().len() == 0 {1} else {conf.servers().len()};
    let worker_num = conf.workers as usize;
    pegasus::run(conf, move || {
        move |input, output| {
            let v_label_ids = vec![1];
            let edge_unit1 = input.input_from(GRAPH.get_all_vertices(Some(&v_label_ids)).map(|v| (v.get_id() as u64))
            .filter(move |v_id| {
                let worker_index = pegasus::get_current_worker().index as u64;
                get_partition(*v_id, num_servers, worker_num) == worker_index
            }))?
            .repartition(move |id| Ok(get_partition(*id, num_servers, worker_num)))
            .flat_map(|v_id| {
                let e_label_ids = vec![12];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?;
            let (edge_unit1, edge_unit2) = edge_unit1.copied()?;
            let (edge_unit2, edge_unit3) = edge_unit2.copied()?;
            edge_unit1.key_by(|path| Ok((path[1], path)))?
            .partition_by_key()
            .inner_join(edge_unit2.key_by(|path| Ok((path[0], path)))?.partition_by_key())?
            .map(|(d1, d2)| {
                let mut new_path = d1.value;
                new_path.extend(&d2.value[1..]);
                Ok(new_path)
            })?
            // .key_by(|path| Ok(((path[0],path[2]), path)))?
            // .partition_by_key()
            // .inner_join(edge_unit3.key_by(|path| Ok(((path[0],path[1]), path)))?.partition_by_key())?
            // .map(|(d1, _d2)| {
            //     Ok(d1.value)
            // })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

/* Use LDBC Data, test third flap map's time */
fn q_benchmark_composition4(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    let num_servers = if conf.servers().len() == 0 {1} else {conf.servers().len()};
    let worker_num = conf.workers as usize;
    pegasus::run(conf, move || {
        move |input, output| {
            let v_label_ids = vec![1];
            input.input_from(GRAPH.get_all_vertices(Some(&v_label_ids)).map(|v| (v.get_id() as u64))
            .filter(move |v_id| {
                let worker_index = pegasus::get_current_worker().index as u64;
                get_partition(*v_id, num_servers, worker_num) == worker_index
            }))?
            .repartition(move |id| Ok(get_partition(*id, num_servers, worker_num)))
            .flat_map(|v_id| {
                let e_label_ids = vec![12];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            .repartition(move |path| Ok(get_partition(path[1], num_servers, worker_num)))
            .flat_map(|path| {
                let extend_item_id = path[1];
                let e_label_ids = vec![12];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vectices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?
            .repartition(move |path| Ok(get_partition(path[0], num_servers, worker_num)))
            .filter(|path| {
                let extend_item_id = path[0];
                let e_label_ids = vec![12];
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
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

/* Use LDBC Data, test second join's time */
fn q_benchmark_join4(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    let num_servers = if conf.servers().len() == 0 {1} else {conf.servers().len()};
    let worker_num = conf.workers as usize;
    pegasus::run(conf, move || {
        move |input, output| {
            let v_label_ids = vec![1];
            let edge_unit1 = input.input_from(GRAPH.get_all_vertices(Some(&v_label_ids)).map(|v| (v.get_id() as u64))
            .filter(move |v_id| {
                let worker_index = pegasus::get_current_worker().index as u64;
                get_partition(*v_id, num_servers, worker_num) == worker_index
            }))?
            .repartition(move |id| Ok(get_partition(*id, num_servers, worker_num)))
            .flat_map(|v_id| {
                let e_label_ids = vec![12];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?;
            let (edge_unit1, edge_unit2) = edge_unit1.copied()?;
            let (edge_unit2, edge_unit3) = edge_unit2.copied()?;
            edge_unit1.key_by(|path| Ok((path[1], path)))?
            .partition_by_key()
            .inner_join(edge_unit2.key_by(|path| Ok((path[0], path)))?.partition_by_key())?
            .map(|(d1, d2)| {
                let mut new_path = d1.value;
                new_path.extend(&d2.value[1..]);
                Ok(new_path)
            })?
            .key_by(|path| Ok(((path[0],path[2]), path)))?
            .partition_by_key()
            .inner_join(edge_unit3.key_by(|path| Ok(((path[0],path[1]), path)))?.partition_by_key())?
            .map(|(d1, _d2)| {
                Ok(d1.value)
            })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}