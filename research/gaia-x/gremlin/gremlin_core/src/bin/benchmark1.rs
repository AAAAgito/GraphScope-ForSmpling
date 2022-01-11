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
        "s1_c" => {s_benchmark1_composition(conf).expect("run job failure")},
        "s1_j" => {s_benchmark1_join(conf).expect("run job failure")},
        "s2_c" => {s_benchmark2_3_composition(conf).expect("run job failure")},
        "s2_j" => {s_benchmark2_3_join(conf).expect("run job failure")},
        "s3_c" => {s_benchmark2_3_composition(conf).expect("run job failure")},
        "s3_j" => {s_benchmark2_3_join(conf).expect("run job failure")},
        "g1_c" => {g_benchmark1_composition(conf).expect("run job failure")},
        "g1_ci" => {g_benchmark1_composition_intersection(conf).expect("run job failure")},
        "g1_j" => {g_benchmark1_join(conf).expect("run job failure")},
        "g2_c" => {g_benchmark2_composition(conf).expect("run job failure")},
        "g2_j" => {g_benchmark2_join(conf).expect("run job failure")},
        "g3_c" => {g_benchmark3_composition(conf).expect("run job failure")},
        "g3_j" => {g_benchmark3_join(conf).expect("run job failure")},
        "g4_c" => {g_benchmark4_composition(conf).expect("run job failure")},
        "g4_j" => {g_benchmark4_join(conf).expect("run job failure")},
        _ =>  {g_benchmark1_composition(conf).expect("run job failure")}
    };
    
    while let Some(Ok(data)) = result.next() {
        println!("{:?}", data);
    }

    let time_cost = start.elapsed().as_millis();
    println!("Time Cost: {} ms", time_cost);
    
    pegasus::shutdown_all();
}

fn _path_extend_00(conf: JobConf) -> Result<ResultStream<u64>, JobSubmitError> {
    pegasus::run(conf, move || {
        let id = pegasus::get_current_worker().index;
        println!("{}", id);
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(Some(&vec![7])).map(|v| (v.get_id() as u64)).filter(|v_id| {
                let worker_id = pegasus::get_current_worker();
                let worker_num = worker_id.local_peers as u64;
                let worker_index = worker_id.index as u64;
                *v_id % worker_num == worker_index
            }))?
            .count()?
            .sink_into(output)
        }
    })
}

fn s_benchmark1_composition(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        move |input, output| {
            let mut source = input.input_from(vec![vec![0 as u64]])?;
            let size = 1000;
            let mut n = 0;
            while n < size {
                if n == 0 {
                    source = source.flat_map(|path| {
                        let extend_item_id = path[(path.len()-1) as usize];
                        let e_label_ids = vec![0];
                        let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
                        .filter(|v| v.get_id() == 1);
                        Ok(adj_vectices.map(move |v| {
                            let mut new_path = path.clone();
                            new_path.push(v.get_id() as u64);
                            new_path
                        }))
                    })?
                    ;
                }
                else {
                    source = source.flat_map(|path| {
                        let extend_item_id = path[(path.len()-1) as usize];
                        let e_label_ids = vec![0];
                        let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing);
                        Ok(adj_vectices.map(move |v| {
                            let mut new_path = path.clone();
                            new_path.push(v.get_id() as u64);
                            new_path
                        }))
                    })?;
                }
                n += 1;
            }
            n = 0;
            while n < size {
                if n == 0 {
                    source = source.flat_map(|path| {
                        let extend_item_id = path[0];
                        let e_label_ids = vec![0];
                        let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
                        .filter(|v| v.get_id() == 10001);
                        Ok(adj_vectices.map(move |v| {
                            let mut new_path = path.clone();
                            new_path.push(v.get_id() as u64);
                            new_path
                        }))
                    })?;
                }
                else {
                    source = source.flat_map(|path| {
                        let extend_item_id = path[(path.len()-1) as usize];
                        let e_label_ids = vec![0];
                        let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing);
                        Ok(adj_vectices.map(move |v| {
                            let mut new_path = path.clone();
                            new_path.push(v.get_id() as u64);
                            new_path
                        }))
                    })?;
                }
                n += 1;
            }
            source
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

fn s_benchmark1_join(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        move |input, output| {
            let edge_unit1 = input.input_from(vec![vec![0 as u64]])?;
            let (mut edge_unit1, mut edge_unit2) = edge_unit1.copied()?;
            let size = 1000;
            let mut n = 0;
            while n< size {
                if n == 0 {
                    edge_unit1 = edge_unit1.flat_map(|path| {
                        let extend_item_id = path[(path.len()-1) as usize];
                        let e_label_ids = vec![0];
                        let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
                        .filter(|v| v.get_id() == 1);
                        Ok(adj_vectices.map(move |v| {
                            let mut new_path = path.clone();
                            new_path.push(v.get_id() as u64);
                            new_path
                        }))
                    })?;
                    edge_unit2 = edge_unit2.flat_map(|path| {
                        let extend_item_id = path[0];
                        let e_label_ids = vec![0];
                        let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
                        .filter(|v| v.get_id() == 10001);
                        Ok(adj_vectices.map(move |v| {
                            let mut new_path = path.clone();
                            new_path.push(v.get_id() as u64);
                            new_path
                        }))
                    })?;
                }
                else {
                    edge_unit1 = edge_unit1.flat_map(|path| {
                        let extend_item_id = path[(path.len()-1) as usize];
                        let e_label_ids = vec![0];
                        let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing);
                        Ok(adj_vectices.map(move |v| {
                            let mut new_path = path.clone();
                            new_path.push(v.get_id() as u64);
                            new_path
                        }))
                    })?;
                    edge_unit2 = edge_unit2.flat_map(|path| {
                        let extend_item_id = path[(path.len()-1) as usize];
                        let e_label_ids = vec![0];
                        let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing);
                        Ok(adj_vectices.map(move |v| {
                            let mut new_path = path.clone();
                            new_path.push(v.get_id() as u64);
                            new_path
                        }))
                    })?;
                }
                n += 1;
            }
            edge_unit1.key_by(|path| Ok((path[0], path)))?
            .partition_by_key()
            .inner_join(edge_unit2.key_by(|path| Ok((path[0], path)))?.partition_by_key())?
            .map(|(d1, d2)| {
                let mut new_path = d1.value;
                new_path.extend(&d2.value[1..]);
                Ok(new_path)
            })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

fn s_benchmark2_3_composition(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
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
            .repartition(move |id| Ok(get_partition(*id, num_servers, worker_num)))            
            .flat_map(|v_id| {
                let e_label_ids = vec![0];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            .repartition(move |path| Ok(get_partition(path[0], num_servers, worker_num)))
            .flat_map(|path| {
                let extend_item_id = path[0];
                let e_label_ids = vec![1];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vectices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

fn s_benchmark2_3_join(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    let num_servers = if conf.servers().len() == 0 {1} else {conf.servers().len()};
    let worker_num = conf.workers as usize;
    pegasus::run(conf, move|| {
        move |input, output| {
            let v_label_ids = vec![0];
            let left_partition = input.input_from(GRAPH.get_all_vertices(Some(&v_label_ids)).map(|v| (v.get_id() as u64))
            .filter(move |v_id| {
                let worker_index = pegasus::get_current_worker().index as u64;
                get_partition(*v_id, num_servers, worker_num) == worker_index
            }))?
            .repartition(move |id| Ok(get_partition(*id, num_servers, worker_num)));
            let (left_partition, right_partition) = left_partition.copied()?;
            let left_partition = left_partition.flat_map(|v_id| {
                let e_label_ids = vec![0];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?;
            let right_partition = right_partition.flat_map(|v_id| {
                let e_label_ids = vec![1];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?;
            left_partition.key_by(|path| Ok((path[0], path)))?
            .partition_by_key()
            .inner_join(right_partition.key_by(|path| Ok((path[0], path)))?.partition_by_key())?
            .map(|(d1, d2)| {
                let mut new_path = d1.value;
                new_path.extend(&d2.value[1..]);
                Ok(new_path)
            })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

fn g_benchmark1_composition(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
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
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
                                                                        .filter(move |v| (v.get_id() as u64) == check_item_id);
                Ok(adj_vectices.map(move |_v| {
                    path.clone()
                }))
            })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

fn g_benchmark1_composition_intersection(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
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
                let extend_item_id0 = path[0];
                let extend_item_id1 = path[1];
                let e_label_ids = vec![12];
                let adj_vectices1 = GRAPH.get_adj_vertices(extend_item_id1 as usize, Some(&e_label_ids), Direction::Outgoing).map(|v| v.get_id() as u64);
                let vertices_set1: HashSet<u64> = adj_vectices1.collect();
                let adj_vectices0 = GRAPH.get_adj_vertices(extend_item_id0 as usize, Some(&e_label_ids), Direction::Outgoing).filter(move |v| vertices_set1.contains(&(v.get_id() as u64)));
                Ok(adj_vectices0.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

fn g_benchmark1_join(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
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

fn g_benchmark2_composition(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
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
            .repartition(move |v_id| Ok(get_partition(*v_id, num_servers, worker_num)))
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
            .repartition(move |path| Ok(get_partition(path[0], num_servers, worker_num)))
            .flat_map(|path| {
                let extend_item_id = path[0];
                let e_label_ids = vec![15];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vectices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?
            .repartition(move |path| Ok(get_partition(path[1], num_servers, worker_num)))
            .flat_map(|path| {
                let extend_item_id = path[1];
                let e_label_ids = vec![15];
                let check_item_id = path[2];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
                                                                        .filter(move |v| (v.get_id() as u64) == check_item_id);
                Ok(adj_vectices.map(move |_v| {
                    path.clone()
                }))
            })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

fn g_benchmark2_join(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
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
            .repartition(move |id| Ok(get_partition(*id, num_servers, worker_num)));
            let (edge_unit1, edge_unit2) = edge_unit1.copied()?;
            let edge_unit1 = edge_unit1.flat_map(|v_id| {
                let e_label_ids = vec![12];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?;
            let edge_unit2 = edge_unit2.flat_map(|v_id| {
                let e_label_ids = vec![15];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?;
            let (edge_unit2, edge_unit3) = edge_unit2.copied()?;
            edge_unit1.key_by(|path| Ok((path[1], path)))?
            .partition_by_key()
            .inner_join(edge_unit2.key_by(|path| Ok((path[0], path)))?.partition_by_key())?
            .map(|(d1, d2)| {
                let mut new_path = d1.value;
                new_path.extend(&d2.value[1..]);
                Ok(new_path)
            })?
            .key_by(|path| Ok(((path[0], path[2]), path)))?
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

fn g_benchmark3_composition(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
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
            .repartition(move |v_id| Ok(get_partition(*v_id, num_servers, worker_num)))
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
            .repartition(move |path| Ok(get_partition(path[0], num_servers, worker_num)))
            .flat_map(|path| {
                let extend_item_id = path[0];
                let e_label_ids = vec![12];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vectices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?
            .repartition(move |path| Ok(get_partition(path[1], num_servers, worker_num)))
            .flat_map(|path| {
                let extend_item_id = path[1];
                let e_label_ids = vec![12];
                let check_item_id = path[2];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
                                                                        .filter(move |v| (v.get_id() as u64) == check_item_id);
                Ok(adj_vectices.map(move |_v| {
                    path.clone()
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
                let check_item_id = path[3];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
                                                                        .filter(move |v| (v.get_id() as u64) == check_item_id);
                Ok(adj_vectices.map(move |_v| {
                    path.clone()
                }))
            })?
            .repartition(move |path| Ok(get_partition(path[2], num_servers, worker_num)))
            .flat_map(|path| {
                let extend_item_id = path[2];
                let e_label_ids = vec![12];
                let check_item_id = path[3];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
                                                                        .filter(move |v| (v.get_id() as u64) == check_item_id);
                Ok(adj_vectices.map(move |_v| {
                    path.clone()
                }))
            })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

fn g_benchmark3_join(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
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
            .repartition(move |v_id| Ok(get_partition(*v_id, num_servers, worker_num)));
            let (edge_unit1, edge_unit2) = edge_unit1.copied()?;
            let edge_unit1 = edge_unit1.flat_map(|v_id| {
                let e_label_ids = vec![12];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            .repartition(move |path| Ok(get_partition(path[0], num_servers, worker_num)))
            .flat_map(|path| {
                let extend_item_id = path[0];
                let e_label_ids = vec![12];
                let checked_item_id = path[1];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
                .filter(move |v| (v.get_id() as u64) != checked_item_id);
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
                let checked_item_id1 = path[1];
                let checked_item_id2 = path[2];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
                .filter(move |v| (v.get_id() as u64) != checked_item_id1 && (v.get_id() as u64) != checked_item_id2);
                Ok(adj_vectices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?;
            let edge_unit2 = edge_unit2.flat_map(|v_id| {
                let e_label_ids = vec![12];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            .repartition(move |path| Ok(get_partition(path[0], num_servers, worker_num)))
            .flat_map(|path| {
                let extend_item_id = path[0];
                let e_label_ids = vec![12];
                let checked_item_id = path[1];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
                .filter(move |v| (v.get_id() as u64) != checked_item_id);
                Ok(adj_vectices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?
            .repartition(move |path| Ok(get_partition(path[1], num_servers, worker_num)))
            .flat_map(|path| {
                let extend_item_id = path[1];
                let e_label_ids = vec![12];
                let check_item_id = path[2];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
                                                                        .filter(move |v| (v.get_id() as u64) == check_item_id);
                Ok(adj_vectices.map(move |_v| {
                    path.clone()
                }))
            })?;
            edge_unit1.key_by(|path| Ok(((path[1], path[2], path[3]), path)))?
            .partition_by_key()
            .inner_join(edge_unit2.key_by(|path| Ok(((path[0],path[1], path[2]), path)))?.partition_by_key())?
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

fn g_benchmark4_composition(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
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
            .repartition(move |v_id| Ok(get_partition(*v_id, num_servers, worker_num)))
            .flat_map(|v_id| {
                let e_label_ids = vec![13];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            .repartition(move |path| Ok(get_partition(path[0], num_servers, worker_num)))
            .flat_map(|path| {
                let extend_item_id = path[0];
                let e_label_ids = vec![11];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vectices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?
            .repartition(move |path| Ok(get_partition(path[1], num_servers, worker_num)))
            .flat_map(|path| {
                let extend_item_id = path[1];
                let e_label_ids = vec![0];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vectices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?
            .repartition(move |path| Ok(get_partition(path[3], num_servers, worker_num)))
            .flat_map(|path| {
                let extend_item_id = path[3];
                let e_label_ids = vec![11];
                let check_item_id = path[2];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing)
                                                                        .filter(move |v| (v.get_id() as u64) == check_item_id);
                Ok(adj_vectices.map(move |_v| {
                    path.clone()
                }))
            })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

fn g_benchmark4_join(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
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
            .repartition(move |v_id| Ok(get_partition(*v_id, num_servers, worker_num)));
            let (edge_unit1, edge_unit2) = edge_unit1.copied()?;
            let edge_unit1 = edge_unit1.flat_map(|v_id| {
                let e_label_ids = vec![13];
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
                let e_label_ids = vec![0];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vectices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?;
            let edge_unit2 = edge_unit2.flat_map(|v_id| {
                let e_label_ids = vec![11];
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
                let e_label_ids = vec![11];
                let adj_vectices = GRAPH.get_adj_vertices(extend_item_id as usize, Some(&e_label_ids), Direction::Incoming);
                Ok(adj_vectices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?;
            edge_unit1.key_by(|path| Ok(((path[0], path[2]), path)))?
            .partition_by_key()
            .inner_join(edge_unit2.key_by(|path| Ok(((path[0],path[2]), path)))?.partition_by_key())?
            .map(|(d1, d2)| {
                let mut new_path = d1.value;
                new_path.extend(&d2.value[1..2]);
                Ok(new_path)
            })?
            .count()?
            .into_stream()?
            .flat_map(|c| Ok(vec![vec![c]].into_iter()))?
            .sink_into(output)
        }
    })
}

