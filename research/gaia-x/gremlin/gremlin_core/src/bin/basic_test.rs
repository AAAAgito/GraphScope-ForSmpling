use graph_store::config::GraphDBConfig;
use graph_store::graph_db::GlobalStoreTrait;
use graph_store::graph_db_impl::LargeGraphDB;
use gremlin_core::graph_proxy::{create_demo_graph, GRAPH};
use gremlin_core::{Partition, Partitioner, DynResult};
use gremlin_core::structure::GraphElement;
use pegasus::api::{Count, Filter, Fold, KeyBy, Map, PartitionByKey, Sink, Join, Dedup};
use pegasus::result::ResultStream;
use pegasus::{Configuration, JobConf, flat_map, ServerConf};
use std::path::PathBuf;
use std::slice::SliceIndex;
use std::sync::Arc;
use std::vec;
use structopt::StructOpt;

use std::fmt::Debug;
use pegasus::errors::{BuildJobError, JobSubmitError, SpawnJobError, StartupError};
use graph_store::graph_db::Direction;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "s", long = "servers")]
    servers: Option<PathBuf>,
    #[structopt(short = "w", long = "workers", default_value = "1")]
    workers: u32,
}

fn get_partition(id: u64, num_servers: usize, worker_num: usize) -> DynResult<u64> {
    let partition = Partition {num_servers};
    let id = id as u128;
    partition.get_partition(&id, worker_num)
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

    if config.servers.is_some() {
        conf.reset_servers(ServerConf::All);
    }

    pegasus::wait_servers_ready(conf.servers());

    create_demo_graph();

    let mut result = _path_extend_across_partition_01(conf).expect("run job failure");

    while let Some(Ok(data)) = result.next() {
        println!("{:?}", data);
    }
    pegasus::shutdown_all();
}

fn _path_extend_00(conf: JobConf) -> Result<ResultStream<u64>, JobSubmitError> {
    pegasus::run(conf, move || {
        // let id = pegasus::get_current_worker().index;
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(None).map(|v| (v.get_id() as u64)).filter(|v_id| {
                let worker_id = pegasus::get_current_worker();
                let worker_num = worker_id.local_peers as u64;
                let worker_index = worker_id.index as u64;
                *v_id % worker_num == worker_index
            }))?
            .sink_into(output)
        }
    })
}

// g.V().hasID(1).out() (Without History)
fn _path_extend_01(conf: JobConf) -> Result<ResultStream<u64>, JobSubmitError> {
    pegasus::run(conf, move || {
        // let id = pegasus::get_current_worker().index;
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(None).map(|v| (v.get_id() as u64)))?
            .filter(|v_id| Ok(*v_id == 1))?
            .flat_map(|v_id| {
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, None, Direction::Outgoing);
                Ok(adj_vertices.map(|v| v.get_id() as u64))
            })?
            .sink_into(output)
        }
    })
}

// g.V().out()
fn _path_extend_02(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        // let id = pegasus::get_current_worker().index;
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(None).map(|v| vec![(v.get_id() as u64)]))?
            .flat_map(|path| {
                let extend_item_id = path[path.len()-1];
                let adj_vertices = GRAPH.get_adj_vertices(extend_item_id as usize, None, Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?
            .sink_into(output)
        }
    })
}

// g.V().has('name','marko')
fn _path_extend_03(conf: JobConf) -> Result<ResultStream<u64>, JobSubmitError> {
    pegasus::run(conf, move || {
        // let id = pegasus::get_current_worker().index;
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(None).map(|v| v.get_id() as u64))?
            .filter(|v_id| {
                let v = GRAPH.get_vertex(*v_id as usize).unwrap();
                let prop = v.get_property("name");
                let is_marko = if let Some(dyn_type::BorrowObject::String(name)) = prop {
                    name == "marko"
                } else {
                    false
                };
                Ok(is_marko)
            })?
            .sink_into(output)
        }
    })
}

// g.V().has('name', 'marko').out('created')
fn _path_extend_04(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        // let id = pegasus::get_current_worker().index;
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(None).map(|v| v.get_id() as u64))?
            .filter(|v_id| {
                let v = GRAPH.get_vertex(*v_id as usize).unwrap();
                let prop = v.get_property("name");
                let is_marko = if let Some(dyn_type::BorrowObject::String(name)) = prop {
                    name == "marko"
                } else {
                    false
                };
                Ok(is_marko)
            })?
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
            .sink_into(output)
        }
    })
}

/* 
g.V().hasLabel('Person').match(
    __.as('a').out('created').as('b'),
    __.as('a').out('knows').as('c')) 
*/
// With Path Extension
fn _path_extend_05(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move|| {
        move |input, output| {
            let v_label_ids = vec![0];
            input.input_from(GRAPH.get_all_vertices(Some(&v_label_ids)).map(|v| v.get_id() as u64))?
            .repartition(|id| Ok(*id%2))
            .flat_map(|v_id| {
                let e_label_ids = vec![1];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            .repartition(|id| Ok(id[0]%2))
            .flat_map(|path| {
                let extend_item_id = path[0];
                let e_label_ids = vec![0];
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


/* 
g.V().hasLabel('Person').match(
    __.as('a').out('created').as('b'),
    __.as('a').out('knows').as('c')) 
*/
// With Join
fn _path_extend_06(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move|| {
        move |input, output| {
            let v_label_ids = vec![0];
            let left_partition = input.input_from(GRAPH.get_all_vertices(Some(&v_label_ids)).map(|v| v.get_id() as u64))?;
            let (left_partition, right_partition) = left_partition.copied()?;
            let left_partition = left_partition.flat_map(|v_id| {
                let e_label_ids = vec![1];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?;
            let right_partition = right_partition.flat_map(|v_id| {
                let e_label_ids = vec![0];
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
            .sink_into(output)
        }
    })
}

/*
g.V().match(__.as('a').out('created').as('b'),     Step(1)
            __.as('b').in().as('c'))               Step(2)
*/
// With Pure Path Extension

fn _path_extend_07(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(None).map(|v| v.get_id() as u64))?
            .flat_map(|v_id| {
                let e_label_ids = vec![1];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            .flat_map(|path| {
                let extend_item_id = path[1];
                let adj_vertices = GRAPH.get_adj_vertices(extend_item_id as usize, None, Direction::Incoming);
                Ok(adj_vertices.map(move |v| {
                    let mut new_path = path.clone();
                    new_path.push(v.get_id() as u64);
                    new_path
                }))
            })?
            .sink_into(output)
        }
    })
}

/*
g.V().match(__.as('a').out('created').as('b'),     Step(1)
            __.as('b').in().as('c'))               Step(2)
*/
// With Prefix Dedup Strategy
fn _path_extend_08(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        move |input, output| {
            let first_step = input.input_from(GRAPH.get_all_vertices(None).map(|v| v.get_id() as u64))?
            .flat_map(|v_id| {
                let e_label_ids = vec![1];
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?;
            let (first_step, second_step) = first_step.copied()?;
            let second_step = second_step.map(|path| Ok(path[1]))?
            .dedup()?
            .flat_map(|v_id| {
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, None, Direction::Incoming);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?;
            first_step.key_by(|path| Ok((path[1], path)))?
            .partition_by_key()
            .inner_join(second_step.key_by(|path| Ok((path[0], path)))?.partition_by_key())?
            .map(|(d1, d2)| {
                let mut new_path = d1.value;
                new_path.extend(&d2.value[1..]);
                Ok(new_path)
            })?
            .sink_into(output)
        }
    })
}


fn _path_extend_across_partition_01(conf: JobConf) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    let num_servers = conf.servers().len();
    let worker_num = conf.workers as usize;
    pegasus::run(conf, move || {
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(None).map(|v| v.get_id() as u64))?
            .filter(|v_id| Ok(*v_id==2))?
            .repartition(move |id| get_partition(*id, num_servers, worker_num))
            .flat_map(|v_id| {
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, None, Direction::Incoming);
                Ok(adj_vertices.map(move |v| {
                    let mut path = vec![];
                    path.push(v_id);
                    path.push(v.get_id() as u64);
                    path
                }))
            })?
            .repartition(move |path| get_partition(path[1], num_servers, worker_num))
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
            .repartition(move |path| get_partition(path[2], num_servers, worker_num))            
            .flat_map(|path| {
                let extend_item_id = path[2];
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
