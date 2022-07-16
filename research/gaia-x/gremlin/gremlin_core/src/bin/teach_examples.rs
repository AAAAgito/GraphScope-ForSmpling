use graph_store::config::{GraphDBConfig, JsonConf};
use graph_store::graph_db::{GlobalStoreTrait, self};
use graph_store::graph_db::LocalVertex;
use graph_store::graph_db::GlobalStoreUpdate;
use graph_store::graph_db_impl::{LargeGraphDB, MutableGraphDB};
use graph_store::utils::Iter;
use graph_store::schema::LDBCGraphSchema;
use graph_store::common::{DefaultId, LabelId, Label, InternalId, INVALID_LABEL_ID};
use graph_store::ldbc::{LDBCVertexParser,LDBCEdgeParser};
use gremlin_core::graph_proxy::{create_demo_graph, GRAPH};
use gremlin_core::structure::GraphElement;
use pegasus::api::{Count, Filter, Merge, Fold, KeyBy, Map, PartitionByKey, Sink, Join, Dedup};
use pegasus::result::ResultStream;
use pegasus::{Configuration, JobConf, flat_map, ServerConf};
use strum_macros::ToString;
use core::time;
use std::path::PathBuf;
use std::slice::SliceIndex;
use std::sync::Arc;
use std::{vec, array};
use std::collections::HashSet;
use std::collections::HashMap;
use std::collections::VecDeque;
use structopt::StructOpt;

use std::fmt::Debug;
use pegasus::errors::{BuildJobError, JobSubmitError, SpawnJobError, StartupError};
use graph_store::graph_db::Direction;

use std::time::Instant;

use rand;
use rand::Rng;

use std::fs::File;
use std::io::prelude::*;

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

    let start_full = Instant::now();
    // _sampling_degree_distribution();

    _test_pattern_mining();
    // _test_pattern_generate();
    let end_full = Instant::now();
    println!("time cost: {:?}",end_full.duration_since(start_full));
    pegasus::shutdown_all();
}

// done
fn _test_pattern_generate() {
    let mut BI11: Vec<Vec<Vec<u64>>> =Vec::new();
    BI11.push(vec![vec![0,8],vec![]]);
    BI11.push(vec![vec![1,9],vec![17,0]]);
    BI11.push(vec![vec![2,9],vec![17,0]]);
    BI11.push(vec![vec![3,9],vec![17,0]]);
    BI11.push(vec![vec![4,1],vec![11,1,12,5,12,6]]);
    BI11.push(vec![vec![5,1],vec![11,2,12,4,12,6]]);
    BI11.push(vec![vec![6,1],vec![11,3,12,4,12,5]]);
    let result = _pattern_generate(BI11);

    print!("{:?}", result);

}

// done
fn _test_pattern_mining() {

    let mut f = std::fs::File::create("sampling_result.txt").unwrap();
    let mut G1: Vec<Vec<Vec<u64>>> =Vec::new();
    G1.push(vec![vec![0,1],vec![12,1,12,2]]);
    G1.push(vec![vec![1,1],vec![12,2]]);
    G1.push(vec![vec![2,1],vec![]]);
    let mut str_info1: Vec<String> = Vec::new();
    let res = _pattern_generate(G1);
    for i in res.split("==") {
        str_info1.push(i.to_string());
    }

    let mut G2: Vec<Vec<Vec<u64>>> =Vec::new();
    G2.push(vec![vec![0,12],vec![]]);
    G2.push(vec![vec![1,1],vec![12,2,15,0]]);
    G2.push(vec![vec![2,1],vec![15,0]]);
    let mut str_info2: Vec<String> = Vec::new();
    let res = _pattern_generate(G2);
    for i in res.split("==") {
        str_info2.push(i.to_string());
    }

    let mut G3: Vec<Vec<Vec<u64>>> =Vec::new();
    G3.push(vec![vec![0,1],vec![12,2,12,3,12,1]]);
    G3.push(vec![vec![1,1],vec![12,3,12,2]]);
    G3.push(vec![vec![2,1],vec![12,3]]);
    G3.push(vec![vec![3,1],vec![]]);
    let mut str_info3: Vec<String> = Vec::new();
    let res = _pattern_generate(G3);
    for i in res.split("==") {
        str_info3.push(i.to_string());
    }

    // message change to comment
    let mut G4: Vec<Vec<Vec<u64>>> =Vec::new();
    G4.push(vec![vec![0,1],vec![13,1,11,3]]);
    G4.push(vec![vec![1,2],vec![0,2]]);
    G4.push(vec![vec![2,1],vec![11,3]]);
    G4.push(vec![vec![3,9],vec![]]);
    let mut str_info4: Vec<String> = Vec::new();
    let res = _pattern_generate(G4);
    for i in res.split("==") {
        str_info4.push(i.to_string());
    }

    let mut BI11: Vec<Vec<Vec<u64>>> =Vec::new();
    BI11.push(vec![vec![0,8],vec![]]);
    BI11.push(vec![vec![1,9],vec![17,0]]);
    BI11.push(vec![vec![2,9],vec![17,0]]);
    BI11.push(vec![vec![3,9],vec![17,0]]);
    BI11.push(vec![vec![4,1],vec![11,1,12,5,12,6]]);
    BI11.push(vec![vec![5,1],vec![11,2,12,6]]);
    BI11.push(vec![vec![6,1],vec![11,3]]);
    let mut str_info11: Vec<String> = Vec::new();
    let res = _pattern_generate(BI11);
    for i in res.split("==") {
        str_info11.push(i.to_string());
    }

    let mut BI12: Vec<Vec<Vec<u64>>> =Vec::new();
    BI12.push(vec![vec![0,1],vec![]]);
    BI12.push(vec![vec![1,2],vec![0,0,3,2]]);
    BI12.push(vec![vec![2,3],vec![]]);
    let mut str_info12: Vec<String> = Vec::new();
    let res = _pattern_generate(BI12);
    for i in res.split("==") {
        str_info12.push(i.to_string());
    }
    
    // Experiment
    let area = vec![20,40,80,160];
    let rate = vec![10,40,20,10,5];
    let mut string_to_pattern = HashMap::new();
    string_to_pattern.insert("_1_2_13_0_1_0_1_9_11_0_0_0_1_9_11_0_1_0_2_1_0_0_0_0_".to_string(), "G4");
    string_to_pattern.insert("_1_1_12_0_0_1_1_1_12_1_2_0_1_1_12_1_2_1_".to_string(), "G1");
    string_to_pattern.insert("_1_1_12_0_0_1_1_1_12_1_2_0_1_1_12_1_2_1_1_9_11_0_0_1_1_9_11_0_1_2_1_9_11_0_2_0_9_8_17_0_0_0_9_8_17_0_1_0_9_8_17_0_2_0_".to_string(), "B11");
    string_to_pattern.insert("_1_12_15_0_0_0_1_12_15_0_1_0_1_1_12_1_1_0_".to_string(), "G2");
    string_to_pattern.insert("_2_1_0_0_0_0_2_3_3_0_0_0_".to_string(), "B12");

    for i in area {
        for j in rate.clone() {
            f.write("\n area ".as_bytes());
            f.write(i.to_string().as_bytes());
            f.write("\n".as_bytes());
            let result = _sampling_arrange(j, i, vec![str_info1.clone(),str_info2.clone(),str_info4.clone(),str_info11.clone(),str_info12.clone()]);
            println!("pattern code 20{:?}", result);
            f.write("\n rate ".as_bytes());
            f.write(j.to_string().as_bytes());
            for i in result.keys() {
                f.write(":  ".as_bytes());
                f.write(string_to_pattern[i].as_bytes());
                f.write(" ".as_bytes());
                f.write(result[i].to_string().as_bytes());
            }

        }
    }
    // let result = _sampling_arrange(20, 20, vec![str_info1.clone(),str_info2.clone(),str_info4.clone(),str_info11.clone(),str_info12.clone()]);
    // println!("pattern code 50 {:?}", result);
    // let result = _sampling_arrange(40, 20, vec![str_info1.clone(),str_info2.clone(),str_info4.clone(),str_info11.clone(),str_info12.clone()]);
    // println!("pattern code 100 {:?}", result);
    // let result = _sampling_arrange(5, 40, vec![str_info1.clone(),str_info2.clone(),str_info4.clone(),str_info11.clone(),str_info12.clone()]);
    // println!("pattern code 20{:?}", result);
    // let result = _sampling_arrange(10, 40, vec![str_info1.clone(),str_info2.clone(),str_info4.clone(),str_info11.clone(),str_info12.clone()]);
    // println!("pattern code 50 {:?}", result);
    // let result = _sampling_arrange(20, 40, vec![str_info1.clone(),str_info2.clone(),str_info4.clone(),str_info11.clone(),str_info12.clone()]);
    // println!("pattern code 100 {:?}", result);
    // let result = _sampling_arrange(5, 80, vec![str_info1.clone(),str_info2.clone(),str_info4.clone(),str_info11.clone(),str_info12.clone()]);
    // println!("pattern code 20{:?}", result);
    // let result = _sampling_arrange(10, 80, vec![str_info1.clone(),str_info2.clone(),str_info4.clone(),str_info11.clone(),str_info12.clone()]);
    // println!("pattern code 50 {:?}", result);
    // let result = _sampling_arrange(20, 80, vec![str_info1.clone(),str_info2.clone(),str_info4.clone(),str_info11.clone(),str_info12.clone()]);
    // println!("pattern code 100 {:?}", result);
}


fn _sampling_all_vertex(conf: JobConf) -> Result<ResultStream<u64>, JobSubmitError> {
    pegasus::run(conf, move || {
        move |input, output| {
            input.input_from(GRAPH
                .get_all_vertices(None)
                .map(|v| (v.get_id() as u64)))?
                
            .filter(move |v_id| {
                let adj_num = GRAPH.get_both_vertices(*v_id as usize, None).count();
                Ok(adj_num != 0)
            })?
            .sink_into(output)
        }
    })
}
fn _sampling_start_vertex(conf: JobConf, area_num: u64) -> Result<ResultStream<u64>, JobSubmitError> {
    let mut rng = rand::thread_rng();
    let partition = (GRAPH.get_all_vertices(None).count() as u64)/area_num as u64;
    // println!("Graph Size = {:?}", GRAPH.get_all_vertices(None).count() as u64);
    assert_eq!(partition>0,true);
    let select_id = rng.gen_range(0, partition);
    pegasus::run(conf, move || {
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(None).map(|v| (v.get_id() as u64)))?
            .filter(move |v_id| {
                let adj_num = GRAPH.get_both_vertices(*v_id as usize, None).count();
                Ok(adj_num != 0 && (v_id%partition) == select_id)
            })?
            .sink_into(output)
        }
    })
}

fn _sampling_start_vertex_alpha(conf: JobConf, label: u8) -> Result<ResultStream<u64>, JobSubmitError> {
    pegasus::run(conf, move || {
        move |input, output| {
            input.input_from(GRAPH
                .get_all_vertices(Some(&vec![label]))
                .map(|v| (v.get_id() as u64)))?
                
            .filter(move |v_id| {
                let adj_num = GRAPH.get_both_vertices(*v_id as usize, None).count();
                Ok(adj_num != 0)
            })?
            .sink_into(output)
        }
    })
}

fn _sampling_label(conf: JobConf, label: u8) -> Result<ResultStream<u64>, JobSubmitError> {
    pegasus::run(conf, move || {
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(Some(&vec![label])).map(|v| (v.get_id() as u64)))?
            .sink_into(output)
        }
    })
}

fn _sampling_degree_distribution() ->HashMap<u64,u64> {
    let mut vtx_distribution: HashMap<u64,u64> = HashMap::new();
    for i in 0..13u8 {
        let conf = JobConf::new("distribution");
        let mut result = _sampling_label(conf, i).expect("Run Job Error!");
        let mut count = GRAPH.count_all_vertices(Some(&vec![i])) as u64;
        let mut total_degree = 0u64;
        while let Some(Ok(data)) = result.next() {
            total_degree += GRAPH.get_both_vertices(data as usize, None).count() as u64;
        }
        if count >0 {
            println!("label {:?}:   count: {:?}   deg: {:?}", i, count, total_degree/count);
        }
        vtx_distribution.insert(i as u64, count);

    }
    // for i in 0..22u8 {
    //     let edges = GRAPH.get_all_edges(Some(&vec![i])).count();
    //     let edges = GRAPH.get_all_edges(Some(&vec![i]));
    //     let mut label_set = HashSet::new();
    //     let mut label_set2 = HashSet::new();
    //     for i in edges {
    //         let label = GRAPH.get_vertex(i.get_src_id()).unwrap().get_label();
    //         let label2 = GRAPH.get_vertex(i.get_dst_id()).unwrap().get_label();

    //         if !label_set.contains(&label) {
    //             label_set.insert(label);
    //         }
    //         if !label_set2.contains(&label2) {
    //             label_set2.insert(label2);
    //         }
    //     }
    //     println!("edge label {:?}, src_label {:?}, dst_label {:?}",i, label_set, label_set2);
    // }
    vtx_distribution
}

fn _sampling_adjvertex(conf: JobConf, src: &Vec<u64>) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        let src = src.clone();
        move |input, output| {
            input.input_from(src.into_iter())?
            .flat_map(|v_id| {
                let adj_vertices = GRAPH.get_both_vertices(v_id as usize, None);
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


fn _sampling_invertex(conf: JobConf, src: &Vec<u64>) -> Result<ResultStream<u64>, JobSubmitError> {
    pegasus::run(conf, move || {
        let src = src.clone();
        move |input, output| {
            input.input_from(src.into_iter())?
            .flat_map(|v_id| {
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, None, Direction::Incoming);
                Ok(adj_vertices.map(move |v| {
                    v.get_id() as u64
                }))
            })?
            
            .sink_into(output)
        }
    })
}


fn _counting_pattern_indepth2_O(conf: JobConf, src: &Vec<u64>) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        let src = src.clone();
        move |input, output| {
            input.input_from(src.into_iter())?
                .flat_map(|v_id| {
                    let adj_vertices = GRAPH.get_adj_edges(v_id as usize, None, Direction::Outgoing);
                    Ok(adj_vertices.map(move |v| {
                        let mut path = vec![];
    
                        path.push(v_id);
                        path.push(v.get_label() as u64);
                        path.push(v.get_other_id() as u64);
                        path
                    }))
                })?
            
            .sink_into(output)
        }
    })
}


fn _counting_pattern_indepth2_OO(conf: JobConf, src: &Vec<u64>) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        let src = src.clone();
        move |input, output| {
            input.input_from(src.into_iter())?
                .flat_map(|v_id| {
                    let adj_vertices = GRAPH.get_adj_edges(v_id as usize, None, Direction::Outgoing);
                    Ok(adj_vertices.map(move |v| {
                        let mut path = vec![];
    
                        path.push(v_id);
                        path.push(v.get_label() as u64);
                        path.push(v.get_other_id() as u64);
                        path
                    }))
                })?
                .flat_map( |path2| {

                    let adj_vertices = GRAPH.get_adj_edges(path2[2] as usize, None, Direction::Outgoing);
                    Ok(adj_vertices.map(move |v| {
                        let mut path = path2.clone();
                        path.push(v.get_label() as u64);
                        path.push(v.get_other_id()as u64);
                        path
                    }))

                } )?
            
            .sink_into(output)
        }
    })
}

fn _counting_pattern_indepth2_IO(conf: JobConf, src: &Vec<u64>) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        let src = src.clone();
        move |input, output| {
            input.input_from(src.into_iter())?
                .flat_map(|v_id| {
                    let adj_vertices = GRAPH.get_adj_edges(v_id as usize, None, Direction::Incoming);
                    Ok(adj_vertices.map(move |v| {
                        let mut path = vec![];
    
                        path.push(v_id);
                        path.push(v.get_label() as u64);
                        path.push(v.get_other_id() as u64);
                        path
                    }))
                })?
                .flat_map( |path2| {

                    let adj_vertices = GRAPH.get_adj_edges(path2[2] as usize, None, Direction::Outgoing);
                    Ok(adj_vertices.map(move |v| {
                        let mut path = path2.clone();
                        path.push(v.get_label() as u64);
                        path.push(v.get_other_id()as u64);
                        path
                    }))

                } )?
            
            .sink_into(output)
        }
    })
}


fn _counting_pattern_indepth2_OI(conf: JobConf, src: &Vec<u64>) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        let src = src.clone();
        move |input, output| {
            input.input_from(src.into_iter())?
                .flat_map(|v_id| {
                    let adj_vertices = GRAPH.get_adj_edges(v_id as usize, None, Direction::Outgoing);
                    Ok(adj_vertices.map(move |v| {
                        let mut path = vec![];
    
                        path.push(v_id);
                        path.push(v.get_label() as u64);
                        path.push(v.get_other_id() as u64);
                        path
                    }))
                })?
                .flat_map( |path2| {

                    let adj_vertices = GRAPH.get_adj_edges(path2[2] as usize, None, Direction::Incoming);
                    Ok(adj_vertices.map(move |v| {
                        let mut path = path2.clone();
                        path.push(v.get_label() as u64);
                        path.push(v.get_other_id()as u64);
                        path
                    }))

                } )?
            
            .sink_into(output)
        }
    })
}
// pattern mining
// src: 此次faltmap的点id，path：按gid排列的点id，target——label：拓展边label
fn _mining_adjvertex(conf: JobConf, src_gid: u64, target_label: u8, target_gid: u64, path: &Vec<Vec<u64>>, dir: u64) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    let mut direction = Direction::Outgoing;
    if dir==1{
        direction = Direction::Incoming;
    }
    pegasus::run(conf, move || {
        let src = path.clone();
        move |input, output| {
            input.input_from(src.into_iter())?
            .flat_map( move |v_id| {
                let adj_vertices = GRAPH.get_adj_vertices(v_id[src_gid as usize] as usize, Some(&vec![target_label]), direction);
                Ok(adj_vertices.map(move |v| {
                    let mut path = v_id.clone();
                    
                    if path[target_gid as usize]!=0 {
                        if path[target_gid as usize] == v.get_id() as u64 {
                            path[target_gid as usize] = v.get_id() as u64;

                        }
                        else {
                            
                            path = vec![0];
                        }
                    }
                    else {
                        path[target_gid as usize] = v.get_id() as u64;

                    }
                    path
                }))
            })?
            
            .sink_into(output)
        }
    })
}


// sample vertex
// store into graph
// mining mattern
// return 10 most pattern
fn _sampling_arrange(sample_rate: u64, area_num: u64, pattern: Vec<Vec<String>>) -> HashMap<std::string::String, u64> {

    let mut sampled = HashSet::new();
    let mut mut_graph: MutableGraphDB<DefaultId, InternalId> = GraphDBConfig::default().new();

    if sample_rate==100u64 {
        let conf0 = JobConf::new("conf0");
        let mut all_vertices = _sampling_all_vertex(conf0).expect("Run Job Error!");
        while let Some(Ok(data)) = all_vertices.next() {
            sampled.insert(data);
            let sample_label = GRAPH.get_vertex(data as usize).unwrap().get_label();
            let v1: DefaultId = LDBCVertexParser::to_global_id(data as usize, 0);
            mut_graph.add_vertex(v1, sample_label);
        }
    }
    else {
        let vtx_distribution = _sampling_degree_distribution();
        let mut bound_table: Vec<u64> = Vec::new();
        let sample_num = (GRAPH.get_all_vertices(None).count() as u64)*sample_rate/100;
        let total_num = (GRAPH.get_all_vertices(None).count() as u64)*sample_rate/100;
        println!("to_sample num {:?}", sample_num);

        for i in 0..13u64 {
            let exist_num = vtx_distribution[&(i)];
            let min_num = total_num/100;
            let to_sample = exist_num *sample_rate/100;
            if min_num > to_sample {
                if min_num > exist_num {
                    bound_table.push(exist_num);
                }
                else {
                    bound_table.push(min_num);
                }
            }
            else {
                bound_table.push(to_sample);
            }
        }
        let conf1 = JobConf::new("conf1");

        let mut buffer = vec![];
        // select area
        for i in 0..13u8 {
            let confi = JobConf::new("conf1");
            let start_num = 1+ area_num * vtx_distribution[&(i as u64)] / total_num;
            let mut times = 0;
            let mut start_list = _sampling_start_vertex_alpha(confi, i).expect("Run Job Error!");
            while let Some(Ok(data)) = start_list.next() {
                if !sampled.contains(&data){
                    sampled.insert(data);
                    let sample_label = GRAPH.get_vertex(data as usize).unwrap().get_label();
                    bound_table[sample_label[0]as usize] = bound_table[sample_label[0] as usize] -1;
                    if sample_label[1] != 255u8 {
                        bound_table[sample_label[1]as usize] = bound_table[sample_label[1] as usize] -1;
                    }
                    let v1: DefaultId = LDBCVertexParser::to_global_id(data as usize, 0);
                    mut_graph.add_vertex(data as usize, sample_label);
                    buffer.push(data);
                    if times>start_num {
                        break;
                    }
                    times = times+1;
                }
                // println!("{:?}", data);
            }
        }
        let mut new_bound = bound_table.clone();
        let mut remain = 0;
        for i in new_bound.clone() {
            remain = remain +i;
        }
        while remain>0 {
    
            let conf2 = JobConf::new("conf2");
            if buffer.len() as u64 ==0 {
                println!("All vertex getted");
                break;
            }
            let mut result2 = _sampling_adjvertex(conf2, &buffer).expect("Run Job Error!");
            buffer.clear();
            while let Some(Ok(data)) = result2.next() {
                let sample_id: u64 = data[1];
                let src_id: u64 = data[0];
                
                let sample_label = GRAPH.get_vertex(sample_id as usize).unwrap().get_label();
                if !sampled.contains(&sample_id) && new_bound[(sample_label[0]) as usize] >0 && (sample_label[1]== 255 
                    || (sample_label[1] != 255 && new_bound[sample_label[1] as usize] >0)){
                    sampled.insert(sample_id);
    
                    if new_bound[sample_label[0]as usize] <= 0 {
                        continue;
                    }
                    if sample_label[1]as usize != 255{
                        if new_bound[sample_label[1]as usize] as u64 <=0{
                            continue;
                        }
                    }
                    new_bound[sample_label[0]as usize] = new_bound[sample_label[0] as usize] -1;
                    if sample_label[1] != 255u8 {
                        new_bound[sample_label[1]as usize] = new_bound[sample_label[1] as usize] -1;
                    }
                    mut_graph.add_vertex(sample_id as usize, sample_label);
                    
                    let v2: DefaultId = LDBCVertexParser::to_global_id(src_id as usize, 0);
                    let src_label = GRAPH.get_vertex(src_id as usize).unwrap().get_label();
                    if !sampled.contains(&src_id) && !(sampled.len() as u64 >= sample_num){
                        mut_graph.add_vertex(v2, src_label);
                    }
    
                    buffer.push(sample_id);
                }
                // println!("{:?}", data);
            }
            remain = 0;
            for i in new_bound.clone() {
                remain = remain +i;
            }
        }
    }
    

    
    for i in sampled.clone() {
        // edge add
        let mut adjout = GRAPH.get_adj_edges(i as usize, None, Direction::Outgoing);
        while let Some(data) = adjout.next() {
            let label = data.get_label();
            let dst_id: u64 = data.get_dst_id() as u64;
            if sampled.contains(&dst_id) {
                mut_graph.add_edge(i as usize, dst_id as usize, label);
            }
        }

    }
    let schema_file = "data/schema.json";
    let schema =
            LDBCGraphSchema::from_json_file(schema_file).expect("Read graph schema error!");
    let graphdb = &mut_graph.into_graph(schema);

    // for i in 0..13 {
    //     let count = graphdb.get_all_vertices(Some(&vec![i as u8])).count();
    //     let count2 = GRAPH.get_all_vertices(Some(&vec![i as u8])).count();
    //     println!("After Sampling label: {:?}, count: {:?}",i,count);
    //     println!("Global graph label: {:?}, count: {:?}",i,count2);
    // }
    // pattern mining
    // let mining_result = _pattern_mining(pattern.clone(), graphdb);
    // mining_result

    // pattern cardinality estimating

    let start_full = Instant::now();

    println!("finish sampling");
    let confP0 = JobConf::new("conf0");
    let confP1 = JobConf::new("conf0");
    let confP2 = JobConf::new("conf0");
    let confP3 = JobConf::new("conf0");
    let confQ = JobConf::new("conf0");
    let mut pattern_table: HashMap<String, u64> = HashMap::new();
    let srcvtx = graphdb.get_all_vertices(None);
    let mut vtxsrc = Vec::new();
    for i in srcvtx {
        vtxsrc.push(i.get_id() as u64);
    }
    println!("start counting");
    let mut counting_patternO = _counting_pattern_indepth2_O(confP0, &vtxsrc).expect("Run Job Error!");

    let mut counting_patternOO = _counting_pattern_indepth2_OO(confP1, &vtxsrc).expect("Run Job Error!");

    let mut counting_patternOI = _counting_pattern_indepth2_OI(confP2, &vtxsrc).expect("Run Job Error!");

    let mut counting_patternIO = _counting_pattern_indepth2_IO(confP3, &vtxsrc).expect("Run Job Error!");

    let mut count =0;
    let mut thousand_count =0;

    while let Some(data) = counting_patternO.next() {
        let pattern_info = data.unwrap();
        // println!("pattern info {:?}",pattern_info);
        // TODO: repair the bug of edge info
        for i in 0..pattern_info.len()/2+1 {
            let mut edge_set = Vec::new();
            let mut src_set = Vec::new();
            if !vtxsrc.contains(&pattern_info[2*i]) {
                break;
            }
            // generating decode_result
            let label_vtx = graphdb.get_vertex(2*i).unwrap().get_label();
            src_set.push(vec![i as u64,label_vtx[0] as u64]);

            if i < pattern_info.len()/2 {
                edge_set.push(pattern_info[2*i+1] as u64);
                edge_set.push(i as u64);
                edge_set.push(0);

                
            }
            src_set.push(edge_set);
            if i== pattern_info.len()/2 {
                src_set.push(vec![(i+1)as u64,graphdb.get_vertex(2*(i+1)).unwrap().get_label()[0] as u64]);
                src_set.push(vec![]);
                let decode_table = vec![src_set];
                println!("vtx_src: {:?}",decode_table);
                let code =_pattern_generate_2side(decode_table);
                if pattern_table.contains_key(&code) {
                    let mut prev_value = pattern_table.remove(&code).unwrap();
                    prev_value = prev_value +1;
                    pattern_table.insert(code, prev_value);
                }
                else {
                    pattern_table.insert(code, 1);
                }
                break;
            }
        }
        count = count +1;
        if count==100000 {
            thousand_count = thousand_count +1;
            count = 0;
            println!("count {:?} 00000",thousand_count);
        }

    }


    while let Some(data) = counting_patternOO.next() {
        let pattern_info = data.unwrap();
        // println!("pattern info {:?}",pattern_info);
        for i in 0..pattern_info.len()/2+1 {
            let mut edge_set = Vec::new();
            let mut src_set = Vec::new();
            if !vtxsrc.contains(&pattern_info[2*i]) {
                break;
            }
            // generating decode_result
            let label_vtx = graphdb.get_vertex(2*i).unwrap().get_label();
            src_set.push(vec![i as u64,label_vtx[0] as u64]);

            if i < pattern_info.len()/2 {
                edge_set.push(pattern_info[2*i+1] as u64);
                edge_set.push(i as u64);
                edge_set.push(0);

                
            }
            src_set.push(edge_set);
            if i== pattern_info.len()/2 {
                src_set.push(vec![(i+1)as u64,graphdb.get_vertex(2*(i+1)).unwrap().get_label()[0] as u64]);
                src_set.push(vec![]);
                let decode_table = vec![src_set];
                // println!("vtx_src: {:?}",vtx_src);
                let code =_pattern_generate_2side(decode_table);
                if pattern_table.contains_key(&code) {
                    let mut prev_value = pattern_table.remove(&code).unwrap();
                    prev_value = prev_value +1;
                    pattern_table.insert(code, prev_value);
                }
                else {
                    pattern_table.insert(code, 1);
                }
                break;
            }
        }
        count = count +1;
        if count==100000 {
            thousand_count = thousand_count +1;
            count = 0;
            println!("count {:?} 00000",thousand_count);
        }

    }

    while let Some(data) = counting_patternIO.next() {
        let pattern_info = data.unwrap();
        // println!("pattern info {:?}",pattern_info);
        for i in 0..pattern_info.len()/2+1 {
            let mut edge_set = Vec::new();
            let mut src_set = Vec::new();
            if !vtxsrc.contains(&pattern_info[2*i]) {
                break;
            }
            // generating decode_result
            let label_vtx = graphdb.get_vertex(2*i).unwrap().get_label();
            src_set.push(vec![i as u64,label_vtx[0] as u64]);

            if i < pattern_info.len()/2 {
                if i==1 {
                    edge_set.push(pattern_info[2*i+1] as u64);
                    edge_set.push(i as u64);
                    edge_set.push(0);

                }
                else {
                    edge_set.push(pattern_info[2*i+1] as u64);
                    edge_set.push((i+1) as u64);
                    edge_set.push(1);
                }
                
            }
            src_set.push(edge_set);
            if i== pattern_info.len()/2 {
                src_set.push(vec![(i+1)as u64,graphdb.get_vertex(2*(i+1)).unwrap().get_label()[0] as u64]);
                src_set.push(vec![]);
                let decode_table = vec![src_set];
                // println!("vtx_src: {:?}",vtx_src);
                let code =_pattern_generate_2side(decode_table);
                if pattern_table.contains_key(&code) {
                    let mut prev_value = pattern_table.remove(&code).unwrap();
                    prev_value = prev_value +1;
                    pattern_table.insert(code, prev_value);
                }
                else {
                    pattern_table.insert(code, 1);
                }
                break;
            }
        }
        count = count +1;
        if count==100000 {
            thousand_count = thousand_count +1;
            count = 0;
            println!("count {:?} 00000",thousand_count);
        }

    }

    while let Some(data) = counting_patternOI.next() {
        let pattern_info = data.unwrap();
        // println!("pattern info {:?}",pattern_info);
        for i in 0..pattern_info.len()/2+1 {
            let mut edge_set = Vec::new();
            let mut src_set = Vec::new();
            if !vtxsrc.contains(&pattern_info[2*i]) {
                break;
            }
            // generating decode_result
            let label_vtx = graphdb.get_vertex(2*i).unwrap().get_label();
            src_set.push(vec![i as u64,label_vtx[0] as u64]);

            if i < pattern_info.len()/2 {
                if i==0 {
                    edge_set.push(pattern_info[2*i+1] as u64);
                    edge_set.push((i+1) as u64);
                    edge_set.push(0);

                }
                else {
                    edge_set.push(pattern_info[2*i+1] as u64);
                    edge_set.push(i as u64);
                    edge_set.push(1);
                }
                
            }
            src_set.push(edge_set);
            if i== pattern_info.len()/2 {
                src_set.push(vec![(i+1)as u64,graphdb.get_vertex(2*(i+1)).unwrap().get_label()[0] as u64]);
                src_set.push(vec![]);
                let decode_table = vec![src_set];
                // println!("vtx_src: {:?}",vtx_src);
                let code =_pattern_generate_2side(decode_table);
                if pattern_table.contains_key(&code) {
                    let mut prev_value = pattern_table.remove(&code).unwrap();
                    prev_value = prev_value +1;
                    pattern_table.insert(code, prev_value);
                }
                else {
                    pattern_table.insert(code, 1);
                }
                break;
            }
        }
        count = count +1;
        if count==100000 {
            thousand_count = thousand_count +1;
            count = 0;
            println!("count {:?} 00000",thousand_count);
        }

    }
    println!("finish collecting");
    let end_full = Instant::now();
    println!("time cost for generating table: {:?}s",end_full.duration_since(start_full));
    // println!("pattern table {:?}",pattern_table);
    let res = _pattern_estimation_CEG_vertex_version(graphdb, pattern, pattern_table, 0, 2);
    res
    
}


fn _pattern_mining(pattern: Vec<Vec<String>>, graphdb: &LargeGraphDB) -> HashMap<std::string::String, u64> {
    
    let mut catalog: HashMap<String,u64> = HashMap::new();
    for l in pattern{
        // idx: each vertex -> [[gid, label], adjlist]
        // adjlist: Vec![edge label, dst_gid]
        let decode_res: Vec<Vec<Vec<u64>>> = _decode_pattern(l.clone());
        println!("{:?}",decode_res);
        let mut occur_set = HashSet::new();
        let mut graph_vertices = Vec::new();
        for i in graphdb.get_all_vertices(None) {
            graph_vertices.push(i.get_id() as u64);
        }
        let mut path: Vec<Vec<u64>> = Vec::new();
        for i in decode_res.clone() {
            let mut local_todo: Vec<u64> = Vec::new();
            let gid = i[0][0];
            let label = i[0][1];
            let adj_vtx = i[1].clone();
            occur_set.insert(gid);
            if gid==0 {
                let start = graphdb.get_all_vertices(Some(&vec![label as u8]));
                for i in start {
                    let mut path_cell = Vec::new();
                    path_cell.push(i.get_id() as u64);
                    for i in 0..decode_res.len()-1 {
                        path_cell.push(0);
                    }
                    path.push(path_cell);
                    local_todo.push(i.get_id() as u64);
                }
            }
            println!("gid:  {:?}",gid);
            for j in 0..adj_vtx.len()/3 {
                let mut target_label = adj_vtx[j*3];
                let target_gid = adj_vtx[j*3+1];
                let dir = adj_vtx[j*3+2];
                if target_gid< gid {
                    // been visited
                    continue;
                }
                let conf = JobConf::new("mining");
                
                let mut result = _mining_adjvertex(conf, gid, target_label as u8, target_gid, &path, dir).expect("Run Job Error!");
                occur_set.insert(target_gid);
                let mut p: Vec<Vec<u64>> = Vec::new();
                while let Some(Ok(data)) = result.next() {
                    if data==vec![0] {
                        continue;
                    }
                    let mut isload = true;
                    for i in data.clone() {
                        if !graph_vertices.contains(&i) {
                            isload = false;
                        }
                    }
                    if isload {
                        p.push(data);
                    }
                }
                path = p;
            }
        }
        path.sort();
        path.dedup();
        let mut i=0;

        while i <path.len()-1{
            let mut k = path[i].clone();
            k.sort();
            // index problem
            for j in 0..k.len()-1 {
                if k[j]==k[j+1] || k[j]==0 {
                    path.remove(i);
                    break;
                }
            }
            i = i+1;
        }
        let mut local_code:String = String::from("");
        let mut list = l.clone();
        list.sort();
        for m in list{
            local_code = local_code + &m;
            local_code = local_code + "_";
        }
        catalog.insert(local_code, path.len() as u64);
    }
    catalog
}


// this version can only be used in depth =2, otherwise flatmap part should be updated
fn _pattern_estimation_CEG_vertex_version(graphdb: &LargeGraphDB, pattern: Vec<Vec<String>>, pattern_table: HashMap<std::string::String, u64>, aggregator: u64, depth: u64) -> HashMap<std::string::String, u64> {
    let mut catalog: HashMap<String,u64> = HashMap::new();
    for i in pattern {
        // idx: each vertex -> [[gid, label], adjlist]
        // adjlist: Vec![edge label, dst_gid]
        let decode_res: Vec<Vec<Vec<u64>>> = _decode_pattern(i.clone());
        let pattern_table2 = pattern_table.clone();
        println!("gpe length {:?}", decode_res.len());
        // to detect if it is loop(in thresold length), it stores the previous vertex combination
        // once there is a new vertex that has two connection with vtx in previous combination
        // treat as loop
        let mut occur_set: HashSet<Vec<u64>> = HashSet::new();
        let mut occur_vtx: HashSet<u64> = HashSet::new();

        let mut estimate_result:f64 =0f64;

        // first pick a start point (can be any gid)
        // then record number of  cardinality of first node's edge combination
        // for each edge in first node, BFS it and use previous edge info(depends how many hops) --> parameter for mutliply
        // List of BFS: store a whole info:  previous_edge_labels, extend_edge_labels ([in out] together, decode should add in-edge info)
        let mut gpe: Vec<Vec<u64>> = Vec::new();
        let mut result_list: Vec<Vec<u64>> = Vec::new();
        for j in 0..decode_res.len() {
            gpe.push(vec![decode_res[j][0][0]]);
        }
        while !gpe.is_empty() {
            let cur_path = gpe[0].clone();
            gpe.remove(0);
            let start_gid = cur_path[cur_path.len()-1];
            let decode_copy = decode_res[start_gid as usize].clone();
            let list_adj = decode_copy[1].clone();
            for i in 0..list_adj.len()/3 {
                let mut to_push_list = cur_path.clone();
                if !to_push_list.contains(&list_adj[i*3+1]) {
                    to_push_list.push(list_adj[i*3+1]);
                }
                else {
                    continue;
                }
                if to_push_list.len() < decode_res.len() {
                    gpe.push(to_push_list);
                }
                else {
                    result_list.push(to_push_list);
                }
            }
            println!("remian {:?}", gpe);
        }
        println!("gpe: {:?}",result_list);
        // path generated

        // case1: pattern size <= depth, directly get result
        if (result_list[0].len() < depth as usize) {
            // return cardinality
            let mut local_code:String = String::from("");
            let mut list = i.clone();
            list.sort();
            for m in list{
                local_code = local_code + &m;
                local_code = local_code + "_";
            }
            estimate_result = pattern_table[&local_code] as f64;
            catalog.insert(local_code, estimate_result as u64);
        }

        // case2 pattern size > depth, get front partial from pattern table, and extend the remain vertex
        else {
            let mut estimations: Vec<u64> = Vec::new();
            for path in result_list{
                let mut local_code:String = String::from("");
                let mut pattern_table_vertex: HashSet<u64> = HashSet::new();
                for l in 0..depth as usize {
                    pattern_table_vertex.insert(path[l]);
                }
                let mut decode_table = decode_res.clone();
                let mut partial_decode_table = Vec::new();
                let mut partial_added_vtx =Vec::new();
                for i in 0..decode_table.len() {
                    let start_gid = decode_table[i][0][0];
                    if !pattern_table_vertex.contains(&start_gid) {
                        continue;
                    }
                    let start_adj_table = decode_table[i][1].clone();
                    let mut partial_adj_table = Vec::new();
                    for j in 0..start_adj_table.len()/3 {
                        if !pattern_table_vertex.contains(&(start_adj_table[j*3+1])) {
                            continue;
                        }
                        if !partial_added_vtx.contains(&(start_adj_table[j*3+1])) {
                            continue;
                        }
                        partial_adj_table.push(start_adj_table[3*j]);
                        partial_adj_table.push(start_adj_table[3*j+1]);
                        partial_adj_table.push(start_adj_table[3*j+2]);
                    }
                    partial_decode_table.push(vec![decode_table[i][0].clone(),partial_adj_table.clone()]);
                    partial_added_vtx.push(decode_table[i][0][0]);
                }
                let local_code = _pattern_generate_2side(partial_decode_table.clone());
                println!("decode_table {:?}",partial_decode_table);
                println!("local code {:?}", local_code);
                for i in pattern_table.keys() {
                    println!("key: {:?}, value: {:?}",i,pattern_table[i]);
                }
                estimate_result = pattern_table[&local_code] as f64;


                for a in depth as usize..path.len() {
                    // firstly get cardinality of pattern of length depth
                    // during calculating, the function get_adj_inpath will give set of adjs,
                    // 1. if it NOT generate cycle |i.e. it has only one adj in visited,
                            //  and previous node has only one other edge A, then calculate |A,B|/|A|
                    // 2. if it NOT generate cycle |i.e. it has only one adj in visited,
                            //  and previous node has several edges A,B,C then calculate |A,B|/|A| where B is max-weight-edge
                    // if it generate loops, |i.e. it has two visited edges in adj_list, 
                    let extend_gid = path[a];
                    // judge if it has cycle
                    let mut adj_list: Vec<u64> = Vec::new();
                    let mut adj_list_edge: Vec<u64> = Vec::new();
                    let extend_copy = decode_res[extend_gid as usize].clone();
                    let extend_adj = extend_copy[1].clone();
                    for idx in 0..extend_adj.len()/3 {
                        for j in 0..a{
                            if extend_adj[idx*3+1]==path[j] as u64 {
                                adj_list.push(extend_adj[idx*3+1]);
                                adj_list_edge.push(extend_adj[idx*3]);
                            }
                        }
                    }
                    if adj_list.len() >= 2 {
                        // calculate close possibility of all loop (it can just use)
                        
                        let decode_table = decode_res.clone();
                        let mut partial_decode_table = Vec::new();
                        let mut extend_partial_adj = Vec::new();
                        for j in adj_list.clone() {
                            partial_decode_table.push(vec![decode_table[j as usize][0].clone(), vec![]]);
                        }
                        for i in 0..extend_adj.len()/3{
                            if !adj_list.contains(&extend_adj[i*3+1]) {
                                continue;
                            }
                            extend_partial_adj.push(extend_adj[i*3]);
                            extend_partial_adj.push(extend_adj[i*3+1]);
                            extend_partial_adj.push(extend_adj[i*3+2]);
                        }
                        partial_decode_table.push(vec![decode_table[extend_gid as usize][0].clone(), extend_partial_adj]);
                        let local_code = _pattern_generate_2side(partial_decode_table);
                        let mut str_info: Vec<String> = Vec::new();
                        for i in local_code.split("==") {
                            str_info.push(i.to_string());
                        }
                        // CEG estimation for single pattern and no loop (i.e. return u64)
                        let temp_estimation = _pattern_estimation_CEG_vertex_version(graphdb, vec![str_info], pattern_table2.clone(), aggregator, depth);
                        let closing_pattern_num = temp_estimation[&local_code];
                        let mut multiply_extend_degree = 1u64;
                        
                        // next, get all 
                        let mut partial_decode_table = Vec::new();
                        for j in adj_list.clone() {
                            for k in 0..extend_adj.clone().len()/3 {
                                if k as u64*3+1 == j {
                                    partial_decode_table.push(vec![decode_table[j as usize][0].clone(), vec![]]);
                                    partial_decode_table.push(vec![decode_table[extend_gid as usize][0].clone(), vec![extend_adj[k*3], extend_adj[k*3+1], extend_adj[k*3+2]]]);
                                    let edge_encoding = _pattern_generate_2side(partial_decode_table.clone());
                                    multiply_extend_degree = multiply_extend_degree * pattern_table[&edge_encoding];
                                }
                            }
                        }
                        estimate_result = estimate_result * closing_pattern_num as f64 / multiply_extend_degree as f64;
                    }
                    else if adj_list.len() <1 {
                        println!("find previous node in path error!");
                        continue;
                    }
                    // even when there are multiple adjs, calculate all adj_list
                    // TODO, change to find all info in pattern table rather than use graphdb
                    for i in 0..adj_list.len() {
                        // NOT loop case
                        assert_eq!(adj_list.len(),1);
                        let previous_node = adj_list[i];
                        let current_edge = adj_list_edge[i];
                        let previous_copy = decode_res[previous_node as usize].clone();
                        let previous_adj = previous_copy[1].clone();
                        let mut local_max_edge: u64 = 0;
                        let mut local_max_weight: u64 = 1;
                        for idx in 0..previous_adj.len()/3 {
                            if previous_adj[idx*3+1] != extend_gid {
                                let weight_count = graphdb.count_all_edges(Some(&vec![previous_adj[idx*3] as u8])) as u64;
                                if weight_count >= local_max_weight {
                                    local_max_weight = weight_count;
                                    local_max_edge = previous_adj[idx*2];
                                }
                            }
                        }
                        // finish selected max-weight-path
                        //write a flatmap to get (local_edge---flat_map----current_edge / local_edge)
                        let prev_node_with_prev_edge = graphdb.get_all_edges(Some(&vec![local_max_edge as u8]));
                        // TODO: change to find number in the pattern table
                        let prev_count = graphdb.get_all_edges(Some(&vec![local_max_edge as u8])).count();
                        let mut count: u64 =0;
                        for i in prev_node_with_prev_edge {
                            if graphdb.get_vertex(i.get_dst_id()).unwrap().get_label()[0] == decode_res[previous_node as usize][0][1] as u8 {
                                count = count + graphdb.get_both_edges(i.get_dst_id(), Some(&vec![current_edge as u8])).count() as u64;
                            }
                            else {
                                count = count + graphdb.get_both_edges(i.get_other_id(), Some(&vec![current_edge as u8])).count() as u64;
                            }
                        }
                        estimate_result = estimate_result * count as f64 / prev_count as f64;
                    }
                    

                }
                // here has generated a result for current path
                estimations.push(estimate_result as u64);
            }
            // aggregate it
            // avg, max, min ...
            let mut mid= 0u64;
            let res_len = estimations.len();
            if aggregator == 0 {
                for i in estimations.clone() {
                    mid = mid + i;
                }
                estimate_result = mid as f64 / (res_len as f64);
            }
            if aggregator == 1 {
                for i in estimations.clone() {
                    if i > mid {
                        mid =i
                    }
                }
                estimate_result = mid as f64;
            }
            if aggregator ==2 {
                for i in estimations.clone() {
                    if i < mid {
                        mid =i
                    }
                }
                estimate_result = mid as f64;
            }
        }
        let mut local_code:String = String::from("");
        let mut list = i.clone();
        list.sort();
        for m in list{
            local_code = local_code + &m;
            local_code = local_code + "_";
        }
        catalog.insert(local_code, estimate_result as u64);
    }
    catalog
}

// authorize
fn _pattern_generate(src: Vec<Vec<Vec<u64>>>) -> String {

    // idx: each vertex -> [[gid, label], [adjlist]]
    // adjlist: Vec![edge label, dst_gid..]
    let mut mut_graph: MutableGraphDB<DefaultId, InternalId> = GraphDBConfig::default().new();

    let mut vertexsrc :Vec<u64> = Vec::new();
    for i in src.clone() {
        
        let label = [i[0][1] as u8,0];
        vertexsrc.push(i[0][0]);
        mut_graph.add_vertex(i[0][0] as usize, label);
    }
    for i in src.clone() {
        let adj_list = i[1].clone();
        let src_id = i[0][0];
        for j in 0..adj_list.len()/2 {
            mut_graph.add_edge(src_id as usize, adj_list[2*j+1] as usize, adj_list[2*j] as u8);
        }
    }
    let schema_file = "data/schema.json";
    let schema =
            LDBCGraphSchema::from_json_file(schema_file).expect("Read graph schema error!");
    // println!("997 {:?}",vertexsrc);
    let result = _update_catalog_loca(vertexsrc, &mut_graph.into_graph(schema), None, None);
    result

}


fn _pattern_generate_2side(src: Vec<Vec<Vec<u64>>>) -> String {

    // idx: each vertex -> [[gid, label], [adjlist]]
    // adjlist: Vec![edge label, dst_gid..]
    let mut mut_graph: MutableGraphDB<DefaultId, InternalId> = GraphDBConfig::default().new();

    let mut vertexsrc :Vec<u64> = Vec::new();
    for i in src.clone() {
        
        let label = [i[0][1] as u8,0];
        vertexsrc.push(i[0][0]);
        mut_graph.add_vertex(i[0][0] as usize, label);
    }
    for i in src.clone() {
        let adj_list = i[1].clone();
        let src_id = i[0][0];
        for j in 0..adj_list.len()/3 {
            if adj_list[3*j+2]==0 {
                mut_graph.add_edge(src_id as usize, adj_list[3*j+1] as usize, adj_list[3*j] as u8);
            }
            else { 
            mut_graph.add_edge(adj_list[3*j+1] as usize,src_id as usize,  adj_list[3*j] as u8);
            }
        }
    }
    let schema_file = "data/schema.json";
    let schema =
            LDBCGraphSchema::from_json_file(schema_file).expect("Read graph schema error!");

    let result = _update_catalog_loca(vertexsrc, &mut_graph.into_graph(schema), None, None);
    result

}


fn _decode_pattern(pattern: Vec<String>) ->Vec<Vec<Vec<u64>>> {
    let mut occur_set: HashMap<(u64, u64),u64> = HashMap::new();
    let mut gids: Vec<Vec<Vec<u64>>> = Vec::new();
    for i in pattern.clone() {
        if i.len()==0{
            continue;
        }
        let v: Vec<&str> = i.split("_").collect();
        let v1: (u64,u64) = (v[0].parse().unwrap(),v[4].parse().unwrap());
        let v2: (u64,u64) = (v[1].parse().unwrap(),v[5].parse().unwrap());
        if !occur_set.contains_key(&v1) {
            gids.push(vec![vec![occur_set.len() as u64, v[0].parse().unwrap()],vec![]]);
            occur_set.insert(v1, occur_set.len() as u64);
        }
        if !occur_set.contains_key(&v2) {
            gids.push(vec![vec![occur_set.len() as u64, v[1].parse().unwrap()],vec![]]);
            occur_set.insert(v2, occur_set.len() as u64);
        }
    }
    
    for i in pattern.clone() {
        if i.len()==0{
            continue;
        }
        let v: Vec<&str> = i.split("_").collect();
        let v1: (u64,u64) = (v[0].parse().unwrap(),v[4].parse().unwrap());
        let v2: (u64,u64) = (v[1].parse().unwrap(),v[5].parse().unwrap());
        let edgelabel:u8 = v[2].parse().unwrap();


        let idx =occur_set[&v1];
        gids[idx as usize][1].push(edgelabel as u64);
        gids[idx as usize][1].push(occur_set[&v2]);
        gids[idx as usize][1].push(0);

        let idx =occur_set[&v2];
        gids[idx as usize][1].push(edgelabel as u64);
        gids[idx as usize][1].push(occur_set[&v1]);
        gids[idx as usize][1].push(1);
    }
    gids
}


fn _get_pattern_gid(pattern: Vec<String>) ->HashMap<(u64, u64), u64> {
    let mut occur_set: HashMap<(u64, u64),u64> = HashMap::new();
    let mut gid_table: HashMap<u64, (u64, u64)> = HashMap::new();
    let mut gids: Vec<Vec<Vec<u64>>> = Vec::new();
    for i in pattern.clone() {
        let v: Vec<&str> = i.split("_").collect();
        let v1: (u64,u64) = (v[0].parse().unwrap(),v[4].parse().unwrap());
        let v2: (u64,u64) = (v[1].parse().unwrap(),v[5].parse().unwrap());
        if !occur_set.contains_key(&v1) {
            occur_set.insert(v1, occur_set.len() as u64);
            gid_table.insert(occur_set.len() as u64, v1);
            gids.push(vec![vec![occur_set.len() as u64, v[0].parse().unwrap()],vec![]]);
        }
        if !occur_set.contains_key(&v2) {
            occur_set.insert(v2, occur_set.len() as u64);
            gid_table.insert(occur_set.len() as u64, v2);
            gids.push(vec![vec![occur_set.len() as u64, v[1].parse().unwrap()],vec![]]);
        }
    }
    occur_set
}

/// giving vertex index in hashmap
fn _indexing_vertex(set: Vec<u64>, graphdb: &LargeGraphDB) -> HashMap<u64,u8> {
    let mut vec_set = set.clone();
    let mut index_map: HashMap<u64,u8> = HashMap::new();
    let mut occur_set: HashSet<u64> = HashSet::new();
    while vec_set.len() != 0 {
        let mut index_sort: Vec<u64> = Vec::new();
        if occur_set.contains(&vec_set[0]){
            vec_set.remove(0);
            continue;
        }
        let v1 = vec_set[0];
        occur_set.insert(v1);
        index_sort.push(v1);
        vec_set.remove(0);
        for i in vec_set.clone(){
            if graphdb.get_vertex(i as usize).unwrap().get_label()[0] == graphdb.get_vertex(v1 as usize).unwrap().get_label()[0] && (v1 != i) {
                occur_set.insert(i);
                index_sort.push(i);
            }
        }
        for i in 0..index_sort.len()-1 {
            for j in 0..index_sort.len() {
                if !_cmp(index_sort[i],index_sort[i+1], graphdb) {
                    let mid = index_sort[i];
                    index_sort[i] = index_sort[i+1];
                    index_sort[i+1] = mid;
                }
            }
        }
        for i in 0..index_sort.len() {
            index_map.insert(index_sort[i], i as u8);
        }
    }
    index_map
}

// given set of vertices, return edge set
fn _get_edges(vec_id: HashSet<u64>, graphdb: &LargeGraphDB) -> Vec<(usize,usize)> {
    let mut edge_set:  Vec<(usize,usize)> = Vec::new();
    let cloneset = vec_id.clone();
    for i in cloneset.into_iter() {
        let edges = graphdb.get_adj_edges(i as usize, None, Direction::Outgoing);
        for j in edges{
            if vec_id.contains(&(j.get_dst_id() as u64)) && vec_id.contains(&(j.get_src_id() as u64)) {
                edge_set.push(j.get_edge_id());
            }
        }
    }
    edge_set
}

/// compare vertex
fn _cmp(v1: u64, v2: u64, graphdb: &LargeGraphDB) -> bool {
    let out1 =  graphdb.get_adj_vertices(v1 as usize, None, Direction::Outgoing).count();
    let out2 =  graphdb.get_adj_vertices(v2 as usize, None, Direction::Outgoing).count();
    if out1 > out2 {
        return true;
    }
    else if out2 > out1{
        return false;
    }
    else {
        let mut outv1: Vec<u64> = graphdb.get_adj_vertices(v1 as usize, None, Direction::Outgoing).map(|x| x.get_label()[0] as u64).collect();
        let mut outv2: Vec<u64> = graphdb.get_adj_vertices(v2 as usize, None, Direction::Outgoing).map(|x| x.get_label()[0] as u64).collect();
        outv1.sort();
        outv2.sort();
        for i in 0..outv1.len() {
            if outv1[i] > outv2[i] {
                return true;
            }
            if outv1[i] < outv2[i] {
                return false;
            }
        }
        
        let in1 =  graphdb.get_adj_vertices(v1 as usize, None, Direction::Incoming).count();
        let in2 =  graphdb.get_adj_vertices(v2 as usize, None, Direction::Incoming).count();
        if in1 > in2 {
            return true;
        }
        else if in2 > in1 {
            return false;
        }
        else {
            let mut inv1: Vec<u64> = graphdb.get_adj_vertices(v1 as usize, None, Direction::Incoming).map(|x| x.get_label()[0] as u64).collect();
            let mut inv2: Vec<u64> = graphdb.get_adj_vertices(v2 as usize, None, Direction::Incoming).map(|x| x.get_label()[0] as u64).collect();
            inv1.sort();
            inv2.sort();
            for i in 0..inv1.len() {
                if inv1[i] > inv2[i] {
                    return true;
                }
                if inv1[i] < inv2[i] {
                    return false;
                }
            }
            let label1 = graphdb.get_vertex(v1 as usize).unwrap().get_label()[0] as u64;
            let label2 = graphdb.get_vertex(v2 as usize).unwrap().get_label()[0] as u64;
            if label1 > label2 {
                return true;
            }
            else if label2 > label1 {
                return false;
            }
            else {
                if v1 > v2 {
                    return true;
                }
                else {
                    return false;
                }
            }
        }
    }
}

/// return a path
// authorize
fn _update_catalog_loca(src: Vec<u64>, graphdb: &LargeGraphDB, edge_info: Option<Vec<(usize,usize)>>, edge_label_info: Option<Vec<u8>>) -> String {
    let mut vertex_set = HashSet::new();
    let cloneset = src.clone();
    let mut edge_label = Vec::new();
    let indexing_map = _indexing_vertex(src, graphdb);
    let mut edge_set_given = false;
    for i in cloneset {
        vertex_set.insert(i);
    }
    let mut edgeset= Vec::new();
    if edge_info != None {
        edgeset = edge_info.unwrap();
        edge_label = edge_label_info.unwrap();
        edge_set_given = true;
    }
    else {
        edgeset = _get_edges(vertex_set, graphdb);
    }
    let code_len = edgeset.len();
    let mut total_code = String::from("");
    let mut codes: Vec<String> = Vec::new();
    // println!(" all id {:?}", indexing_map);
    for i in 0..edgeset.len() {
        let mut label = 0;
        let mut src_id = edgeset[i].0;
        let mut dst_id = 0;
        if edge_set_given {
            dst_id = edgeset[i].1;
            label = edge_label[i];
        }
        else {
            dst_id = graphdb.get_edge(edgeset[i]).unwrap().get_other_id();
            label = graphdb.get_edge(edgeset[i]).unwrap().get_label();
        }
        // println!("dst_id {:?}",dst_id);
        let idx1 = indexing_map[&(src_id as u64)];
        let idx2 = indexing_map[&(dst_id as u64)];
        let label1 = graphdb.get_vertex(src_id).unwrap().get_label();
        let label2 = graphdb.get_vertex(dst_id).unwrap().get_label();
        let mut dir: u8 = 0;
        // TODO: add label[1]
        let local_code = label1[0].to_string() + "_" + &label2[0].to_string() + "_" + &label.to_string() + "_" + &(dir.to_string()) + "_" + &(idx1.to_string()) + "_" + &(idx2.to_string());
        codes.push(local_code);
    }
    codes.sort();
    for i in codes{
        total_code += "==";
        total_code += &i;
    }

    total_code
}


fn _update_catalog(src: Vec<u64>, graphdb: &LargeGraphDB) -> String {
    let mut vertex_set = HashSet::new();
    let cloneset = src.clone();
    let indexing_map = _indexing_vertex(src, graphdb);
    for i in cloneset {
        vertex_set.insert(i);
    }
    let edgeset = _get_edges(vertex_set, graphdb);

    let code_len = edgeset.len();
    let mut total_code = String::from("");
    let mut codes: Vec<String> = Vec::new();
    for i in edgeset {
        let label = graphdb.get_edge(i).unwrap().get_label();
        let dst_id = graphdb.get_edge(i).unwrap().get_other_id();
        let idx1 = indexing_map[&(i.0 as u64)];
        let idx2 = indexing_map[&(dst_id as u64)];
        let label1 = GRAPH.get_vertex(i.0).unwrap().get_label();
        let label2 = GRAPH.get_vertex(dst_id).unwrap().get_label();
        let mut dir: u8 = 0;
        if label1 != label2{
            dir = 0;
        }
        else {
            if idx1 > idx2 {
                dir = 1;
            }
        }
        let local_code = label1[0].to_string() + "_" + &label2[0].to_string() + "_" + &label.to_string() + "_" + &(dir.to_string()) + "_" + &(idx1.to_string()) + "_" + &(idx2.to_string());
        codes.push(local_code);
    }
    codes.sort();
    for i in codes{
        if total_code.len()!=0{
            total_code += "==";
        }
        total_code += &i;
    }

    total_code
}


fn _pattern_counting(graphdb: &LargeGraphDB, min_pattern_size: u64, max_pattern_size: u64, top_frequent_pattern: u64, vertex_set: HashSet<u64>, sample_method: u64) -> HashMap<std::string::String, u64> {
    
    let mut catalog = HashMap::new();
    let mut occur_count = HashSet::new();
    let mut vertex_occur_count = HashSet::new();
    let mut to_mining = VecDeque::new();
    println!("test sampled length  {:?}", vertex_set.len());

    let mut select_iter = vertex_set.iter();
    // for test
    let mut appear_max_len =0;

    let filename: String;
    if sample_method==0 {
        filename= String::from("text1.txt");
    }
    else {
        filename= String::from("text2.txt");
    }
    let mut f = std::fs::File::create(filename).unwrap();


    while !to_mining.is_empty() || vertex_occur_count.len()<vertex_set.len(){
        // println!("Get started");
        let select_option = select_iter.next();
        if select_option.is_none() {
            break;
        }
        let select_id :&u64 = select_option.unwrap();
        if vertex_occur_count.contains(select_id) {
            continue;
        }
        to_mining.push_back(vec![*select_id]);
        vertex_occur_count.insert(*select_id);

        // f.write("\n".as_bytes());
        // f.write(select_id.to_string().as_bytes());
        

        while !to_mining.is_empty() {
            // println!("Minning queue size = {:?}", to_mining.len() as u64);
            let path = to_mining.pop_front().unwrap();
            if path.len()as u64 > appear_max_len {
                appear_max_len = path.len()as u64;
            }
            if path.len() as u64 > max_pattern_size {
                continue;
            }
            // println!("Path {:?}",path);
            if path.len()>min_pattern_size as usize {
                let c_path = path.clone();
                let cc_path = path.clone();
                // println!("=================update catalog====================");
                let cata_index =_update_catalog(c_path, graphdb);
                f.write("\n".as_bytes());
                for i in cc_path.clone() {
                    f.write(i.to_string().as_bytes());
                    f.write(", ".as_bytes());
                }
                f.write(cc_path.len().to_string().as_bytes());
                if !catalog.contains_key(&cata_index) {
                    catalog.insert(cata_index, 1);
                }
                else {
                    let key = cata_index;
                    let value = catalog[&key] as u64 +1;
                    catalog.remove(&key);
                    catalog.insert(key, value);
                }
            }
            // let mut result = _sampling_adjvertex(confadj, &path).expect("Run Job Error!");
            for component_vtx in path.clone() {
                let mut result = graphdb.get_both_vertices(component_vtx as usize, None);
                // get extend vertex into to_mining
                while let Some(data) = result.next() {
                    
                    // println!("Get adj vertex");
                    let sample_id: u64 = data.get_id() as u64;
                    let mut sample_extend = path.clone();
                    if !sample_extend.contains(&sample_id){
                        sample_extend.push(sample_id);
                    }
                    if graphdb.get_vertex(sample_id as usize).is_none() {
                        continue;
                    }
                    sample_extend.sort();
                    if (path.len() as u64 <= max_pattern_size) && !occur_count.contains(&sample_extend) {
                        let mut clone_path = path.clone();
                        if !clone_path.contains(&sample_id){
                            clone_path.push(sample_id);
                        }
                        let fs_record = clone_path.clone();
                        let path_len: u64 = clone_path.len() as u64;
                        if path_len <= max_pattern_size{
                            clone_path.sort();
                            let clone_path2 = clone_path.clone();
                            to_mining.push_back(clone_path);
                            occur_count.insert(clone_path2);

                        }
                    }
                    
                    if !vertex_occur_count.contains(&sample_id) {
                        to_mining.push_back(vec![sample_id]);
                        vertex_occur_count.insert(sample_id);
                    }
                }
            }
            
        f.write("\n   == ".as_bytes());
        f.write(vertex_occur_count.len().to_string().as_bytes());
        if vertex_occur_count.len() > vertex_set.len() {
            break;
        }
        }
    }
    
    println!("Length {:?}",appear_max_len);
    let statistic_catalog = catalog.clone();

    let pattern_kinds = statistic_catalog.len() as u64;
    println!("Pattern type number is {:?}   pattern number is {:?}", pattern_kinds, occur_count.len());
    
    println!("catalog :\n {:?} \n",statistic_catalog);

    statistic_catalog
    
    
}





    // // Exp1
    // f.write("\n\n".as_bytes()).unwrap();
    // f.write("   Experiment 1 ".as_bytes()).unwrap();
    // f.write("\n\n".as_bytes()).unwrap();
    // let para_1_exp1 = vec![5];
    // let para_2_exp1 = vec![5];
    // let para_3_exp1 = vec![2];
    // let para_4_exp1 = vec![3];
    
    
    // for l in para_4_exp1.clone() {
    //     for k in para_3_exp1.clone() {
    //         let start_full = Instant::now();
    //         println!("======================================================================");
    //         println!("              Sample ALL                ");
    //         println!("======================================================================");
    //         println!("rate: {:?},  area: {:?},  min_size: {:?},  max_size: {:?}", 100, 0, &k, &l);
    //         let mut cmp_result = _sampling_arrange(1, 10, k, l, 15, 0);
    //         let end_full = Instant::now();
    //         println!("time cost: {:?}",end_full.duration_since(start_full));
    //         for j in para_2_exp1.clone() {
                
    //             f.write("\n\n".as_bytes()).unwrap();
    //             f.write("area: ".as_bytes()).unwrap();
    //             f.write(j.to_string().as_bytes()).unwrap();
    //             f.write("   min_size: ".as_bytes()).unwrap();
    //             f.write(k.to_string().as_bytes()).unwrap();


    //                 for i in para_1_exp1.clone() {
    //                     let sample_method = 1u64;
    //                     let start_time = Instant::now();
    //                     println!("======================================================================");
    //                     println!("                              ");
    //                     println!("======================================================================");
    //                     println!("rate: {:?},  area: {:?},  min_size: {:?},  max_size: {:?}", &i, &j, &k, &l);
    //                     let result = _sampling_arrange(i, j, k, l, l, sample_method);
    //                     let end_time = Instant::now();
    //                     f.write("\n\n".as_bytes()).unwrap();
    //                     f.write("   sample rate: ".as_bytes()).unwrap();
    //                     f.write(i.to_string().as_bytes()).unwrap();
    //                     for q in result{
    //                         f.write("\n".as_bytes()).unwrap();
    //                         f.write(q.0.to_string().as_bytes());
    //                         f.write(": ".as_bytes()).unwrap();
    //                         f.write(q.1.to_string().as_bytes());
    //                     }
    //                     println!("time cost: {:?}",end_time.duration_since(start_time));
    //                 }
                
                
    //         }
    //     }
    // }

    // // Exp2
    // f.write("\n\n".as_bytes()).unwrap();
    // f.write("   Experiment 2 ".as_bytes()).unwrap();
    // f.write("\n\n".as_bytes()).unwrap();
    // let para_1_exp2 = vec![10];
    // let para_2_exp2 = vec![10];
    // let para_3_exp2 = vec![2];
    // let para_4_exp2 = vec![4, 5];
    // for l in para_4_exp2.clone() {
    //     for k in para_3_exp2.clone() {
    //         for j in para_2_exp2.clone() {
                
    //             f.write("\n\n".as_bytes()).unwrap();
    //             f.write("area: ".as_bytes()).unwrap();
    //             f.write(j.to_string().as_bytes()).unwrap();
    //             f.write("   min_size: ".as_bytes()).unwrap();
    //             f.write(k.to_string().as_bytes()).unwrap();


    //                 for i in para_1_exp2.clone() {
    //                     let sample_method = 1u64;
    //                     let start_time = Instant::now();
    //                     println!("======================================================================");
    //                     println!("                              ");
    //                     println!("======================================================================");
    //                     println!("rate: {:?},  area: {:?},  min_size: {:?},  max_size: {:?}", &i, &j, &k, &l);
    //                     let result = _sampling_arrange(i, j, k, l, l, sample_method);
    //                     let end_time = Instant::now();
    //                     f.write("\n\n".as_bytes()).unwrap();
    //                     f.write("   sample rate: ".as_bytes()).unwrap();
    //                     f.write(i.to_string().as_bytes()).unwrap();
    //                     for q in result{
    //                         f.write("\n".as_bytes()).unwrap();
    //                         f.write(q.0.to_string().as_bytes());
    //                         f.write(": ".as_bytes()).unwrap();
    //                         f.write(q.1.to_string().as_bytes());
    //                     }
    //                     println!("time cost: {:?}",end_time.duration_since(start_time));
    //                 }
                
                
    //         }
    //     }
    // }
    
    // // Exp3
    // f.write("\n\n".as_bytes()).unwrap();
    // f.write("   Experiment 3 ".as_bytes()).unwrap();
    // f.write("\n\n".as_bytes()).unwrap();
    // let para_1_exp3 = vec![10, 15];
    // let para_2_exp3 = vec![10, 20, 40];
    // let para_3_exp3 = vec![2];
    // let para_4_exp3 = vec![3];
    // for l in para_4_exp3.clone() {
    //     for k in para_3_exp3.clone() {
    //         for j in para_2_exp3.clone() {
                
    //             f.write("\n\n".as_bytes()).unwrap();
    //             f.write("area: ".as_bytes()).unwrap();
    //             f.write(j.to_string().as_bytes()).unwrap();
    //             f.write("   min_size: ".as_bytes()).unwrap();
    //             f.write(k.to_string().as_bytes()).unwrap();


    //                 for i in para_1_exp3.clone() {
    //                     let sample_method = 1u64;
    //                     let start_time = Instant::now();
    //                     println!("======================================================================");
    //                     println!("                              ");
    //                     println!("======================================================================");
    //                     println!("rate: {:?},  area: {:?},  min_size: {:?},  max_size: {:?}", &i, &j, &k, &l);
    //                     let result = _sampling_arrange(i, j, k, l, l, sample_method);
    //                     let end_time = Instant::now();
    //                     f.write("\n\n".as_bytes()).unwrap();
    //                     f.write("   sample rate: ".as_bytes()).unwrap();
    //                     f.write(i.to_string().as_bytes()).unwrap();
    //                     for q in result{
    //                         f.write("\n".as_bytes()).unwrap();
    //                         f.write(q.0.to_string().as_bytes());
    //                         f.write(": ".as_bytes()).unwrap();
    //                         f.write(q.1.to_string().as_bytes());
    //                     }
    //                     println!("time cost: {:?}",end_time.duration_since(start_time));
    //                 }
                
                
    //         }
    //     }
    // }