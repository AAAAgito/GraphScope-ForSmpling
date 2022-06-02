use graph_store::config::{GraphDBConfig, JsonConf};
use graph_store::graph_db::GlobalStoreTrait;
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
use std::vec;
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

    let para_1 = vec![5, 10u64, 20, 30, 40, 50, 60];
    let para_2 = vec![70, 60, 50, 40, 30, 20, 10];
    let para_3 = vec![2];
    let para_4 = vec![3];
    let mut result: Vec<Vec<Vec<u128>>> =vec![vec![vec![]]];
    
    result.pop();
    let mut f = std::fs::File::create("data.txt").unwrap();
    _sampling_degree_distribution();
    
    for l in para_4.clone() {
        for k in para_3.clone() {
            let start_full = Instant::now();
            println!("======================================================================");
            println!("              Sample ALL                ");
            println!("======================================================================");
            println!("rate: {:?},  area: {:?},  min_size: {:?},  max_size: {:?}", 100, 0, &k, &l);
            let cmp_result = _sampling_arrange(100, 0, k, l, 15, 0);
            let end_full = Instant::now();
            let result = _sampling_arrange(100, 0, k, l, 15, 1);
            if result.eq(&cmp_result) {
                println!("Not change");
            } else {
                println!("change");
            }
            println!("time cost: {:?}",end_full.duration_since(start_full));
            // for j in para_2.clone() {
                
            //     f.write("\n\n".as_bytes()).unwrap();
            //     f.write("area: ".as_bytes()).unwrap();
            //     f.write(j.to_string().as_bytes()).unwrap();
            //     f.write("   min_size: ".as_bytes()).unwrap();
            //     f.write(k.to_string().as_bytes()).unwrap();


            //     for times in 0..5 {
            //         for i in para_1.clone() {
            //             let sample_method = 1u64;
            //             let start_time = Instant::now();
            //             println!("======================================================================");
            //             println!("                              ");
            //             println!("======================================================================");
            //             println!("rate: {:?},  area: {:?},  min_size: {:?},  max_size: {:?}", &i, &j, &k, &l);
            //             result.push( _sampling_arrange(i, j, k, l, l, sample_method));
            //             let end_time = Instant::now();
            //             println!("time cost: {:?}",end_time.duration_since(start_time));
            //         }
            //         let mut accurate_list: Vec<f64> = vec![];
            //         let ans: &Vec<Vec<u128>> = &cmp_result.clone();
            //         for z in 0..para_1.len() {
            //             let cur: &Vec<Vec<u128>> = &result[z];
            //             let total = ans.len() as f64;
            //             let mut correct = 0f64;
            //             for j in cur {
            //                 if ans.contains(j) {
            //                     correct = correct +1.0;
            //                 }
            //             }
            //             let acc: f64 = correct/total;
            //             accurate_list.push(acc);
            //         }
            //         f.write("\n".as_bytes()).unwrap();
            //         for i in accurate_list {
            //             f.write(i.to_string().as_bytes()).unwrap();
            //             f.write(" | ".as_bytes()).unwrap();
            //             println!("Accurate: {:?}", i);
            //         }
            //         result.clear();
            //     }
                
            // }
        }
    }
    
    pegasus::shutdown_all();
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

fn _sampling_start_vertex_alpha(conf: JobConf, label: u8, area_num: u64) -> Result<ResultStream<u64>, JobSubmitError> {
    let mut rng = rand::thread_rng();
    let partition = (GRAPH.get_all_vertices(None).count() as u64)/area_num as u64;
    // println!("Graph Size = {:?}", GRAPH.get_all_vertices(None).count() as u64);
    assert_eq!(partition>0,true);
    let select_id = rng.gen_range(0, partition);
    pegasus::run(conf, move || {
        move |input, output| {
            input.input_from(GRAPH.get_all_vertices(Some(&vec![label])).map(|v| (v.get_id() as u64)))?
            .filter(move |v_id| {
                let adj_num = GRAPH.get_both_vertices(*v_id as usize, None).count();
                Ok(adj_num != 0 && (v_id%partition) == select_id)
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

fn _sampling_degree_distribution() {
    for i in 0..20u8 {
        let conf = JobConf::new("distribution");
        let mut result = _sampling_label(conf, i).expect("Run Job Error!");
        let mut count = 0u64;
        let mut total_degree = 0u64;
        while let Some(Ok(data)) = result.next() {
            count += 1;
            total_degree += GRAPH.get_both_vertices(data as usize, None).count() as u64;
        }
        if count >0 {
            println!("label {:?}:   count: {:?}   deg: {:?}", i, count, total_degree/count);
        }
    }
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

fn _sampling_outvertex(conf: JobConf, src: &Vec<u64>) -> Result<ResultStream<u64>, JobSubmitError> {
    pegasus::run(conf, move || {
        let src = src.clone();
        move |input, output| {
            input.input_from(src.into_iter())?
            .flat_map(|v_id| {
                let adj_vertices = GRAPH.get_adj_vertices(v_id as usize, None, Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    v.get_id() as u64
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

// not used
fn _sampling_adjvertex_mhrw(conf: JobConf, src: &Vec<u64>) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        let src = src.clone();
        move |input, output| {
            input.input_from(src.into_iter())?
            .flat_map(|v_id| {
                let adj_vertices = GRAPH.get_both_vertices(v_id as usize, None);
                Ok(adj_vertices.map(move |v| {
                    let mut rng = rand::thread_rng();
                    let rnd = rng.gen_range(0, 1);
                    let degree_src = GRAPH.get_both_vertices(v_id as usize, None).count();
                    let degree_dst = GRAPH.get_both_vertices(v.get_id() as usize, None).count();
                    let mut path = vec![];
                    if rnd <= degree_src/degree_dst {
                        path.push(v_id);
                        path.push(v.get_id() as u64);

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
fn _sampling_arrange(sample_rate: u64, area_num: u64, min_pattern_size: u64, max_pattern_size: u64, top_frequent_pattern: u64, sample_method: u64) -> Vec<Vec<u128>>{

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
        let sample_num = (GRAPH.get_all_vertices(None).count() as u64)*sample_rate/100;
        // println!("Expected Sample length = {:?}", sample_num as u64);
        let conf1 = JobConf::new("conf1");
        let conf1_2 = JobConf::new("conf1");
        let conf1_3 = JobConf::new("conf1");

        let mut start_vertices = _sampling_start_vertex(conf1, area_num).expect("Run Job Error!");
        // alpha version
        // let mut start_vertices = _sampling_start_vertex_alpha(conf1, 6, 2*area_num/3).expect("Run Job Error!")
        // .chain(_sampling_start_vertex_alpha(conf1_2, 8, area_num/6).expect("Run Job Error!"))
        // .chain(_sampling_start_vertex_alpha(conf1_3, 0, area_num/6).expect("Run Job Error!"));


        let mut buffer = vec![];
    
        
        while let Some(Ok(data)) = start_vertices.next() {
            if !sampled.contains(&data){
                sampled.insert(data);
                let sample_label = GRAPH.get_vertex(data as usize).unwrap().get_label();
                let v1: DefaultId = LDBCVertexParser::to_global_id(data as usize, 0);
                mut_graph.add_vertex(v1, sample_label);
                buffer.push(data);
            }
            // println!("{:?}", data);
        }
        while !sampled.len() as u64 >= sample_num {
    
            let conf2 = JobConf::new("conf2");
            if buffer.len() as u64 ==0 {
                // println!("All vertex getted");
                break;
            }
            let mut result2 = _sampling_adjvertex(conf2, &buffer).expect("Run Job Error!");
            buffer.clear();
            while let Some(Ok(data)) = result2.next() {
                let sample_id: u64 = data[1];
                let src_id: u64 = data[0];
                

                // apply MHRW, not used
                // let mut rng = rand::thread_rng();
                // let rnd = rng.gen_range(0, 1);
                // let degree_src = GRAPH.get_both_vertices(src_id as usize, None).count();
                // let degree_dst = GRAPH.get_both_vertices(sample_id as usize, None).count();
                // if sample_method==1u64 {
                //     if rnd >= degree_src/degree_dst {
                //         continue;
                //     }
                // }

                if !sampled.contains(&sample_id) && !(sampled.len() as u64 >= sample_num){
                    sampled.insert(sample_id);
    
                    let v1: DefaultId = LDBCVertexParser::to_global_id(sample_id as usize, 0);
                    let sample_label = GRAPH.get_vertex(sample_id as usize).unwrap().get_label();
                    mut_graph.add_vertex(v1, sample_label);
                    
                    let v2: DefaultId = LDBCVertexParser::to_global_id(src_id as usize, 0);
                    let src_label = GRAPH.get_vertex(src_id as usize).unwrap().get_label();
                    if !sampled.contains(&src_id) && !(sampled.len() as u64 >= sample_num){
                        mut_graph.add_vertex(v2, src_label);
                    }
                    // println!("src: {:?}, dst: {:?}",src_label,sample_label);
                    
    
                    buffer.push(sample_id);
                }
                // println!("{:?}", data);
            }
        }
    }
    

    
    for i in sampled.clone() {
        // edge add
        let conf3 = JobConf::new("conf3");
        let mut adjout = _sampling_outvertex(conf3, &vec![i]).expect("Run Job Error!");
        while let Some(Ok(data)) = adjout.next() {
            
            let dst_id: u64 = data;
            if sampled.contains(&dst_id) {
                mut_graph.add_edge(i as usize, dst_id as usize, 0u8);
            }
        }

    }

    // println!("Sample length = {:?}", sampled.len() as u64);

    let schema_file = "data/schema.json";
    let schema =
            LDBCGraphSchema::from_json_file(schema_file).expect("Read graph schema error!");
    _pattern_mining(&mut_graph.into_graph(schema), min_pattern_size, max_pattern_size, top_frequent_pattern, sampled, sample_method)
    
}

fn _pattern_mining(graphdb: &LargeGraphDB, min_pattern_size: u64, max_pattern_size: u64, top_frequent_pattern: u64, vertex_set: HashSet<u64>, sample_method: u64) ->Vec<Vec<u128>> {
    
    let mut catalog = HashMap::new();
    let mut occur_count = HashSet::new();
    let mut vertex_occur_count = HashSet::new();
    let mut to_mining = VecDeque::new();
    println!("test sampled length  {:?}", vertex_set.len());

    catalog.insert(vec![0u128,0u128], 0);
    occur_count.insert(vec![0u64]);
    vertex_occur_count.insert(0u64);
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
            println!("this break");
            break;
        }
        let select_id :&u64 = select_option.unwrap();
        if vertex_occur_count.contains(select_id) {
            continue;
        }
        to_mining.push_back(vec![*select_id]);
        occur_count.insert(vec![*select_id]);
        vertex_occur_count.insert(*select_id);
        f.write("\n".as_bytes());
        f.write(select_id.to_string().as_bytes());
        

        while !to_mining.is_empty() {
            // println!("Minning queue size = {:?}", to_mining.len() as u64);
            let confadj = JobConf::new("confadj");
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
                // println!("=================update catalog====================");
                let cata_index =_update_catalog(&c_path, graphdb);
                if !catalog.contains_key(&vec![cata_index[0],cata_index[1]]) {
                    catalog.insert(vec![cata_index[0],cata_index[1]], 1);
                }
                else {
                    let key = vec![cata_index[0],cata_index[1]];
                    let value = catalog[&key] as u128 +1;
                    catalog.remove(&key);
                    catalog.insert(key, value);
                }
            }
            let mut result = _sampling_adjvertex(confadj, &path).expect("Run Job Error!");
            
            // get extend vertex into to_mining
            while let Some(Ok(data)) = result.next() {
                
                // println!("Get adj vertex");
                let sample_id: u64 = data[1];
                let mut sample_extend = path.clone();
                if !sample_extend.contains(&sample_id){
                    sample_extend.push(sample_id);
                }
                if graphdb.get_vertex(sample_id as usize).is_none() {
                    continue;
                }
                sample_extend.sort();
                if (path.len() as u64 <= max_pattern_size-1) && !occur_count.contains(&sample_extend) {
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
                    f.write("\n".as_bytes());
                    for i in fs_record.clone() {
                        f.write(i.to_string().as_bytes());
                        f.write(", ".as_bytes());
                    }
                    f.write(path_len.to_string().as_bytes());
                    f.write("   | ".as_bytes());
                    f.write(to_mining.len().to_string().as_bytes());
                    // input: hashset: occur_count, catalog: HashMap
                    // output: update HashMap
                    
                }
                
                if !occur_count.contains(&vec![sample_id]) {
                    to_mining.push_back(vec![sample_id]);
                    occur_count.insert(vec![sample_id]);
                    vertex_occur_count.insert(sample_id);
                    f.write("\n".as_bytes());
                    f.write(sample_id.to_string().as_bytes());
                }
            }
            // occur_count.clear();
        f.write("\n   == ".as_bytes());
        f.write(vertex_occur_count.len().to_string().as_bytes());
        f.write("   | ".as_bytes());
        f.write(to_mining.len().to_string().as_bytes());
        }
    }
    
    println!("Length {:?}",appear_max_len);
    let statistic_catalog = catalog.clone();

    let mut most_fre_pattern:Vec<Vec<u128>> = vec![vec![]];
    let mut fre_count :Vec<u128> = vec![0];
    let catalog_clone = catalog.clone();
    let mut patterns: Vec<Vec<u128>> = catalog.into_keys().collect();
    let mut pattern_counts: Vec<u128> = catalog_clone.into_values().collect();
    let mut sort_num = top_frequent_pattern;
    if sort_num > pattern_counts.len() as u64-1 {
        sort_num = pattern_counts.len() as u64-1;
    }
    for j in 0..sort_num as usize {
        let mut max_count: u128 = 0;
        let mut max_index: usize = 0;
        if pattern_counts.len()==0 {
            break;
        }
        for i in 0..pattern_counts.len() {
            if pattern_counts[i]>max_count {
                max_count = pattern_counts[i];
                max_index = i;
            }
        }
        most_fre_pattern.push(vec![patterns[max_index][0], patterns[max_index][1]]);
        fre_count.push(pattern_counts[max_index]);
        pattern_counts.remove(max_index);
        patterns.remove(max_index);
    }
    // println!("|||||Message catalog|||||");
    // println!("{:?}",fre_count);
    // println!("catalog: {:?}",statistic_catalog);
    let pattern_kinds = statistic_catalog.len() as u64;
    println!("Pattern type number is {:?}   vertex number is {:?}", pattern_kinds, occur_count.len());
    
    // println!("catalog :\n {:?} \n",statistic_catalog);

    most_fre_pattern
    
    
}

fn _sort_vertex(vec_id: &mut Vec<u64>, vec_in: &mut Vec<u64>, vec_out: &mut Vec<u64>, vec_label: &mut Vec<Label>) {
    for i in (vec_id.len()-1)..1 {
        if vec_in[i] < vec_in[i-1] {
            let mut mid = vec_in[i];
            vec_in[i] = vec_in[i-1];
            vec_in[i-1] = mid;

            mid = vec_out[i];
            vec_out[i] = vec_out[i-1];
            vec_out[i-1] = mid;
            let lmid = vec_label[i];
            vec_label[i] = vec_label[i-1];
            vec_label[i-1] = lmid;
            
            mid = vec_id[i];
            vec_id[i] = vec_id[i-1];
            vec_id[i-1] = mid;
        }
        else if vec_in[i] == vec_in[i-1] && vec_out[i] > vec_out[i-1] {
            let mut mid = vec_in[i];
            vec_in[i] = vec_in[i-1];
            vec_in[i-1] = mid;

            mid = vec_out[i];
            vec_out[i] = vec_out[i-1];
            vec_out[i-1] = mid;

            let lmid = vec_label[i];
            vec_label[i] = vec_label[i-1];
            vec_label[i-1] = lmid;
            
            mid = vec_id[i];
            vec_id[i] = vec_id[i-1];
            vec_id[i-1] = mid;
        }
        else if vec_in[i] == vec_in[i-1] && vec_out[i] == vec_out[i-1] && vec_label[i][0] < vec_label[i-1][0] {
            let mut mid = vec_in[i];
            vec_in[i] = vec_in[i-1];
            vec_in[i-1] = mid;

            mid = vec_out[i];
            vec_out[i] = vec_out[i-1];
            vec_out[i-1] = mid;

            let lmid = vec_label[i];
            vec_label[i] = vec_label[i-1];
            vec_label[i-1] = lmid;
            
            mid = vec_id[i];
            vec_id[i] = vec_id[i-1];
            vec_id[i-1] = mid;
        }
    }
}

// return a path
fn _update_catalog(src: &Vec<u64>, graphdb: &LargeGraphDB) -> Vec<u128> {
    
    let prime_number = vec![2, 3, 5, 7, 11, 13, 17, 19, 23, 29,
        31, 37, 41, 43, 47, 53, 59, 61, 67, 71];
    let mut vertex_indegree = HashMap::new();
    let mut vertex_outdegree = HashMap::new();
    let mut vertex_label = HashMap::new();
    let mut vertex_id = HashMap::new();
    
    vertex_indegree.insert(0, 0);
    vertex_outdegree.insert(0, 0);
    vertex_label.insert(0, [0,0]);

    let mut iter = src.iter();
    while let Some(data) = iter.next() {
        
        // println!("valid at get vertex ");
        let label = graphdb.get_vertex(*data as usize).unwrap().get_label();
        let indegree = graphdb.get_adj_vertices(*data as usize, None, Direction::Incoming).count() as u64;
        let outdegree = graphdb.get_adj_vertices(*data as usize, None, Direction::Outgoing).count() as u64;

        vertex_id.insert(*data as u64, indegree);
        vertex_indegree.insert(*data as u64, indegree);
        vertex_outdegree.insert(*data as u64, outdegree);
        vertex_label.insert(*data as u64, label);

    }
    // sort
    let mut vec_id: Vec<u64> = vertex_id.into_keys().collect();
    let mut vec_in: Vec<u64> = vertex_indegree.into_values().collect();
    let mut vec_out: Vec<u64> = vertex_outdegree.into_values().collect();
    let mut vec_label: Vec<Label> = vertex_label.into_values().collect();

    // println!("vertex info {:?}",vec_label);
    _sort_vertex(&mut vec_id, &mut vec_in, &mut vec_out, &mut vec_label);
    
    let mut string_out = 0u128;
    let mut string_in = 0u128;
    while !vec_id.is_empty() {
        
        let chose_vertex = vec_id[0];
        let mut outvertex = graphdb.get_adj_vertices(chose_vertex as usize, None, Direction::Outgoing);
        let mut invertex = graphdb.get_adj_vertices(chose_vertex as usize, None, Direction::Incoming);
        let mut local_string_out = 1u128;
        let mut local_string_in = 1u128;

        // let test_out: Vec<LocalVertex<usize>> = graphdb.get_adj_vertices(chose_vertex as usize, None, Direction::Outgoing).collect();
        // let test_in: Vec<LocalVertex<usize>> = graphdb.get_adj_vertices(chose_vertex as usize, None, Direction::Incoming).collect();

        // println!("outadj    {:?}", test_out.len());
        // println!(" inadj    {:?}", test_in.len());
        while let Some(data) = outvertex.next() {
            // TODO:fix label problem
            // println!("label1= {:?}", data.get_label()[0] as u64);
            // println!("              label2= {:?}", data.get_label()[1] as u64);
            if data.get_label()[0] as usize +1 > prime_number.len(){
                continue;
            }
            if !vec_id.contains(&(data.get_id() as u64)) {
                continue;
            }
            // println!("Ok at prime");
            let prime: u128 = prime_number[data.get_label()[0] as usize +1];
            local_string_out = local_string_out * prime;
        }
        while let Some(data) = invertex.next() {
            if data.get_label()[0] as usize +1 > prime_number.len(){
                continue;
            }
            if !vec_id.contains(&(data.get_id() as u64)) {
                continue;
            }
            let prime: u128 = prime_number[data.get_label()[0] as usize +1];
            local_string_in = local_string_in * prime;
        }

        string_out = string_out*255 + local_string_out + graphdb.get_vertex(chose_vertex as usize).unwrap().get_label()[0] as u128;
        string_in = string_in*255 + local_string_in + graphdb.get_vertex(chose_vertex as usize).unwrap().get_label()[0] as u128;
        vec_id.pop();
    }
    let result = vec![string_out, string_in];
    result

}



fn _origion_graph_mining(graphdb: &LargeGraphDB, min_pattern_size: u64, max_pattern_size: u64, top_frequent_pattern: u64, vertex_set: HashSet<u64>) ->HashMap<Vec<u128>,u128> {
    
    let mut catalog = HashMap::new();
    let mut occur_count = HashSet::new();
    let mut to_mining = VecDeque::new();
    

    catalog.insert(vec![0u128,0u128], 0);
    occur_count.insert(0 as u64);
    let mut select_iter = vertex_set.iter();
    // for test
    let mut appear_max_len =0;

    while to_mining.is_empty() || occur_count.len()<vertex_set.len(){
        // println!("Get started");
        let select_option = select_iter.next();
        if select_option.is_none() {
            break;
        }
        let select_id :&u64 = select_option.unwrap();
        if occur_count.contains(select_id) {
            continue;
        }
        to_mining.push_back(vec![*select_id]);
        

        while !to_mining.is_empty() {
            // println!("Minning queue size = {:?}", to_mining.len() as u64);
            let confadj = JobConf::new("confadj");
            let path = to_mining.pop_front().unwrap();
            if path.len()as u64 > appear_max_len {
                appear_max_len = path.len()as u64;
            }
            // println!("Path {:?}",path);
            if path.len()>min_pattern_size as usize {
                let c_path = path.clone();
                // println!("=================update catalog====================");
                let cata_index =_update_catalog(&c_path, graphdb);
                if !catalog.contains_key(&vec![cata_index[0],cata_index[1]]) {
                    catalog.insert(vec![cata_index[0],cata_index[1]], 1);
                }
                else {
                    let key = vec![cata_index[0],cata_index[1]];
                    let value = catalog[&key] as u128 +1;
                    catalog.remove(&key);
                    catalog.insert(key, value);
                }
            }
            let mut result = _sampling_adjvertex(confadj, &path).expect("Run Job Error!");
            
            // get extend vertex into to_mining
            while let Some(Ok(data)) = result.next() {
                
                // println!("Get adj vertex");
                let sample_id: u64 = data[1];
                if graphdb.get_vertex(sample_id as usize).is_none() {
                    continue;
                }
                
                if (!path.len() as u64 >= max_pattern_size) && !occur_count.contains(&sample_id) {
                    let mut clone_path = path.clone();
                    clone_path.push(sample_id);
                    to_mining.push_back(clone_path);
                    // input: hashset: occur_count, catalog: HashMap
                    // output: update HashMap
                    
                }
                
                if !occur_count.contains(&sample_id) {
                    to_mining.push_back(vec![sample_id]);
                    occur_count.insert(sample_id);
                }
            }
            // occur_count.clear();
        }
    }
    
    println!("Length {:?}",appear_max_len);
    let statistic_catalog = catalog.clone();

    let mut most_fre_pattern:Vec<Vec<u128>> = vec![vec![]];
    let mut fre_count :Vec<u128> = vec![0];
    let catalog_clone = catalog.clone();
    let mut patterns: Vec<Vec<u128>> = catalog.into_keys().collect();
    let mut pattern_counts: Vec<u128> = catalog_clone.into_values().collect();
    let mut sort_num = top_frequent_pattern;
    if sort_num > pattern_counts.len() as u64-1 {
        sort_num = pattern_counts.len() as u64-1;
    }
    for j in 0..sort_num as usize {
        let mut max_count: u128 = 0;
        let mut max_index: usize = 0;
        if pattern_counts.len()==0 {
            break;
        }
        for i in 0..pattern_counts.len() {
            if pattern_counts[i]>max_count {
                max_count = pattern_counts[i];
                max_index = i;
            }
        }
        most_fre_pattern.push(vec![patterns[max_index][0], patterns[max_index][1]]);
        fre_count.push(pattern_counts[max_index]);
        pattern_counts.remove(max_index);
        patterns.remove(max_index);
    }
    // println!("|||||Message catalog|||||");
    // println!("{:?}",fre_count);
    // println!("catalog: {:?}",statistic_catalog);
    let pattern_kinds = statistic_catalog.len() as u64;
    println!("Pattern type number is {:?}   vertex number is {:?}", pattern_kinds, occur_count.len());

    statistic_catalog.clone()
    
}


fn _sampling_origion(sample_rate: u64, area_num: u64, min_pattern_size: u64, max_pattern_size: u64, top_frequent_pattern: u64) -> HashMap<Vec<u128>,u128>{

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
        let sample_num = (GRAPH.get_all_vertices(None).count() as u64)*sample_rate/100;
        // println!("Expected Sample length = {:?}", sample_num as u64);
        let conf1 = JobConf::new("conf1");
        let mut start_vertices = _sampling_start_vertex(conf1, area_num).expect("Run Job Error!");
        let mut buffer = vec![];
    
        
        while let Some(Ok(data)) = start_vertices.next() {
            if !sampled.contains(&data){
                sampled.insert(data);
                let sample_label = GRAPH.get_vertex(data as usize).unwrap().get_label();
                let v1: DefaultId = LDBCVertexParser::to_global_id(data as usize, 0);
                mut_graph.add_vertex(v1, sample_label);
                buffer.push(data);
            }
            // println!("{:?}", data);
        }
        while !sampled.len() as u64 >= sample_num {
    
            let conf2 = JobConf::new("conf2");
            if buffer.len() as u64 ==0 {
                // println!("All vertex getted");
                break;
            }
            let mut result2 = _sampling_adjvertex(conf2, &buffer).expect("Run Job Error!");
            buffer.clear();
            while let Some(Ok(data)) = result2.next() {
                let sample_id: u64 = data[1];
                let src_id: u64 = data[0];
                if !sampled.contains(&sample_id) && !(sampled.len() as u64 >= sample_num){
                    sampled.insert(sample_id);
    
                    let v1: DefaultId = LDBCVertexParser::to_global_id(sample_id as usize, 0);
                    let sample_label = GRAPH.get_vertex(sample_id as usize).unwrap().get_label();
                    mut_graph.add_vertex(v1, sample_label);
                    
                    let v2: DefaultId = LDBCVertexParser::to_global_id(src_id as usize, 0);
                    let src_label = GRAPH.get_vertex(src_id as usize).unwrap().get_label();
                    if !sampled.contains(&src_id) && !(sampled.len() as u64 >= sample_num){
                        mut_graph.add_vertex(v2, src_label);
                    }
                    // println!("src: {:?}, dst: {:?}",src_label,sample_label);
                    
    
                    buffer.push(sample_id);
                }
                // println!("{:?}", data);
            }
        }
    }
    

    
    for i in sampled.clone() {
        // edge add
        let conf3 = JobConf::new("conf3");
        let mut adjout = _sampling_outvertex(conf3, &vec![i]).expect("Run Job Error!");
        while let Some(Ok(data)) = adjout.next() {
            
            let dst_id: u64 = data;
            if sampled.contains(&dst_id) {
                mut_graph.add_edge(i as usize, dst_id as usize, 0u8);
            }
        }

    }

    // println!("Sample length = {:?}", sampled.len() as u64);

    let schema_file = "data/schema.json";
    let schema =
            LDBCGraphSchema::from_json_file(schema_file).expect("Read graph schema error!");
    _origion_graph_mining(&mut_graph.into_graph(schema), min_pattern_size, max_pattern_size, top_frequent_pattern, sampled)
    
}
