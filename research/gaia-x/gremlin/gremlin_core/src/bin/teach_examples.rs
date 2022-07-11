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
    let mut f = std::fs::File::create("data.txt").unwrap();

    _sampling_degree_distribution();

    // Exp1
    f.write("\n\n".as_bytes()).unwrap();
    f.write("   Experiment 1 ".as_bytes()).unwrap();
    f.write("\n\n".as_bytes()).unwrap();
    let para_1_exp1 = vec![5];
    let para_2_exp1 = vec![5];
    let para_3_exp1 = vec![2];
    let para_4_exp1 = vec![3];
    
    
    for l in para_4_exp1.clone() {
        for k in para_3_exp1.clone() {
            let start_full = Instant::now();
            println!("======================================================================");
            println!("              Sample ALL                ");
            println!("======================================================================");
            println!("rate: {:?},  area: {:?},  min_size: {:?},  max_size: {:?}", 100, 0, &k, &l);
            let mut cmp_result = _sampling_arrange(1, 10, k, l, 15, 0);
            let end_full = Instant::now();
            println!("time cost: {:?}",end_full.duration_since(start_full));
            for j in para_2_exp1.clone() {
                
                f.write("\n\n".as_bytes()).unwrap();
                f.write("area: ".as_bytes()).unwrap();
                f.write(j.to_string().as_bytes()).unwrap();
                f.write("   min_size: ".as_bytes()).unwrap();
                f.write(k.to_string().as_bytes()).unwrap();


                    for i in para_1_exp1.clone() {
                        let sample_method = 1u64;
                        let start_time = Instant::now();
                        println!("======================================================================");
                        println!("                              ");
                        println!("======================================================================");
                        println!("rate: {:?},  area: {:?},  min_size: {:?},  max_size: {:?}", &i, &j, &k, &l);
                        let result = _sampling_arrange(i, j, k, l, l, sample_method);
                        let end_time = Instant::now();
                        f.write("\n\n".as_bytes()).unwrap();
                        f.write("   sample rate: ".as_bytes()).unwrap();
                        f.write(i.to_string().as_bytes()).unwrap();
                        for q in result{
                            f.write("\n".as_bytes()).unwrap();
                            f.write(q.0.to_string().as_bytes());
                            f.write(": ".as_bytes()).unwrap();
                            f.write(q.1.to_string().as_bytes());
                        }
                        println!("time cost: {:?}",end_time.duration_since(start_time));
                    }
                
                
            }
        }
    }

    // Exp2
    f.write("\n\n".as_bytes()).unwrap();
    f.write("   Experiment 2 ".as_bytes()).unwrap();
    f.write("\n\n".as_bytes()).unwrap();
    let para_1_exp2 = vec![10];
    let para_2_exp2 = vec![10];
    let para_3_exp2 = vec![2];
    let para_4_exp2 = vec![4, 5];
    for l in para_4_exp2.clone() {
        for k in para_3_exp2.clone() {
            for j in para_2_exp2.clone() {
                
                f.write("\n\n".as_bytes()).unwrap();
                f.write("area: ".as_bytes()).unwrap();
                f.write(j.to_string().as_bytes()).unwrap();
                f.write("   min_size: ".as_bytes()).unwrap();
                f.write(k.to_string().as_bytes()).unwrap();


                    for i in para_1_exp2.clone() {
                        let sample_method = 1u64;
                        let start_time = Instant::now();
                        println!("======================================================================");
                        println!("                              ");
                        println!("======================================================================");
                        println!("rate: {:?},  area: {:?},  min_size: {:?},  max_size: {:?}", &i, &j, &k, &l);
                        let result = _sampling_arrange(i, j, k, l, l, sample_method);
                        let end_time = Instant::now();
                        f.write("\n\n".as_bytes()).unwrap();
                        f.write("   sample rate: ".as_bytes()).unwrap();
                        f.write(i.to_string().as_bytes()).unwrap();
                        for q in result{
                            f.write("\n".as_bytes()).unwrap();
                            f.write(q.0.to_string().as_bytes());
                            f.write(": ".as_bytes()).unwrap();
                            f.write(q.1.to_string().as_bytes());
                        }
                        println!("time cost: {:?}",end_time.duration_since(start_time));
                    }
                
                
            }
        }
    }
    
    // Exp3
    f.write("\n\n".as_bytes()).unwrap();
    f.write("   Experiment 3 ".as_bytes()).unwrap();
    f.write("\n\n".as_bytes()).unwrap();
    let para_1_exp3 = vec![10, 15];
    let para_2_exp3 = vec![10, 20, 40];
    let para_3_exp3 = vec![2];
    let para_4_exp3 = vec![3];
    for l in para_4_exp3.clone() {
        for k in para_3_exp3.clone() {
            for j in para_2_exp3.clone() {
                
                f.write("\n\n".as_bytes()).unwrap();
                f.write("area: ".as_bytes()).unwrap();
                f.write(j.to_string().as_bytes()).unwrap();
                f.write("   min_size: ".as_bytes()).unwrap();
                f.write(k.to_string().as_bytes()).unwrap();


                    for i in para_1_exp3.clone() {
                        let sample_method = 1u64;
                        let start_time = Instant::now();
                        println!("======================================================================");
                        println!("                              ");
                        println!("======================================================================");
                        println!("rate: {:?},  area: {:?},  min_size: {:?},  max_size: {:?}", &i, &j, &k, &l);
                        let result = _sampling_arrange(i, j, k, l, l, sample_method);
                        let end_time = Instant::now();
                        f.write("\n\n".as_bytes()).unwrap();
                        f.write("   sample rate: ".as_bytes()).unwrap();
                        f.write(i.to_string().as_bytes()).unwrap();
                        for q in result{
                            f.write("\n".as_bytes()).unwrap();
                            f.write(q.0.to_string().as_bytes());
                            f.write(": ".as_bytes()).unwrap();
                            f.write(q.1.to_string().as_bytes());
                        }
                        println!("time cost: {:?}",end_time.duration_since(start_time));
                    }
                
                
            }
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
                let label = (GRAPH.get_vertex(*v_id as usize).unwrap().get_label()[0] )as u64;
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
    for i in 0..50u8 {
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

// pattern mining
// src: 此次faltmap的点id，path：按gid排列的点id，target——label：拓展边label
fn _mining_adjvertex(conf: JobConf, src_gid: u64, target_label: u8, target_gid: u64, path: &Vec<Vec<u64>>) -> Result<ResultStream<Vec<u64>>, JobSubmitError> {
    pegasus::run(conf, move || {
        let src = path.clone();
        move |input, output| {
            input.input_from(src.into_iter())?
            .flat_map( move |v_id| {
                let adj_vertices = GRAPH.get_adj_vertices(v_id[src_gid as usize] as usize, Some(&vec![target_label]), Direction::Outgoing);
                Ok(adj_vertices.map(move |v| {
                    let mut path = v_id.clone();

                    path[target_gid as usize] = v.get_id() as u64;
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
fn _sampling_arrange(sample_rate: u64, area_num: u64, min_pattern_size: u64, max_pattern_size: u64, top_frequent_pattern: u64, sample_method: u64) -> HashMap<std::string::String, u128> {

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
    
                    buffer.push(sample_id);
                }
                // println!("{:?}", data);
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
    _pattern_counting(&mut_graph.into_graph(schema), min_pattern_size, max_pattern_size, top_frequent_pattern, sampled, sample_method)
    
}

fn _pattern_counting(graphdb: &LargeGraphDB, min_pattern_size: u64, max_pattern_size: u64, top_frequent_pattern: u64, vertex_set: HashSet<u64>, sample_method: u64) -> HashMap<std::string::String, u128> {
    
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
                    let value = catalog[&key] as u128 +1;
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


fn _pattern_mining(pattern: Vec<Vec<String>>) -> HashMap<std::string::String, u64> {
    
    let mut catalog: HashMap<String,u64> = HashMap::new();
    for l in pattern{
        // idx: each vertex -> [[gid, label], adjlist]
        // adjlist: Vec![edge label, dst_gid]
        let decode_res: Vec<Vec<Vec<u64>>> = _decode_pattern(l.clone());
        // let occur_set: HashSet::new();
        
        let mut path: Vec<Vec<u64>> = Vec::new();

        for i in decode_res {
            let mut local_todo: Vec<u64> = Vec::new();
            let gid = i[0][0];
            let label = i[0][1];
            let adj_vtx = i[1].clone();
            if gid==0 {
                let start = GRAPH.get_all_vertices(Some(&vec![label as u8]));
                for i in start {
                    path.push(vec![i.get_id() as u64,0,0,0,0]);
                    local_todo.push(i.get_id() as u64);
                }
            }

            for j in 0..adj_vtx.len()/2 {
                let target_label = adj_vtx[j*2] as u8;
                let target_gid = adj_vtx[j*2+1];
                let conf = JobConf::new("mining");
                let mut result = _mining_adjvertex(conf, gid, target_label, target_gid, &path).expect("Run Job Error!");
                
                let mut p: Vec<Vec<u64>> = Vec::new();
                while let Some(Ok(data)) = result.next() {
                    p.push(data);
                }
                path = p;
            }
        }
        for i in 0..path.len(){
            let mut k = path[i].clone();
            k.sort();
            for j in 0..k.len()-1 {
                if k[j]==k[j+1] {
                    path.remove(i);
                }
            }
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

fn _pattern_estimation_CEG(graphdb: &LargeGraphDB, pattern: Vec<Vec<String>>, pattern_table: HashMap<std::string::String, u64>) -> HashMap<std::string::String, u64> {
    let mut catalog: HashMap<String,u64> = HashMap::new();
    for i in pattern {
        // idx: each vertex -> [[gid, label], adjlist]
        // adjlist: Vec![edge label, dst_gid]
        let decode_res: Vec<Vec<Vec<u64>>> = _decode_pattern_twosides(i.clone());
        let gid_table = _get_pattern_gid(i.clone());
        // to detect if it is loop(in thresold length), it stores the previous vertex combination
        // once there is a new vertex that has two connection with vtx in previous combination
        // treat as loop
        let mut occur_set: HashSet<Vec<u64>> = HashSet::new();
        let mut occur_vtx: HashSet<u64> = HashSet::new();

        let path_len: u64;
        let aggregator: u64;
        let depth: u64 = 0;

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
            for i in 0..list_adj.len()/2 {
                let mut to_push_list = cur_path.clone();
                if !to_push_list.contains(&list_adj[i*2+1]) {
                    to_push_list.push(list_adj[i*2+1]);
                }
                if to_push_list.len() < decode_res.len() {
                    gpe.push(to_push_list);
                }
                else {
                    result_list.push(to_push_list);
                }
            }
        }
        // path generated
        let mut estimate_result:u64 =0;
        if (result_list[0].len() < depth as usize) {
            // return cardinality
            let mut local_code:String = String::from("");
            let mut list = i.clone();
            list.sort();
            for m in list{
                local_code = local_code + &m;
                local_code = local_code + "_";
            }
            estimate_result = pattern_table[&local_code];
            catalog.insert(local_code, estimate_result);
        }
        else {
            let mut estimations: Vec<u64> = Vec::new();
            for path in result_list{
                let mut local_code:String = String::from("");
                let mut pattern_table_vertex: HashSet<u64> = HashSet::new();
                for l in 0..depth as usize {
                    pattern_table_vertex.insert(path[l]);
                }
                let mut list = i.clone();
                list.sort();
                for pattern_edges in list.clone(){
                    let v: Vec<&str> = pattern_edges.split("_").collect();
                    let v1: (u64,u64) = (v[0].parse().unwrap(),v[4].parse().unwrap());
                    let v2: (u64,u64) = (v[1].parse().unwrap(),v[5].parse().unwrap());
                    if pattern_table_vertex.contains(&gid_table[&v1]) && pattern_table_vertex.contains(&gid_table[&v2]) {
                        local_code = local_code + &pattern_edges;
                        local_code = local_code + "_";
                    }
                }
                estimate_result = pattern_table[&local_code];
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
                    for idx in 0..extend_adj.len()/2 {
                        for j in 0..a{
                            if extend_adj[idx*2+1]==j as u64 {
                                adj_list.push(extend_adj[idx*2+1]);
                                adj_list_edge.push(extend_adj[idx*2]);
                            }
                        }
                    }
                    if adj_list.len() >= 2 {
                        // TODO: loop case
                    }
                    else if adj_list.len() <1 {
                        println!("find previous node in path error!");
                    }
                    else {
                        // NOT loop case
                        assert_eq!(adj_list.len(),1);
                        let previous_node = adj_list[0];
                        let current_edge = adj_list_edge[0];
                        let previous_copy = decode_res[previous_node as usize].clone();
                        let previous_adj = previous_copy[1].clone();
                        let mut local_max_edge: u64 = 0;
                        let mut local_max_weight: u64 = 1;
                        let mut current_weight: u64 = 1;
                        for idx in 0..previous_adj.len()/2 {
                            if previous_adj[idx*2+1] != extend_gid {
                                let weight_count = graphdb.count_all_edges(Some(&vec![previous_adj[idx*2] as u8])) as u64;
                                if weight_count >= local_max_weight {
                                    local_max_weight = weight_count;
                                    local_max_edge = previous_adj[idx*2];
                                }
                            }
                        }
                        // finish selected max-weight-path
                        //write a flatmap to get (local_edge---flat_map----current_edge / local_edge)
                        let conf = JobConf::new("Extending");
                        let prev_node_with_prev_edge = graphdb.get_all_edges(Some(&vec![local_max_edge as u8]));
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
                        estimate_result = estimate_result * count / prev_count as u64;
                    }
                    

                }
                // here has generated a result for current path
                estimations.push(estimate_result);
            }
            // TODO: aggregate it
            // avg, max, min ...
        }
        let mut local_code:String = String::from("");
        let mut list = i.clone();
        list.sort();
        for m in list{
            local_code = local_code + &m;
            local_code = local_code + "_";
        }
        catalog.insert(local_code, estimate_result);
    }
    catalog
}

// fn get_adj_inpath: given decode_res, visited_set, extend_vertex
// judge connected edges with visited_set, return vec![index of edge] of adj_list of extend_vtx

// fn _pattern_estimation_MDTree(graphdb: &LargeGraphDB, pattern: Vec<String>) -> HashMap<std::string::String, u128> {
//     for i in pattern {
//         // decode the pattern
//         let decode_res: 
//     }
// }

// fn _pattern_generate() {

// }

fn _decode_pattern(pattern: Vec<String>) ->Vec<Vec<Vec<u64>>> {
    let mut occur_set: HashMap<(u64, u64),u64> = HashMap::new();
    let mut gids: Vec<Vec<Vec<u64>>> = Vec::new();
    for i in pattern.clone() {
        let v: Vec<&str> = i.split("_").collect();
        let v1: (u64,u64) = (v[0].parse().unwrap(),v[4].parse().unwrap());
        let v2: (u64,u64) = (v[1].parse().unwrap(),v[5].parse().unwrap());
        if !occur_set.contains_key(&v1) {
            occur_set.insert(v1, occur_set.len() as u64);
            gids.push(vec![vec![occur_set.len() as u64, v[0].parse().unwrap()],vec![]]);
        }
        if !occur_set.contains_key(&v2) {
            occur_set.insert(v2, occur_set.len() as u64);
            gids.push(vec![vec![occur_set.len() as u64, v[1].parse().unwrap()],vec![]]);
        }
    }
    
    for i in pattern.clone() {
        let v: Vec<&str> = i.split("_").collect();
        let v1: (u64,u64) = (v[0].parse().unwrap(),v[4].parse().unwrap());
        let v2: (u64,u64) = (v[1].parse().unwrap(),v[5].parse().unwrap());
        let edgelabel:u8 = v[2].parse().unwrap();
        if v[3]=="0" {
            let idx =occur_set[&v1];
            gids[idx as usize][1].push(edgelabel as u64);
            gids[idx as usize][1].push(occur_set[&v2]);
        }
        else {
            let idx =occur_set[&v2];
            gids[idx as usize][1].push(edgelabel as u64);
            gids[idx as usize][1].push(occur_set[&v1]);
        }
    }
    gids
}

fn _decode_pattern_twosides(pattern: Vec<String>) ->Vec<Vec<Vec<u64>>> {
    let mut occur_set: HashMap<(u64, u64),u64> = HashMap::new();
    let mut gids: Vec<Vec<Vec<u64>>> = Vec::new();
    for i in pattern.clone() {
        let v: Vec<&str> = i.split("_").collect();
        let v1: (u64,u64) = (v[0].parse().unwrap(),v[4].parse().unwrap());
        let v2: (u64,u64) = (v[1].parse().unwrap(),v[5].parse().unwrap());
        if !occur_set.contains_key(&v1) {
            occur_set.insert(v1, occur_set.len() as u64);
            gids.push(vec![vec![occur_set.len() as u64, v[0].parse().unwrap()],vec![]]);
        }
        if !occur_set.contains_key(&v2) {
            occur_set.insert(v2, occur_set.len() as u64);
            gids.push(vec![vec![occur_set.len() as u64, v[1].parse().unwrap()],vec![]]);
        }
    }
    
    for i in pattern.clone() {
        let v: Vec<&str> = i.split("_").collect();
        let v1: (u64,u64) = (v[0].parse().unwrap(),v[4].parse().unwrap());
        let v2: (u64,u64) = (v[1].parse().unwrap(),v[5].parse().unwrap());
        let edgelabel:u8 = v[2].parse().unwrap();
        if v[3]=="0" {
            let idx =occur_set[&v1];
            gids[idx as usize][1].push(edgelabel as u64);
            gids[idx as usize][1].push(occur_set[&v2]);
            gids[idx as usize][1].push(0);

            let idx =occur_set[&v2];
            gids[idx as usize][1].push(edgelabel as u64);
            gids[idx as usize][1].push(occur_set[&v1]);
            gids[idx as usize][1].push(1);
        }
        else {
            let idx =occur_set[&v2];
            gids[idx as usize][1].push(edgelabel as u64);
            gids[idx as usize][1].push(occur_set[&v1]);
            gids[idx as usize][1].push(0);
            
            let idx =occur_set[&v1];
            gids[idx as usize][1].push(edgelabel as u64);
            gids[idx as usize][1].push(occur_set[&v2]);
            gids[idx as usize][1].push(1);
        }
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
        for i in 1..index_sort.len() {
            for j in 0..index_sort.len() {
                if !_cmp(index_sort[i],index_sort[i-1], graphdb) {
                    let mid = index_sort[i];
                    index_sort[i] = index_sort[i-1];
                    index_sort[i-1] = mid;
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
fn _update_catalog(src: Vec<u64>, graphdb: &LargeGraphDB) -> String {
    let mut vertex_set = HashSet::new();
    let cloneset = src.clone();
    let indexing_map = _indexing_vertex(src, graphdb);
    for i in cloneset {
        vertex_set.insert(i);
    }
    let edgeset = _get_edges(vertex_set, graphdb);

    let code_len = edgeset.len();
    let mut total_code = code_len.to_string();
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
        total_code += "==";
        total_code += &i;
    }

    total_code
}



