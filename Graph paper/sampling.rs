fn sampling() {
    // parameter N
    let mut SampleNum = N;
    let mut plan = LogicalPlan::default()
    // Randomly select unique nodes as
    // starting nodes
    // needs sampling like sample() or coin()
    let scan = = pb::Scan {
        scan_opt: 0,
        alias: None,
        params: Some(query_params(vec![], vec![])),
        idx_predicate: None,
    };
    // while plan.len() < N {
    let mut opr_id = plan
            .append_operator_as_node(scan.into(), vec![])
            .unwrap();
    // 结合自己的pattern mining算法，边采样边挖掘
    // 与其说是sampling + mining/counting
    // 可简化为 mining/counting with sampling Algorithm
    let expand = pb::EdgeExpand {
        v_tag: None,
        direction: 0,
        params: Some(query_params(vec![], vec![])),
        is_edge: true,
        alias: Some("b".into()),
    };
    opr_id = plan
            .append_operator_as_node(expand.into(), vec![opr_id as u32])
            .unwrap();
    let getv = pb::GetV {
        tag: None,
        opt: 1,
        params: Some(query_params(vec![], vec![])),
        alias: Some("v".into()),
    };
    plan.append_operator_as_node(getv.into(), vec![1])
        .unwrap();
    // e.g. FSGS Simple Mining
    Mining()
    // end while
}