use crate::descriptor::Node;
use crate::descriptor::{
    DataId, FormattedDuration, Input, InputMapping, MultipleOperatorDefinitions, NodeId, NodeKind,
    NormalOperatorDefinition, UserInputMapping,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt::Write as _,
    time::Duration,
};

/// 将所有节点转为mermaid图字符串
pub fn visualize_nodes(nodes: &[Node]) -> String {
    let mut flowchart = "flowchart TB\n".to_owned();
    let mut all_nodes = HashMap::new();

    // 处理节点信息
    for node in nodes {
        visualize_node(node, &mut flowchart);
        all_nodes.insert(&node.id, node);
    }

    // 处理dataflow中的timer
    let dataflow_timers = collect_dataflow_timers(nodes);
    if !dataflow_timers.is_empty() {
        writeln!(flowchart, "subgraph ___dataflow___ [dataflow]").unwrap();
        writeln!(flowchart, "  subgraph ___timer_timer___ [timer]").unwrap();
        for interval in dataflow_timers {
            let duration = FormattedDuration(interval);
            writeln!(flowchart, "    dataflow/timer/{duration}[\\{duration}/]").unwrap();
        }
        flowchart.push_str("  end\n");
        flowchart.push_str("end\n");
    }

    // 处理每个节点的输入
    for node in nodes {
        visualize_node_inputs(node, &mut flowchart, &all_nodes)
    }

    flowchart
}

/// 收集dataflow中的timer
pub fn collect_dataflow_timers(nodes: &[Node]) -> BTreeSet<Duration> {
    let mut dataflow_timers = BTreeSet::new();
    for node in nodes {
        match &node.kind {
            NodeKind::Operators(node) => {
                for operator in &node.operators {
                    collect_dataflow_nodes(
                        operator.config.run_config.inputs.values(),
                        &mut dataflow_timers,
                    );
                }
            }

            NodeKind::Operator(_) => todo!(),
        }
    }
    dataflow_timers
}

/// 将inputs中的timer收集起来，放入到dataflow_timers
fn collect_dataflow_nodes(
    values: std::collections::btree_map::Values<DataId, Input>,
    dataflow_timers: &mut BTreeSet<Duration>,
) {
    for input in values {
        match &input.mapping {
            InputMapping::User(_) => {}
            InputMapping::Timer { interval } => {
                dataflow_timers.insert(*interval);
            }
        }
    }
}

// 可视化节点，主要是operators的可视化，将其转为字符串
fn visualize_node(node: &Node, flowchart: &mut String) {
    let node_id = &node.id;
    match &node.kind {
        NodeKind::Operators(MultipleOperatorDefinitions { operators, .. }) => {
            visualize_operators(node_id, operators, flowchart)
        }
        NodeKind::Operator(_) => todo!(),
    }
}

/// 可视化operators
fn visualize_operators(
    node_id: &NodeId,
    operators: &[NormalOperatorDefinition],
    flowchart: &mut String,
) {
    writeln!(flowchart, "subgraph {node_id}").unwrap();
    for operator in operators {
        let operator_id = &operator.id;
        if operator.config.run_config.inputs.is_empty() {
            // source operator
            writeln!(flowchart, "  {node_id}/{operator_id}[\\{operator_id}/]").unwrap();
        } else if operator.config.run_config.outputs.is_empty() {
            // sink operator
            writeln!(flowchart, "  {node_id}/{operator_id}[/{operator_id}\\]").unwrap();
        } else {
            // normal operator
            writeln!(flowchart, "  {node_id}/{operator_id}[{operator_id}]").unwrap();
        }
    }

    flowchart.push_str("end\n");
}

fn visualize_node_inputs(node: &Node, flowchart: &mut String, nodes: &HashMap<&NodeId, &Node>) {
    let node_id = &node.id;
    match &node.kind {
        NodeKind::Operators(MultipleOperatorDefinitions { operators, .. }) => {
            for operator in operators {
                visualize_operator_inputs(
                    &format!("{node_id}/{}", operator.id),
                    &operator.config.run_config.inputs,
                    flowchart,
                    nodes,
                )
            }
        }
        NodeKind::Operator(_) => todo!(),
    }
}

fn visualize_operator_inputs(
    target: &str,
    inputs: &BTreeMap<DataId, Input>,
    flowchart: &mut String,
    nodes: &HashMap<&NodeId, &Node>,
) {
    for (input_id, input) in inputs {
        println!("input_id: {:#?}", input_id);
        println!("input: {:#?}", input);
        match &input.mapping {
            // 对于时间类型的输入，将timmer 作为 source
            mapping @ InputMapping::Timer { .. } => {
                writeln!(flowchart, "  {} -- {input_id} --> {target}", mapping).unwrap();
            }
            InputMapping::User(mapping) => {
                visualize_user_mapping(mapping, target, nodes, input_id, flowchart)
            }
        }
    }
}

fn visualize_user_mapping(
    mapping: &UserInputMapping,
    target: &str,
    nodes: &HashMap<&NodeId, &Node>,
    input_id: &DataId,
    flowchart: &mut String,
) {
    let UserInputMapping { source, output } = mapping;
    let mut source_found = false;
    if let Some(source_node) = nodes.get(source) {
        match &source_node.kind {
            NodeKind::Operators(MultipleOperatorDefinitions { operators, .. }) => {
                let (operator_id, output) = output.split_once('/').unwrap_or(("", output));
                if let Some(operator) = operators
                    .iter()
                    .find(|o| o.id.to_string().as_str() == operator_id)
                {
                    if operator.config.run_config.outputs.contains(output) {
                        let data = if output == input_id.as_str() {
                            output.to_string()
                        } else {
                            format!("{output} as {input_id}")
                        };
                        writeln!(flowchart, "  {source}/{operator_id} -- {data} --> {target}")
                            .unwrap();
                        source_found = true;
                    }
                }
            }
            _ => todo!(),
        }
    }
    if !source_found {
        writeln!(flowchart, "  missing>missing] -- {input_id} --> {target}").unwrap();
    }
}
