use anyhow::Result;
use serde::{Deserialize, Serialize,Deserializer};
use std::collections::{BTreeSet, HashMap, HashSet};

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Source {
    id: String,
    output: String,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Sink {
    id: String,
    input: String,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Operator {
    id: String,
    inputs: BTreeSet<String>,
    outputs: BTreeSet<String>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Descriptor {
    #[serde(default)]
    sources: HashSet<Source>,
    #[serde(default)]
    sinks: HashSet<Sink>,
    #[serde(default)]
    operators: HashSet<Operator>,
    #[serde(default)]
    id: String,
    #[serde(flatten)]
    node: Node,
}
#[derive(Debug,Default, PartialEq, Eq, Serialize, Clone)]
pub struct Node {
    // #[serde(skip_serializing_if = "Option::is_none")]
    /// 节点ID
    pub id: String,
    pub x: String,
}

// 手动进行反序列化
impl<'de> Deserialize<'de> for Node {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut node: Node = Node::default(); // 创建默认的 Node 结构体

        // Deserialize input data as a map
        let map = serde_yaml::Value::deserialize(deserializer)?;
        println!("map:{:#?}",map);

        // Extract id and x from the map
        if let Some(id) = map.get("id").and_then(serde_yaml::Value::as_str) {
            node.id = id.to_owned();
        }

        if let Some(x) = map.get("x").and_then(serde_yaml::Value::as_str) {
            node.x = x.to_owned();
        }

        Ok(node)
    }
}


impl Descriptor {
    pub fn visualize_as_mermaid(&self) -> Result<String> {
        let mut flowchart = "flowchart TB\n".to_owned();
        for source in &self.sources {
            let id = &source.id;
            flowchart.push_str(&format!("  {id}[\\{id}/]\n"));
        }
        for operator in &self.operators {
            let id = &operator.id;
            flowchart.push_str(&format!("  {id}\n"));
        }
        for sink in &self.sinks {
            let id = &sink.id;
            flowchart.push_str(&format!("  {id}[/{id}\\]\n"));
        }

        let mut expected_inputs: HashMap<_, BTreeSet<_>> = HashMap::new();
        for operator in &self.operators {
            for input in &operator.inputs {
                expected_inputs
                    .entry(input.to_owned())
                    .or_default()
                    .insert(&operator.id);
            }
        }
        for sink in &self.sinks {
            expected_inputs
                .entry(sink.input.to_owned())
                .or_default()
                .insert(&sink.id);
        }

        for source in &self.sources {
            let targets = expected_inputs.remove(&source.output).unwrap_or_default();
            let id = &source.id;
            let output = &source.output;
            for target in targets {
                flowchart.push_str(&format!("  {id} -- {output} --> {target}\n"));
            }
        }

        for operator in &self.operators {
            let id = &operator.id;
            for output in &operator.outputs {
                let targets = expected_inputs.remove(output).unwrap_or_default();
                for target in targets {
                    flowchart.push_str(&format!("  {id} -- {output} --> {target}\n"));
                }
            }
        }

        for (output, targets) in expected_inputs.drain() {
            for target in targets {
                flowchart.push_str(&format!("  missing>missing] -- {output} --> {target}\n"));
            }
        }

        Ok(flowchart)
    }
}
