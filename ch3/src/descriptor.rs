use crate::visualize::{self};
use anyhow::{Context, Result};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use serde_with_expand_env::with_expand_envs;
use std::{
    borrow::Borrow,
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    fmt,
    path::Path,
    str::FromStr,
    time::Duration,
};

/// 用于从String创建自定义类型的宏
macro_rules! custom_type_of_String {
    ($vis:vis $name:ident) => {
        #[doc = concat!("The `", stringify!($name), "` is an alias for String.")]
        #[derive(Debug, Clone,Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        $vis struct $name(String);

        impl From<$name> for String {
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl From<String> for $name {
            fn from(id: String) -> Self {
                Self(id)
            }
        }

        impl FromStr for $name {
            type Err = Infallible;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self(s.to_owned()))
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }

        impl std::ops::Deref for $name {
            type Target = String;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl AsRef<String> for $name {
            fn as_ref(&self) -> &String {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl Borrow<String> for $name {
            fn borrow(&self) -> &String {
                &self.0
            }
        }

        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                &self.0
            }
        }
    };
}

/// 用于解析申明式描述文件的结构体
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Descriptor {
    pub version: String,
    /// 描述整个数据流的部署信息
    #[serde(default)]
    pub deploy: Deploy,
    /// 数据流的运行节点
    pub nodes: Vec<Node>,
}

/// 实现一些函数
impl Descriptor {
    /// 将节点的部署信息设置为默认的部署信息
    /// 如果当前节点没有部署信息，就去获取description的部署信息
    fn resolve_node_deploy_defaults(&self, node: Node) -> Deploy {
        // 处理machine
        let default_machine = self.deploy.machine.clone().unwrap_or_default();
        let machine = match node.deploy.machine {
            Some(m) => m,
            None => default_machine.to_owned(),
        };
        // 重新设置deploy的
        Deploy {
            machine: Some(machine),
            ..node.deploy
        }
    }

    /// 将单op节点转为多op节点,只需要转换一下kind即可
    fn resolve_node_to_operators(&self, node: Node) -> NodeKind {
        if let NodeKind::Operator(operator) = node.kind.clone() {
            // 如果是单个操作符节点，就将其转为多个操作符节点
            NodeKind::Operators(MultipleOperatorDefinitions {
                operators: vec![NormalOperatorDefinition {
                    id: operator
                        .id
                        .unwrap_or_else(|| OperatorId(node.id.to_string())),
                    config: operator.config,
                }],
            })
        } else {
            // 如果不是操作符节点，就直接返回
            node.kind.clone()
        }
    }

    /// 处理每个op的input 映射，将索引的单op型节点，转化一下
    /// 使用 NodeKind::Operator 所以需要先调用
    pub fn resolve_operator_inputs_output(&self) -> Vec<Node> {
        let operator_nodes: Vec<_> = self
            .nodes
            .iter()
            .filter_map(|n| match &n.kind {
                NodeKind::Operator(_) => Some(&n.id),
                _ => None,
            })
            .collect();
        let mut nodes = self.nodes.clone();
        for node in nodes.iter_mut() {
            // 处理每个节点，每个op的输入
            // 这里是打算将所有的Operator都转为Operators
            let input_mappings: Vec<_> = match &mut node.kind {
                NodeKind::Operators(node) => node
                    .operators
                    .iter_mut()
                    .flat_map(|op| op.config.run_config.inputs.values_mut())
                    .collect(),
                NodeKind::Operator(operator) => {
                    operator.config.run_config.inputs.values_mut().collect()
                }
            };
            // 处理每个节点的每个 输入映射
            // 对于所有的user类型的输入映射，都需要将其输出(即mapping的output)转为和输出相同的格式，转为 op_name/output
            for mapping in input_mappings
                .into_iter()
                .filter_map(|i| match &mut i.mapping {
                    InputMapping::Timer { .. } => None,
                    InputMapping::User(m) => Some(m),
                })
            {
                if operator_nodes.contains(&&mapping.source) {
                    mapping.output = DataId::from(format!("{}/{}", mapping.source, mapping.output));
                }
            }
        }
        nodes
    }

    /// 处理节点的一些默认值
    /// 2. 将节点的部署信息设置为默认的部署信息,主要是需要处理需要将节点的默认值和描述的默认值合并
    /// 3. 处理每个节点及OP，每个op的输出，将输出转为和输出相同的格式，转为 op_name/output
    pub fn resolve_node_defaults(&self) -> Vec<Node> {
        let mut nodes = self.resolve_operator_inputs_output();
        for mut node in nodes.iter_mut() {
            // 处理节点的deploy的默认值
            node.deploy = self.resolve_node_deploy_defaults(node.clone());
            // 将node 从 单op节点转为多op节点
            node.kind = self.resolve_node_to_operators(node.clone());
            // 处理节点的input，将索引的单op型节点，转化一下
        }
        nodes
    }

    /// 从文件中读取描述文件
    /// 然后反序列化
    pub fn blocking_read(path: &Path) -> Result<Descriptor> {
        let buf = std::fs::read(path).context("failed to open given file")?;
        Descriptor::parse(buf)
    }

    /// 封装了一下反序列化函数，输入是一个字节数组，输出是一个Descriptor
    /// 这里使用了serde_yaml::from_slice
    /// 所以可以修改该函数修改配置文件类型
    pub fn parse(buf: Vec<u8>) -> Result<Descriptor> {
        serde_yaml::from_slice(&buf).context("failed to parse given descriptor")
    }

    /// 可视化当前dataflow yaml 定义文件，作为mermaid图
    pub fn visualize_as_mermaid(&self) -> Result<String> {
        let resolved = self.resolve_node_defaults();
        let flowchart = visualize::mermaid::visualize_nodes(&resolved);

        Ok(flowchart)
    }
}

custom_type_of_String!(pub NodeId); //节点Id
custom_type_of_String!(pub DataId); //数据Id
custom_type_of_String!(pub WorkId); //工作节点Id
custom_type_of_String!(pub OperatorId); //OpId

/// 自定义枚举类型，分别对应不同类型的环境变量值
/// 分别是布尔值，整数值，字符串值
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EnvValue {
    /// 指定了反序列化函数 with_expand_envs
    /// 它会在转换之前展开所有环境变量的字段
    /// 依赖 creates serde-with-expand-env
    #[serde(deserialize_with = "with_expand_envs")]
    Bool(bool),
    #[serde(deserialize_with = "with_expand_envs")]
    Integer(u64),
    #[serde(deserialize_with = "with_expand_envs")]
    String(String),
}
/// 为环境变量设置Display
impl fmt::Display for EnvValue {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EnvValue::Bool(bool) => fmt.write_str(&bool.to_string()),
            EnvValue::Integer(u64) => fmt.write_str(&u64.to_string()),
            EnvValue::String(str) => fmt.write_str(str),
        }
    }
}

/// 描述节点的部署信息
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deploy {
    /// 节点部署的机器
    pub machine: Option<WorkId>,
    /// 部署时需要限制的资源
    pub cpu: Option<f64>,
    pub gpu: Option<f64>,
    pub memory: Option<i64>,
    /// 并行数                                                                                                                                                                                                                                                                                                                                                                                                                     
    pub min_workers: Option<i64>,
    pub max_wowkers: Option<i64>,
}

/// dataflow的工作节点申明结构体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// 节点ID
    pub id: NodeId,
    /// 节点名称
    pub name: Option<String>,
    /// 节点描述
    pub description: Option<String>,
    /// 环境变量设置
    pub env: Option<BTreeMap<String, EnvValue>>,
    /// 部署信息
    #[serde(default)]
    pub deploy: Deploy,
    /// 嵌入节点类型枚举
    #[serde(flatten)]
    pub kind: NodeKind,
}

/// 节点的类型，这里是个枚举，三选一
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NodeKind {
    Operators(MultipleOperatorDefinitions),
    Operator(SingleOperatorDefinition),
}

/// Operators配置信息的包装-多个Operator列表
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipleOperatorDefinitions {
    pub operators: Vec<NormalOperatorDefinition>,
}

/// 节点的运行配置
/// ```yaml
/// inputs:
///     counter_1: cxx-node-c-api/counter
///     tick:
///         source: dataflow/timer/millis/100
///         queue_size: 1000
/// outputs:
///     - half-status
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRunConfig {
    /// 其中 DataId 就是 inputs 中 数据的id，如counter_1，counter_2 等
    /// Input 就是输入的映射，其中包含了输入的来源和队列的大小
    #[serde(default)]
    pub inputs: BTreeMap<DataId, Input>,

    /// yaml中定义的是一个列表，outputs需要将其反序列化为 dataId set
    #[serde(default)]
    pub outputs: BTreeSet<DataId>,
}

/// 描述了Operator的来源
#[derive(Debug, Serialize, Deserialize, Clone)]
/// 并且所有都使用中划线分割约定
#[serde(rename_all = "kebab-case")]
pub enum OperatorSource {
    SharedLibrary(String),
    Python(String),
    Wasm(String),
    Shell(String),
}

/// Operator配置信息
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OperatorConfig {
    /// 名字
    pub name: Option<String>,
    /// 描述
    pub description: Option<String>,

    /// 自定义节点的运行参数
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<String>,

    ///环境变量
    pub envs: Option<BTreeMap<String, EnvValue>>,

    /// 使用中划线分割定义Operator的来源
    #[serde(flatten)]
    pub source: OperatorSource,

    /// 描述运行之前的构建命令
    /// skip_serializing_if 表示 当值为None时，不序列化
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build: Option<String>,

    /// 节点运行配置
    #[serde(flatten)]
    pub run_config: NodeRunConfig,
}

/// Operator配置信息的包装-单个的op
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SingleOperatorDefinition {
    pub id: Option<OperatorId>,
    /// 嵌入了OperatorConfig
    #[serde(flatten)]
    pub config: OperatorConfig,
}
/// Operator配置信息的包装-多个op
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NormalOperatorDefinition {
    pub id: OperatorId,
    /// 嵌入了OperatorConfig
    #[serde(flatten)]
    pub config: OperatorConfig,
}

///
/// ```yaml
/// inputs:
///     counter_1: cxx-node-c-api/counter
///     tick:
///         source: dataflow/timer/millis/100
///         queue_size: 1000
/// ```
/// 其中
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, from = "InputDef", into = "InputDef")]
pub struct Input {
    pub mapping: InputMapping,
    pub queue_size: Option<usize>,
}
/// 使用InputDef来兼容两种输入格式
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum InputDef {
    /// 1. 只有mapping
    /// counter_1: cxx-node-c-api/counter
    /// 匹配的 cxx-node-c-api/counter
    MappingOnly(InputMapping),
    /// 2. 有mapping和queue_size
    ///     tick:
    ///         source: dataflow/timer/millis/100
    ///         queue_size: 1000
    WithOptions {
        /// 这里匹配的 source: dataflow/timer/millis/100
        source: InputMapping,
        /// 这里匹配的 queue_size: 1000
        queue_size: Option<usize>,
    },
}

/// 为InputDef实现From<Input>和From<InputDef>
impl From<Input> for InputDef {
    fn from(input: Input) -> Self {
        match input {
            Input {
                mapping,
                queue_size: None,
            } => Self::MappingOnly(mapping),
            Input {
                mapping,
                queue_size,
            } => Self::WithOptions {
                source: mapping,
                queue_size,
            },
        }
    }
}

impl From<InputDef> for Input {
    fn from(value: InputDef) -> Self {
        match value {
            InputDef::MappingOnly(mapping) => Self {
                mapping,
                queue_size: None,
            },
            InputDef::WithOptions { source, queue_size } => Self {
                mapping: source,
                queue_size,
            },
        }
    }
}

/// newType模式，创建 FormattedDuration类型专门用于格式化Duration
/// 然后为其实现Display trait
pub struct FormattedDuration(pub Duration);
impl fmt::Display for FormattedDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.subsec_millis() == 0 {
            write!(f, "secs/{}", self.0.as_secs())
        } else {
            write!(f, "millis/{}", self.0.as_millis())
        }
    }
}

/// 解析处理
/// 1. dataflow/timer/millis/100
/// 2. cxx-node-c-api/counter
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum InputMapping {
    /// dataflow/timer/millis/100
    /// 表示的是内部实现的 timer 类型
    /// 未来更多的类型可以在这里添加
    Timer {
        interval: Duration,
    },
    User(UserInputMapping),
}

/// 手动实现序列化
/// 直接序列化为str
impl Serialize for InputMapping {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}
/// 手动实现反序列化
impl<'de> Deserialize<'de> for InputMapping {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // 先反序列化为String
        let string = String::deserialize(deserializer)?;
        // 解析 source/output 的形式
        let (source, output) = string
            .split_once('/')
            .ok_or_else(|| serde::de::Error::custom("input must start with `<source>/`"))?;

        // 根据source的不同，解析为不同的InputMapping
        let deserialized = match source {
            // 如果source 是dataflow，那么表示是内部实现的 output
            // 我们进一步匹配处理 output
            "dataflow" => match output.split_once('/') {
                Some(("timer", output)) => {
                    // 匹配 unit/value 的形式
                    let (unit, value) = output.split_once('/').ok_or_else(|| {
                        serde::de::Error::custom(
                            "timer input must specify unit and value (e.g. `secs/5` or `millis/100`)",
                        )
                    })?;
                    // 只接受 secs 和 millis
                    let interval: Duration = match unit {
                        "secs" => {
                            let value = value.parse().map_err(|_| {
                                serde::de::Error::custom(format!(
                                    "secs must be an integer (got `{value}`)"
                                ))
                            })?;
                            Duration::from_secs(value)
                        }
                        "millis" => {
                            let value = value.parse().map_err(|_| {
                                serde::de::Error::custom(format!(
                                    "millis must be an integer (got `{value}`)"
                                ))
                            })?;
                            Duration::from_millis(value)
                        }
                        other => {
                            return Err(serde::de::Error::custom(format!(
                                "timer unit must be either secs or millis (got `{other}`"
                            )))
                        }
                    };
                    Self::Timer { interval }
                }
                Some((other, _)) => {
                    return Err(serde::de::Error::custom(format!(
                        "unknown dataflow input `{other}`"
                    )))
                }
                None => {
                    return Err(serde::de::Error::custom(
                        "dataflow input has invalid format",
                    ))
                }
            },
            // 否则是用户的 output
            _ => Self::User(UserInputMapping {
                source: source.to_owned().into(),
                output: output.to_owned().into(),
            }),
        };

        Ok(deserialized)
    }
}

impl InputMapping {
    /// 获取 InputMapping 的source
    /// 内部的source是dataflow
    /// 用户的source就是source字段
    pub fn source(&self) -> &NodeId {
        static DATAFLOW_NODE_ID: OnceCell<NodeId> = OnceCell::new();

        match self {
            InputMapping::User(mapping) => &mapping.source,
            InputMapping::Timer { .. } => {
                DATAFLOW_NODE_ID.get_or_init(|| NodeId("dataflow".to_string()))
            }
        }
    }
}

/// 为 InputMapping实现Display trait
impl fmt::Display for InputMapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InputMapping::Timer { interval } => {
                let duration = FormattedDuration(*interval);
                write!(f, "dataflow/timer/{duration}")
            }
            InputMapping::User(mapping) => {
                write!(f, "{}/{}", mapping.source, mapping.output)
            }
        }
    }
}

/// 用户的输入映射
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct UserInputMapping {
    pub source: NodeId,
    pub output: DataId,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_create() {
        let cargo_path = env!("CARGO_MANIFEST_DIR");
        println!("{:?}", cargo_path);
        let pathbuf = PathBuf::from(cargo_path.to_string() + "\\example.yaml");
        println!("{:?}", pathbuf);
        let des = Descriptor::blocking_read(&pathbuf).unwrap();
        println!("{:#?}", des);
        println!("\n\n\n");
        println!("{:#?}", des.resolve_node_defaults());
        println!("\n\n\n");
        let visualized = des.visualize_as_mermaid().unwrap();
        println!("\n\n\n");
        println!("{:#?}", visualized);
        println!("\n\n\n");
    }
}
