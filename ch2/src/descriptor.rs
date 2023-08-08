use anyhow::Result;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use serde_with_expand_env::with_expand_envs;
use std::{
    borrow::Borrow,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    convert::Infallible,
    fmt,
    str::FromStr,
    time::Duration,
};

/// 用于从String创建自定义类型的宏
macro_rules! custom_type_of_String {
    ($vis:vis $name:ident) => {
        #[doc = concat!("The `", stringify!($name), "` is an alias for String.")]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
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
    pub nodes: Vec<Node>,
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

/// 节点的类型，这里是个枚举，三选一
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeKind {
    Custom(CustomNode),
    /// 使用 SingleOperatorDefinition 来定义一个单一的 Operator
    /// 这里的因含意思就是将Operator 作为一个Node来使用
    /// 但是一般不会这么使用
    Operator(SingleOperatorDefinition),
    /// 多个配置
    Operators(MultipleOperatorDefinitions),
}

/// 自定义节点的运行配置
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRunConfig {
    #[serde(default)]
    pub inputs: BTreeMap<DataId, Input>,
    #[serde(default)]
    pub outputs: BTreeSet<DataId>,
}

/// 描述了Operator的来源
/// 并且所有都使用中划线分割约定
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub enum OperatorSource {
    SharedLibrary(String),
    Python(String),
    Wasm(String),
}

/// 自定义节点配置信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomNode {
    /// 自定义节点的来源
    pub source: String,
    /// 自定义节点的运行参数
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<String>,

    ///环境变量
    pub envs: Option<BTreeMap<String, EnvValue>>,

    /// 描述运行之前的构建命令
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build: Option<String>,

    /// 节点运行配置
    #[serde(flatten)]
    pub run_config: NodeRunConfig,
}

/// Operator配置信息
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OperatorConfig {
    /// 名字
    pub name: Option<String>,
    /// 描述
    pub description: Option<String>,

    /// 使用中划线分割定义Operator的来源
    #[serde(flatten)]
    pub source: OperatorSource,

    /// 描述运行之前的构建命令
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build: Option<String>,

    /// 节点运行配置
    #[serde(flatten)]
    pub run_config: NodeRunConfig,
}

/// Operator配置信息的包装
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NormalOperatorDefinition {
    pub id: OperatorId,
    /// 嵌入了OperatorConfig
    #[serde(flatten)]
    pub config: OperatorConfig,
}

/// Operator配置信息的包装-单个的Operator，这里的ID是复用的Node的ID
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SingleOperatorDefinition {
    /// 如果是 SingleOperatorDefinition ，这里的id会和上一级的id冲突，所以我们需要手动设置这个id
    pub id: Option<OperatorId>,

    /// 嵌入了 Operator 的配置信息
    #[serde(flatten)]
    pub config: OperatorConfig,
}

/// Operators列表
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MultipleOperatorDefinitions {
    pub operators: Vec<NormalOperatorDefinition>,
}


#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, from = "InputDef", into = "InputDef")]
pub struct Input {
    pub mapping: InputMapping,
    pub queue_size: Option<usize>,
}

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

pub fn format_duration(interval: Duration) -> FormattedDuration {
    FormattedDuration(interval)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum InputMapping {
    Timer { interval: Duration },
    User(UserInputMapping),
}

impl InputMapping {
    pub fn source(&self) -> &NodeId {
        static DORA_NODE_ID: OnceCell<NodeId> = OnceCell::new();

        match self {
            InputMapping::User(mapping) => &mapping.source,
            InputMapping::Timer { .. } => DORA_NODE_ID.get_or_init(|| NodeId("dora".to_string())),
        }
    }
}

impl fmt::Display for InputMapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InputMapping::Timer { interval } => {
                let duration = format_duration(*interval);
                write!(f, "dora/timer/{duration}")
            }
            InputMapping::User(mapping) => {
                write!(f, "{}/{}", mapping.source, mapping.output)
            }
        }
    }
}

impl Serialize for InputMapping {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for InputMapping {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        let (source, output) = string
            .split_once('/')
            .ok_or_else(|| serde::de::Error::custom("input must start with `<source>/`"))?;

        let deserialized = match source {
            "dataflow" => match output.split_once('/') {
                Some(("timer", output)) => {
                    let (unit, value) = output.split_once('/').ok_or_else(|| {
                        serde::de::Error::custom(
                            "timer input must specify unit and value (e.g. `secs/5` or `millis/100`)",
                        )
                    })?;
                    let interval = match unit {
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
                        "unknown dora input `{other}`"
                    )))
                }
                None => return Err(serde::de::Error::custom("dora input has invalid format")),
            },
            _ => Self::User(UserInputMapping {
                source: source.to_owned().into(),
                output: output.to_owned().into(),
            }),
        };

        Ok(deserialized)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct UserInputMapping {
    pub source: NodeId,
    pub output: DataId,
}


#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum InputDef {
    MappingOnly(InputMapping),
    WithOptions {
        source: InputMapping,
        queue_size: Option<usize>,
    },
}

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

