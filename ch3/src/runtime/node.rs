use crate::{
    communication::{pub_sub::ZenohCommunicationLayer, PubSubCommunicationLayer, Publisher},
    descriptor::descriptor::{DataId, NodeRunConfig},
};
use anyhow::{anyhow, Result};

/// 运行时节点
pub struct RuntimeNode {
    /// 节点id
    id: String,
    /// 节点名称
    name: String,
    /// 节点描述
    description: String,
    /// 节点运行配置
    node_config: NodeRunConfig,
    /// 通信层
    communication: Box<dyn PubSubCommunicationLayer>,
}

impl RuntimeNode {
    /// 初始化 节点
    /// id: OperatorId/NodeId,
    /// name: Operator name,
    /// description: Operator description,
    /// node_config: Operator node_config,
    /// endpoints: Node Deploy endpoints,
    /// mode: Node Deploy mode,
    pub fn init(
        id: String,
        name: String,
        description: String,
        node_config: NodeRunConfig,
        endpoints: Vec<String>,
        mode: String,
    ) -> Result<Self> {
        let communication = Box::new(ZenohCommunicationLayer::init(endpoints, mode, id.clone())?);
        Ok(Self {
            id,
            name,
            description,
            node_config,
            communication: communication,
        })
    }

    /// 获取当前节点的某个数据的发送者
    pub fn sender(&mut self, data_id: &DataId) -> Result<Box<dyn Publisher>> {
        log::debug!("Node {:?} sender with data_id: {}", self.id, data_id);
        Ok(self
            .communication
            .publisher(&data_id)
            .expect(&format!(
                "failed create publisher for output {data_id} of node {node_id}",
                data_id = data_id.to_string(),
                node_id = self.id
            ))
            .dyn_clone())
    }
    /// 从当前节点向output发送数据
    pub fn send_output(&mut self, data_id: &DataId, data: &[u8]) -> Result<()> {
        let topic = format!("{self_id}/{data_id}", self_id = &self.id);
        if !self.node_config.outputs.contains(data_id) {
            return Err(anyhow!("send output failed ,unknown output {data_id}"));
        }
        self.sender(&DataId::from(topic.clone()))?
            .publish(data)
            .map_err(|e| anyhow!("send output to topic:{topic} failed,: {e}"))?;
        Ok(())
    }

    /// 获取节点id
    pub fn id(&self) -> &String {
        &self.id
    }
    /// 获取节点名字
    pub fn name(&self) -> &String {
        &self.name
    }
    /// 获取节点描述
    pub fn description(&self) -> &String {
        &self.description
    }
    /// 获取节点运行配置
    pub fn node_config(&self) -> &NodeRunConfig {
        &self.node_config
    }
}
