use crate::descriptor::descriptor::{DataId, FormattedDuration, NodeRunConfig, NormalNode};
use anyhow::{Ok, Result};

use super::node::RuntimeNode;
use futures::StreamExt;
use tokio_stream::wrappers::IntervalStream;

/// 定时器节点
pub struct TimerNode(pub RuntimeNode);

/// 启动定时器节点
pub async fn start(nodes: &Vec<NormalNode>, endpoints: &Vec<String>) -> Result<()> {
    let timer_mapping = NormalNode::collect_timer_input_from_nodes(nodes);
    log::debug!("Launch TimerNode {:#?} ", timer_mapping);
    let mut timer_node = TimerNode::init(
        &NodeRunConfig {
            inputs: timer_mapping.clone(),
            outputs: timer_mapping.keys().cloned().collect(),
        },
        &endpoints,
    )?;
    timer_node.run().await?;
    log::debug!("Launch TimerNode success");
    Ok(())
}

// 一些常量
pub const TIMER_NODE_ID: &str = "dataflow/timer";
pub const TIMER_NODE_NAME: &str = "dataflow_timer_node";
pub const TIMER_NODE_MODE: &str = "peer";
pub const TIMER_NODE_DESCRIPTION: &str = "Timer nodes used throughout the entire dataflow network.";

impl TimerNode {
    /// 初始化 Timer 节点
    pub fn init(node_config: &NodeRunConfig, endpoints: &Vec<String>) -> Result<Self> {
        Ok(Self(RuntimeNode::init(
            TIMER_NODE_ID.to_string(),
            TIMER_NODE_NAME.to_string(),
            TIMER_NODE_DESCRIPTION.to_string(),
            node_config.clone(),
            endpoints.clone(),
            TIMER_NODE_MODE.to_string(),
        )?))
    }
    /// 运行节点
    pub async fn run(&mut self) -> Result<()> {
        log::debug!("Node {:?} run", self.id());
        // 收集所有的timer
        for duration in self.node_config().collect_input_timers().into_iter() {
            // 转为duration，并且根据其获取发送者
            let duration_output = FormattedDuration(duration);
            let publisher = self.0.sender(&DataId::from(format!("{duration_output}")))?;
            log::debug!("Node {:?} duration {}", self.id(), duration_output);
            // 然后利用子线程定时的向topic(data_id) 推送消息
            tokio::spawn(async move {
                let mut stream = IntervalStream::new(tokio::time::interval(duration));
                while let Some(_) = stream.next().await {
                    publisher.dyn_clone().publish(&vec![]).expect(&format!(
                        "timer {duration_output} failed to publish timer tick message"
                    ));
                    log::debug!("timer {} publish success", duration_output);
                }
            });
        }
        Ok(())
    }

    /// 获取节点id
    pub fn id(&self) -> &String {
        &self.0.id()
    }
    /// 获取节点名字
    pub fn name(&self) -> &String {
        &self.0.name()
    }
    /// 获取节点描述
    pub fn description(&self) -> &String {
        &self.0.description()
    }
    /// 获取节点运行配置
    pub fn node_config(&self) -> &NodeRunConfig {
        &self.0.node_config()
    }
}
