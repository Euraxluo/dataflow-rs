use std::collections::BTreeMap;

use crate::descriptor::descriptor::{DataId, Deploy, FormattedDuration, NodeRunConfig, NormalNode};
use anyhow::Result;

use futures::StreamExt;
use log::{debug, info};
use tokio_stream::wrappers::IntervalStream;

use super::Runtime;

/// 启动定时器节点
pub async fn start(nodes: &Vec<NormalNode>, deploy: &Deploy) -> Result<()> {
    let timer_mapping = NormalNode::collect_timer_input_from_nodes(nodes);
    info!("Start TimerNode {:#?} ", timer_mapping);
    let mut timer_node = TimerNode::init(
        &NodeRunConfig {
            inputs: timer_mapping.clone(),
            outputs: timer_mapping.keys().cloned().collect(),
        },
        deploy.endpoints.as_ref().clone().unwrap(),
    )?;
    timer_node.run().await?;
    info!("Start TimerNode success");
    Ok(())
}

/// 定时器节点
pub struct TimerNode(pub Runtime);

// 一些常量
pub const TIMER_NODE_ID: &str = "dataflow/timer";
pub const TIMER_NODE_NAME: &str = "dataflow_timer_node";
pub const TIMER_NODE_MODE: &str = "peer";
pub const TIMER_NODE_DESCRIPTION: &str = "Timer nodes used throughout the entire dataflow network.";

impl TimerNode {
    /// 初始化 Timer 节点
    pub fn init(node_config: &NodeRunConfig, endpoints: &Vec<String>) -> Result<Self> {
        Ok(Self(Runtime::init(
            TIMER_NODE_ID.to_string(),
            TIMER_NODE_NAME.to_string(),
            TIMER_NODE_DESCRIPTION.to_string(),
            BTreeMap::new(),
            node_config.clone(),
            endpoints.clone(),
            TIMER_NODE_MODE.to_string(),
        )?))
    }
    /// 运行节点
    pub async fn run(&mut self) -> Result<()> {
        debug!("Node {:?} run", self.id());
        // 收集所有的timer
        for duration in self.node_config().collect_input_timers().into_iter() {
            // 转为duration，并且根据其获取发送者
            let duration_output = FormattedDuration(duration);
            let publisher = self.0.sender(&DataId::from(format!("{duration_output}")))?;
            debug!("Node {:?} duration {}", self.id(), duration_output);
            // 然后利用子线程定时的向topic(data_id) 推送消息
            tokio::spawn(async move {
                let mut stream = IntervalStream::new(tokio::time::interval(duration));
                while let Some(_) = stream.next().await {
                    publisher.dyn_clone().publish(&vec![]).expect(&format!(
                        "timer {duration_output} failed to publish timer tick message"
                    ));
                    debug!("timer {} publish success", duration_output);
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
