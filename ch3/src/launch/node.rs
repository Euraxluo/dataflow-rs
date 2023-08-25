use crate::{
    dataflow_description_from_env,
    descriptor::descriptor::{Descriptor, NormalNode},
    runtime::timer::{self, TIMER_NODE_ID},
};
use anyhow::{anyhow, Context, Result};
use log::{debug, info};

use std::{env, path::PathBuf};

/// 根据描述文件及节点id启动执行的节点
/// 其中dataflow 和 working_dir 两个参数是可选的,并且最少传递一个
pub async fn start(
    // 描述文件路径,不传的话会尝试从环境变量获取
    dataflow: Option<PathBuf>,
    // 待启动的节点id
    node_id: String,
) -> Result<()> {
    info!(
        "Start Node dataflow: {:#?} node_id: {:?}",
        dataflow, node_id
    );
    // 获取描述文件和工作目录
    let (descriptor, working_dir) = match dataflow {
        Some(path) => {
            // 读取描述文件并解析
            let descriptor = Descriptor::blocking_read(&path)
                .with_context(|| format!("failed to read dataflow at `{}`", path.display()))?;
            // 获取描述文件定义的的工作目录
            let working_dir = path
                .canonicalize()
                .context("failed to canonicalize dataflow path")?
                .parent()
                .ok_or_else(|| anyhow!("dataflow path has no parent dir"))?
                .to_owned();
            (descriptor, working_dir)
        }
        None => {
            let descripton =
                dataflow_description_from_env().context("failed to read dataflow from env")?;
            (
                descripton,
                PathBuf::from(
                    env::current_dir()
                        .context("failed to get working dir and can not get from dataflow")?,
                ),
            )
        }
    };

    // 对描述文件进行校验
    descriptor
        .validate(&working_dir)
        .context("failed to validate dataflow")?;

    // 处理所有节点的默认值
    let nodes = descriptor.resolve_node_defaults();
    debug!(
        "Start Node descriptor: {:?} node_id: {:?}",
        descriptor, node_id
    );
    // 根据node_id找到对应的节点
    match node_id.as_str() {
        TIMER_NODE_ID => {
            debug!("Launch TimerNode {:?}", TIMER_NODE_ID);
            // 启动定时器节点
            timer::start(&nodes, &descriptor.deploy.endpoints.unwrap()).await?;
        }
        _ => {
            // 找到我们需要处理的那个节点
            let node = nodes
                .iter()
                .find(|n| n.id.to_string() == node_id)
                .ok_or_else(|| anyhow!("node with id `{}` not found in dataflow ", node_id))?;
            debug!("Launch Node {:?}", node.id);
            launch_node(node).await?;
        }
    }
    Ok(())
}

/// 运行节点
/// 对于每一个节点都spawn一个进程
async fn launch_node(node: &NormalNode) -> Result<tokio::task::JoinHandle<Result<()>>> {
    let result = tokio::spawn(async move { Ok(()) });
    Ok(result)
}
