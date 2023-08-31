use std::{collections::BTreeMap, path::PathBuf};

use crate::{
    descriptor::descriptor::{Deploy, NormalNode},
    runtime::actuator::executor,
};
use anyhow::{Context, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use log::{error, info};

/// 启动一个节点，拉起多个操作节点，并在此进行控制
pub async fn start(node: &NormalNode, deploy: &Deploy, working_dir: &PathBuf) -> Result<()> {
    info!("Start Node {:#?} ", node.id);

    let mut tasks = FuturesUnordered::new();

    for operator in &node.kind.operators {
        let mut operator_clone = operator.clone();
        operator_clone.config.name = Some(format!(
            "{}-{}",
            node.name.as_ref().unwrap_or(&node.id.to_string()),
            operator_clone
                .config
                .name
                .as_ref()
                .unwrap_or(&operator_clone.id.to_string())
        ));
        operator_clone.config.description = Some(format!(
            "{}-{}",
            node.description.as_ref().unwrap_or(&node.id.to_string()),
            operator
                .config
                .description
                .as_ref()
                .unwrap_or(&operator.id.to_string())
        ));
        if let Some(envs) = &node.envs {
            for (k, v) in envs.iter() {
                operator_clone
                    .config
                    .envs
                    .get_or_insert_with(BTreeMap::new)
                    .insert(k.clone(), v.clone());
            }
        }
        let result = executor(&operator_clone, deploy, working_dir)
            .await
            .with_context(|| {
                format!(
                    "start node failed to spawn operator {operator_id}",
                    operator_id = operator.id
                )
            })?;
        tasks.push(result);
    }
    while let Some(task_result) = tasks.next().await {
        if let Err(e) = task_result {
            error!(
                "start node failed to join one async task of operators: {}",
                e
            );
        }
    }
    info!("Start Nodes Success");
    Ok(())
}
