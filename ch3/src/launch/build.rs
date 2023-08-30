use std::path::{Path, PathBuf};

use crate::descriptor::descriptor::NormalNode;
use anyhow::{anyhow, Context, Result};
use log::{debug, info};
use tokio::process::Command;

/// 构建一个节点，执行该节点所有operator的构建命令
pub async fn build(node: &NormalNode, working_dir: &PathBuf) -> Result<()> {
    info!("Build Node {:#?} ", node.id);
    for operator in &node.kind.operators {
        debug!(
            "Build `{}/{}` Command: {:#?}",
            node.id,
            operator.id,
            operator.config.build.as_deref()
        );
        run_build_command(operator.config.build.as_deref(), working_dir)
            .await
            .with_context(|| {
                format!(
                    "build command failed for operator `{}/{}`",
                    node.id, operator.id
                )
            })?;
    }
    info!("Build Node Success");
    Ok(())
}

/// 运行构建的命令
async fn run_build_command(build: Option<&str>, working_dir: &Path) -> Result<()> {
    if let Some(build) = build {
        let mut split = build.split_whitespace();
        let mut cmd = Command::new(
            split
                .next()
                .ok_or_else(|| anyhow!("build command is empty"))?,
        );
        cmd.args(split);
        cmd.current_dir(working_dir);
        let exit_status = cmd
            .status()
            .await
            .with_context(|| format!("failed to run `{}`", build))?;
        if exit_status.success() {
            Ok(())
        } else {
            Err(anyhow!("build command returned an error code"))
        }
    } else {
        Ok(())
    }
}
