use std::path::{Path, PathBuf};

use crate::{
    adjust_executable_target_path, descriptor::descriptor::NormalOperatorDefinition, download_file,
    source_is_url,
};
use anyhow::{anyhow, Context, Result};
use log::debug;
use tokio::process::Command;

use super::OperatorActuator;

// 定义一个结构体来作为执行类型
pub(crate) struct ExeTarget(pub NormalOperatorDefinition);

/// 为可执行目标实现 `OperatorActuator` trait
impl OperatorActuator for ExeTarget {
    fn execute(&mut self, working_dir: &PathBuf) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let operator_id = self.0.id.to_string();
        let target = self.0.config.source.to_string();
        let args = self
            .0
            .config
            .args
            .as_ref()
            .map(String::as_str)
            .unwrap_or_default();

        let mut target_cmd = Command::new(
            // 如果是url类型的source，需要先下载到本地
            if source_is_url(target.as_str()) {
                // 构造下载地址，
                let adjust_target_path =
                    adjust_executable_target_path(&Path::new("build").join(operator_id.as_str()))?;
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?
                    .block_on(download_file(
                        target.as_str(),
                        &working_dir.join(&adjust_target_path),
                    ))
                    .context("failed to download executable target operator")?;
                adjust_target_path
            } else {
                // 否则直接构造可执行文件路径
                adjust_executable_target_path(Path::new(&target))?
            },
        );
        if let Some(envs) = &self.0.config.envs {
            for (k, v) in envs.iter() {
                target_cmd.env(k.as_str(), v.to_string().as_str());
            }
        }
        // 设置工作目录和命令参数
        target_cmd.args(args.split_whitespace());
        target_cmd.current_dir(working_dir);

        debug!("OperatorActuator ExeTraget exec command {:?}", target_cmd);
        let mut child = target_cmd
            .spawn()
            .with_context(|| format!("failed to run command `{}`", target))?;
        let result = tokio::spawn(async move {
            let status = child.wait().await.context("child process failed")?;
            if status.success() {
                println!("operator {operator_id} finished");
                Ok(())
            } else if let Some(code) = status.code() {
                Err(anyhow!(
                    "operator {operator_id} failed with exit code: {code}"
                ))
            } else {
                Err(anyhow!("operator {operator_id} failed (unknown exit code)"))
            }
        });
        Ok(result)
    }
}
