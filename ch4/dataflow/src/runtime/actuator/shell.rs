use std::path::PathBuf;

use crate::descriptor::descriptor::NormalOperatorDefinition;
use anyhow::{anyhow, Context, Result};
use log::debug;
use tokio::process::Command;

use super::OperatorActuator;

// 定义一个结构体来作为执行类型
pub(crate) struct Shell(pub NormalOperatorDefinition);
impl OperatorActuator for Shell {
    fn execute(&mut self, working_dir: &PathBuf) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let operator_id = self.0.id.to_string();
        let shell = self.0.config.source.to_string();
        let args = self
            .0
            .config
            .args
            .as_ref()
            .map(String::as_str)
            .unwrap_or_default();
        let mut shell_cmd = Command::new(
            if cfg!(target_os = "windows") {
                "cmd"
            } else {
                "sh"
            }
        );
        shell_cmd.args(["/C", shell.as_str(), args]);

        if let Some(envs) = &self.0.config.envs {
            for (k, v) in envs.iter() {
                shell_cmd.env(k.as_str(), v.to_string().as_str());
            }
        }
        shell_cmd.current_dir(working_dir);
        debug!("OperatorActuator Shell exec command {:?}", shell_cmd);

        let mut child = shell_cmd
            .spawn()
            .with_context(|| format!("failed to run command `{}`", shell))?;
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
