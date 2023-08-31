use std::{
    env,
    path::{Path, PathBuf},
};

use crate::{
    adjust_executable_target_path,
    descriptor::descriptor::{Deploy, NormalOperatorDefinition, OperatorSource},
    download_file, source_is_url,
};
use anyhow::{anyhow, Context, Result};
use tokio::process::Command;

use super::OperatorActuator;

// 定义一个结构体来作为执行类型
pub(crate) struct PythonModule(pub NormalOperatorDefinition);

impl OperatorActuator for PythonModule {
    fn execute(&mut self, working_dir: &PathBuf) -> Result<tokio::task::JoinHandle<Result<()>>> {
        todo!()
    }
}
