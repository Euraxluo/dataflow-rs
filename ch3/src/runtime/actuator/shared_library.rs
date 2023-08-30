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

pub(crate) struct SharedLibrary(pub NormalOperatorDefinition);
impl OperatorActuator for SharedLibrary {
    fn execute(&mut self, working_dir: &PathBuf) -> Result<tokio::task::JoinHandle<Result<()>>> {
        todo!()
    }
}
