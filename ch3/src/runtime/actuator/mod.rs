use std::{
    env,
    path::{Path, PathBuf},
};

use crate::descriptor::descriptor::{Deploy, NormalOperatorDefinition, OperatorSource};
use anyhow::Result;
use log::debug;

use self::{exe_target::ExeTarget, shell::Shell};

pub mod exe_target;
pub mod python_module;
pub mod shared_library;
pub mod shell;
pub mod wasm_module;

/// 执行器trait
pub(crate) trait OperatorActuator {
    fn execute(&mut self, working_dir: &PathBuf) -> Result<tokio::task::JoinHandle<Result<()>>>;
}

/// 构造operator的执行器
pub(crate) async fn executor(
    operator: &NormalOperatorDefinition,
    deploy: &Deploy,
    working_dir: &PathBuf,
) -> Result<tokio::task::JoinHandle<Result<()>>> {
    let mut child: Box<dyn OperatorActuator> = match &operator.config.source {
        // 可执行文件
        OperatorSource::ExeTarget(_) => Box::new(ExeTarget(operator.clone())),
        OperatorSource::Shell(_) => Box::new(Shell(operator.clone())),
        OperatorSource::PythonModule(_) => todo!(),
        OperatorSource::SharedLibrary(_) => todo!(),
        OperatorSource::WasmModule(_) => todo!(),
    };
    debug!("OperatorActuator {} execute", operator.id);
    child.execute(working_dir)
}
