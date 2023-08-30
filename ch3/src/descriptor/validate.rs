use crate::{adjust_executable_target_path, adjust_shared_library_path, source_is_url};

use super::descriptor::{
    DataId, Deploy, Descriptor, Input, InputMapping, NormalNode, OperatorId, OperatorSource,
    UserInputMapping,
};
use anyhow::{anyhow, bail, Context, Result};
use std::{path::Path, process::Command};

/// 处理url，进行网络请求检查
fn resolve_url(url: &str) -> Result<()> {
    let client = reqwest::blocking::Client::new();
    let response = client.head(url).send()?;
    // let response = reqwest::Client::builder().build()?.head(url).send().await?;
    if !response.status().is_success() {
        bail!("`{}` is not a valid URL.", url);
    } else {
        Ok(())
    }
}

/// 校验dataflow
/// 当build为True时，表示需要进行build，所以对于可执行文件的检查可以放宽
pub(crate) fn validate_dataflow(
    dataflow: &Descriptor,
    working_dir: &Path,
    build: bool,
) -> Result<()> {
    let nodes = dataflow.resolve_node_defaults();
    // 检查描述文件的 deploy
    validate_deploy(dataflow.deploy.clone())
        .with_context(|| anyhow!("dataflow deploy validate error"))?;
    for node in &nodes {
        // 检查每一个节点的 deploy
        validate_deploy(node.deploy.clone())
            .with_context(|| anyhow!("node {:?} deploy validate error", node.id))?;

        // 对每一个节点的每一个op进行校验
        for operator_definition in &node.kind.operators {
            // 检查每一个op 的source 是否存在
            validate_source(&operator_definition.config.source, working_dir, build).with_context(
                || {
                    anyhow!(
                        "failed to check source:{:?} ,work dir is:{:?}",
                        operator_definition.config.source,
                        working_dir
                    )
                },
            )?;
            // 检查每一个op 的inputs 是否存在
            for (input_id, input) in &operator_definition.config.run_config.inputs {
                validate_input(
                    input,
                    &nodes,
                    &format!("{}/{}/{input_id}", operator_definition.id, node.id),
                )?;
            }
        }
    }

    Ok(())
}

/// 检查deploy
fn validate_deploy(deploy: Deploy) -> Result<()> {
    if let Some(endpoints) = deploy.endpoints {
        if endpoints.is_empty() {
            return Err(anyhow!("node has no endpoints defined"));
        }
    } else {
        return Err(anyhow!("node has no endpoints defined"));
    }
    Ok(())
}
/// 检查各种source是否存在
/// build 如果为True，说明需要进行build，所以对于可执行文件的检查可以放宽
fn validate_source(source: &OperatorSource, working_dir: &Path, build: bool) -> Result<()> {
    match source {
        OperatorSource::SharedLibrary(path) => {
            if source_is_url(path) {
                resolve_url(&path)
                    .with_context(|| format!("Could not find shared library url `{}`", path))?;
            } else {
                // 调整共享库lib的路径，再判断是否存在
                let path = adjust_shared_library_path(Path::new(&path))?;
                if !working_dir.join(&path).exists() && !build {
                    bail!("no shared library at `{}`", path.display());
                }
            }
        }
        OperatorSource::PythonModule(path) => {
            if source_is_url(path) {
                resolve_url(&path)
                    .with_context(|| format!("Could not find Python library url `{}`", path))?;
            } else if !working_dir.join(path).exists() && !build {
                bail!("no Python library at `{path}`");
            }
        }
        OperatorSource::WasmModule(path) => {
            if source_is_url(path) {
                resolve_url(&path)
                    .with_context(|| format!("Could not find WASM library url `{}`", path))?;
            } else if !working_dir.join(path).exists() && !build {
                bail!("no WASM library at `{path}`");
            }
        }
        OperatorSource::Shell(shell) => {
            // 如果是shell
            // 首先进行split with 空格，然后获取第一个命令作为first_cmd
            let first_cmd = shell
                .trim()
                .split_ascii_whitespace()
                .next()
                .with_context(|| format!("Could not get exec part from shell: `{}`", shell))?;
            // 然后根据不同的的操作系统选择不同的命令判断first command 是否可用
            // 这里还涉及到一个work dir的操作
            let mut command = if cfg!(target_os = "windows") {
                Command::new("where")
            } else {
                Command::new("which")
            };
            // 执行命令，并且获取状态
            let status = command
                .arg(first_cmd)
                .status()
                .with_context(|| format!("Can not exec command: `{}`", first_cmd))?;

            if !status.success() && !build {
                bail!("Could not find first command: `{first_cmd}` of shell: `{shell}`");
            }
        }
        OperatorSource::ExeTarget(target) => {
            if source_is_url(target) {
                resolve_url(&target)
                    .with_context(|| format!("Could not find target url `{}`", target))?;
            } else {
                // build 为 true时，说明，后面会进行build，只需要判断是否是一个合法的target的名字即可
                // 这里，假如build为True，并且target中没有空格，那么就不需要进行判断
                // 如果build为false 或者 target中有空格，那么就需要进行判断
                // 如果build为false, 且target不存在，就会异常
                // 如果build为true, 但是target中有空格，就会异常，这时target path没有，也会
                if !build || !target.chars().all(|c| !c.is_whitespace()) {
                    // 调整可执行文件的路径，再判断是否存在
                    let path = adjust_executable_target_path(Path::new(&target))?;
                    if !working_dir.join(&path).exists() {
                        format!("Could not find target path `{target}`");
                        bail!("no executable target at `{}`", path.display());
                    }
                }
            }
        }
    }
    Ok(())
}

/// 检查各种input是否存在
fn validate_input(input: &Input, nodes: &[NormalNode], input_id_str: &str) -> Result<()> {
    match &input.mapping {
        InputMapping::Timer { interval: _ } => {}
        InputMapping::User(UserInputMapping { source, output }) => {
            // 根据 source 从 nodes中找到对应的节点
            let source_node = nodes.iter().find(|n| &n.id == source).ok_or_else(|| {
                anyhow!("source node `{source}` mapped to input `{input_id_str}` does not exist",)
            })?;
            // 根据 output 从 source_node 中找到对应 operator 的 output
            let (operator_id, output) = output.split_once('/').unwrap_or_default();
            let operator_id = OperatorId::from(operator_id.to_owned());
            let output = DataId::from(output.to_owned());

            let operator = source_node
                .kind
                .operators
                .iter()
                .find(|o| o.id == operator_id)
                .ok_or_else(|| {
                    anyhow!(
                        "source operator `{source}/{operator_id}` used \
                        for input `{input_id_str}` does not exist",
                    )
                })?;
            // 如果找不到就添加异常到上下文
            if !operator.config.run_config.outputs.contains(&output) {
                bail!(
                    "output `{source}/{operator_id}/{output}` mapped to \
                    input `{input_id_str}` does not exist",
                );
            }
        }
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_create() {
        let cargo_path = env!("CARGO_MANIFEST_DIR");
        println!("cargo_path>>> {:?}", cargo_path);
        let dataflow = PathBuf::from(cargo_path.to_string() + "\\example.yaml");
        let des = Descriptor::blocking_read(&dataflow).unwrap();
        println!("Descriptor>>> {:#?}", des);
        println!("\n\n\n");
        let binding = dataflow.canonicalize().unwrap();
        let working_dir = binding.parent().unwrap();
        println!("working_dir>>> {:#?}", working_dir);
        println!("\n\n\n");
        validate_dataflow(&des, &working_dir, true).unwrap();
    }
}
