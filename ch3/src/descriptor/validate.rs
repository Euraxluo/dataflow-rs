use super::descriptor::{
    DataId, Descriptor, Input, InputMapping, NormalNode, OperatorId, OperatorSource,
    UserInputMapping,
};
use anyhow::{anyhow, bail, Context, Result};
use std::{
    env::consts::{DLL_PREFIX, DLL_SUFFIX, EXE_EXTENSION},
    path::{Path, PathBuf},
};

/// 判断source 来源是不是url 类型
fn source_is_url(path: &str) -> bool {
    path.starts_with("http://") || path.starts_with("https://")
}

/// 处理path
fn resolve_path(source: &str, working_dir: &Path) -> Result<PathBuf> {
    let path = Path::new(&source);
    let path = if path.extension().is_none() {
        path.with_extension(EXE_EXTENSION)
    } else {
        path.to_owned()
    };

    // Search path within current working directory
    if let Ok(abs_path) = working_dir.join(&path).canonicalize() {
        Ok(abs_path)
    // Search path within $PATH
    } else if let Ok(abs_path) = which::which(&path) {
        Ok(abs_path)
    } else {
        bail!("Could not find source path {}", path.display())
    }
}

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

/// 调整共享库的路径
fn adjust_shared_library_path(path: &Path) -> Result<std::path::PathBuf> {
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow!("shared library path has no file name"))?
        .to_str()
        .ok_or_else(|| anyhow!("shared library file name is not valid UTF8"))?;

    if file_name.starts_with("lib") {
        return Err(anyhow!(
            "Shared library file name must not start with `lib`, prefix is added automatically"
        ));
    }

    if path.extension().is_some() {
        return Err(anyhow!(
            "Shared library file name must have no extension, it is added automatically"
        ));
    }

    let library_filename = format!("{DLL_PREFIX}{file_name}{DLL_SUFFIX}");

    let path = path.with_file_name(library_filename);
    Ok(path)
}

/// 校验dataflow
pub(crate) fn check_dataflow(dataflow: &Descriptor, working_dir: &Path) -> Result<()> {
    let nodes = dataflow.resolve_node_defaults();
    for node in &nodes {
        // 对每一个节点的每一个op进行校验
        for operator_definition in &node.kind.operators {
            // 检查每一个op 的source 是否存在
            check_source(&operator_definition.config.source, working_dir).with_context(|| {
                anyhow!(
                    "failed to check source:{:?} ,work dir is:{:?}",
                    operator_definition.config.source,
                    working_dir
                )
            })?;
            // 检查每一个op 的inputs 是否存在
            for (input_id, input) in &operator_definition.config.run_config.inputs {
                check_input(
                    input,
                    &nodes,
                    &format!("{}/{}/{input_id}", operator_definition.id, node.id),
                )?;
            }
        }
    }

    Ok(())
}

/// 检查各种source是否存在
fn check_source(source: &OperatorSource, working_dir: &Path) -> Result<()> {
    match source {
        OperatorSource::SharedLibrary(path) => {
            if source_is_url(path) {
                resolve_url(&path)
                    .with_context(|| format!("Could not find shared library url `{}`", path))?;
            } else {
                // 调整共享库lib的路径，再判断是否存在
                let path = adjust_shared_library_path(Path::new(&path))?;
                if !working_dir.join(&path).exists() {
                    bail!("no shared library at `{}`", path.display());
                }
            }
        }
        OperatorSource::Python(path) => {
            if source_is_url(path) {
                resolve_url(&path)
                    .with_context(|| format!("Could not find Python library url `{}`", path))?;
            } else if !working_dir.join(path).exists() {
                bail!("no Python library at `{path}`");
            }
        }
        OperatorSource::Wasm(path) => {
            if source_is_url(path) {
                resolve_url(&path)
                    .with_context(|| format!("Could not find WASM library url `{}`", path))?;
            } else if !working_dir.join(path).exists() {
                bail!("no WASM library at `{path}`");
            }
        }
        OperatorSource::Shell(shell) => {
            if source_is_url(shell) {
                resolve_url(&shell)
                    .with_context(|| format!("Could not find shell url `{}`", shell))?;
            } else {
                resolve_path(shell, working_dir)
                    .with_context(|| format!("Could not find shell path `{}`", shell))?;
            }
        }
    }
    Ok(())
}

/// 检查各种input是否存在
fn check_input(input: &Input, nodes: &[NormalNode], input_id_str: &str) -> Result<()> {
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
        let dataflow = PathBuf::from(cargo_path.to_string() + "\\example2.yaml");
        let des = Descriptor::blocking_read(&dataflow).unwrap();
        println!("Descriptor>>> {:#?}", des);
        println!("\n\n\n");
        let binding = dataflow.canonicalize().unwrap();
        let working_dir = binding.parent().unwrap();
        println!("working_dir>>> {:#?}", working_dir);
        println!("\n\n\n");
        check_dataflow(&des, &working_dir).unwrap();
    }
}
