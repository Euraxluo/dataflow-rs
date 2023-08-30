use anyhow::{anyhow, Context, Result};
use descriptor::descriptor::Descriptor;
use event::Event;
use flume::{bounded, Receiver};
use log::{error, info};
#[cfg(unix)]
use std::os::unix::prelude::PermissionsExt;
use std::{
    env::consts::{DLL_PREFIX, DLL_SUFFIX, EXE_SUFFIX},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::io::AsyncWriteExt;
pub mod cli;
pub mod communication;
pub mod descriptor;
pub mod event;
pub mod launch;
pub mod runtime;

/// 用于存储数据流描述文件的环境变量
pub const DATAFLOW_DESCRIPTION_ENV: &str = "DATAFLOW_DESCRIPTION";
/// 从环境变量中读取数据流描述文件内容
pub fn dataflow_description_from_env() -> Result<Descriptor> {
    let descriptor: Descriptor = {
        let raw = std::env::var(DATAFLOW_DESCRIPTION_ENV).context(format!(
            "env variable {DATAFLOW_DESCRIPTION_ENV} must be set"
        ))?;
        serde_yaml::from_str(&raw).context("failed to deserialize description descriptor")?
    };

    Ok(descriptor)
}

/// ctrlc 信号处理器
pub fn ctrlc_handler() -> Result<Receiver<Event>> {
    // 创建一个有界通道，用于发送和接收 Ctrl+C 信号
    let (ctrlc_tx, ctrlc_rx) = bounded(1);

    // 标记是否已经发送过 Ctrl+C 信号
    let ctrlc_sent = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ctrlc_sent_clone = ctrlc_sent.clone();

    // 设置 Ctrl+C 信号的处理函数
    ctrlc::set_handler(move || {
        if ctrlc_sent_clone.swap(true, std::sync::atomic::Ordering::SeqCst) {
            // 如果已经收到过 Ctrl+C 信号，则立即终止程序
            error!("received second ctrlc signal -> aborting immediately");
            std::process::abort();
        } else {
            // 否则，发送 Ctrl+C 事件到通道中
            error!("received ctrlc signal");
            if let Err(e) = ctrlc_tx.send(Event::CtrlC) {
                error!("failed to report ctrl-c event to flume channel: {:?}", e);
            }
        }
    })?;

    // 创建一个异步流，用于监听 Ctrl+C 事件
    Ok(ctrlc_rx)
}

/// 调整共享库的路径
pub(crate) fn adjust_shared_library_path(path: &Path) -> Result<PathBuf> {
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

/// 调整可执行目标的路径
pub(crate) fn adjust_executable_target_path(path: &Path) -> Result<std::path::PathBuf> {
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow!("shared library path has no file name"))?
        .to_str()
        .ok_or_else(|| anyhow!("shared library file name is not valid UTF8"))?;

    if path.extension().is_some() {
        return Err(anyhow!(
            "Executable Target file name must have no extension, it is added automatically"
        ));
    }
    let target_filename = format!("{file_name}{EXE_SUFFIX}");
    let path = path.with_file_name(target_filename);
    Ok(path)
}

/// 判断source 来源是不是url 类型
pub(crate) fn source_is_url(path: &str) -> bool {
    path.starts_with("http://") || path.starts_with("https://")
}

/// 从url下载文件到target_path
pub(crate) async fn download_file<T>(url: T, target_path: &Path) -> Result<()>
where
    T: reqwest::IntoUrl + std::fmt::Display + Copy,
{
    if target_path.exists() {
        info!("Using cache: {:?}", target_path.to_str());
        return Ok(());
    }

    if let Some(parent) = target_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .context("failed to create parent folder")?;
    }

    let response = reqwest::get(url)
        .await
        .with_context(|| format!("failed to request operator from `{url}`"))?
        .bytes()
        .await
        .context("failed to read operator from `{uri}`")?;
    let mut file = tokio::fs::File::create(target_path)
        .await
        .context("failed to create target file")?;
    file.write_all(&response)
        .await
        .context("failed to write downloaded operator to file")?;
    file.sync_all().await.context("failed to `sync_all`")?;

    #[cfg(unix)]
    file.set_permissions(std::fs::Permissions::from_mode(0o764))
        .await
        .context("failed to make downloaded file")?;

    Ok(())
}
