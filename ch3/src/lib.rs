use anyhow::{Context, Ok, Result};
use descriptor::descriptor::Descriptor;
use event::Event;
use flume::{bounded, Receiver};
use log::error;
use std::sync::Arc;

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
