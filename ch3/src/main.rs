use anyhow::Result;
use ch3::{
    cli::{Args, Command},
    ctrlc_handler,
    descriptor::visualize::visualize,
    event::Event,
    launch::{launch, node::start},
};
use clap::Parser;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    args.init_log();
    match args.command {
        // 对描述文件进行可视化
        Command::Show {
            dataflow,
            mermaid,
            open,
        } => visualize(dataflow, mermaid, open)?,
        // launch 所有的进程
        Command::Launch { dataflow, build } => launch(dataflow, build).await?,
        // 启动一个节点
        Command::Start {
            dataflow,
            node,
            build,
        } => start(dataflow, node, build).await?,
    }

    // 在主线程中，等待并监听 Ctrl+C 事件
    let ctrlc_rx = ctrlc_handler()?;
    let mut ctrlc_stream = ctrlc_rx.into_stream();
    while let Some(event) = ctrlc_stream.next().await {
        match event {
            Event::CtrlC => {
                break;
            }
            _ => todo!(),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_start() {
        start(
            Some(PathBuf::from("./demo.yaml")),
            // "python_source_image".into(),
            "dataflow/timer".into(),
            false,
        )
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn test_launch() {
        launch(PathBuf::from("./demo.yaml"), true).await.unwrap();
    }
}
