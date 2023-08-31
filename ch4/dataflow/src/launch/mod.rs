pub mod build;
pub mod node;
use crate::{
    descriptor::descriptor::{Descriptor, NormalNode},
    event::Event,
    runtime::timer::{self},
    DATAFLOW_DESCRIPTION_ENV,
};
use anyhow::{anyhow, Context, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use log::{debug, error, info};
use regex::Regex;
use serde_yaml;
use std::{env, path::PathBuf, process::Stdio};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader},
};

/// 根据描述文件启动所有的节点
pub async fn launch(dataflow: PathBuf, build: bool) -> Result<()> {
    info!("Launch DataFlow");
    // 读取描述文件并解析
    let descriptor = Descriptor::blocking_read(&dataflow).with_context(|| {
        format!(
            "launch dataflow failed to read dataflow at `{}`",
            dataflow.display()
        )
    })?;
    // 获取描述文件定义的的工作目录
    let working_dir = dataflow
        .canonicalize()
        .context("launch dataflow failed to canonicalize dataflow path")?
        .parent()
        .ok_or_else(|| anyhow!("launch dataflow failed that dataflow path has no parent dir"))?
        .to_owned();
    // 对描述文件进行校验
    descriptor
        .validate(&working_dir, build)
        .context("launch dataflow failed to validate dataflow")?;
    // 处理所有节点的默认值
    let nodes = descriptor.resolve_node_defaults();
    // 启动所有的节点
    launch_nodes(&nodes, &descriptor, &working_dir, build).await?;
    info!("Launch Nodes Success");
    Ok(())
}

/// 启动所有的节点
async fn launch_nodes(
    nodes: &Vec<NormalNode>,
    descriptor: &Descriptor,
    working_dir: &PathBuf,
    build: bool,
) -> Result<()> {
    info!("Launch Nodes");
    timer::start(&nodes, &descriptor.deploy).await?;

    let mut tasks = FuturesUnordered::new();
    for node in nodes {
        let node_id = node.id.clone();
        let result = spawn_node(node.clone(), descriptor, working_dir, build)
            .await
            .with_context(|| format!("launch nodes failed to spawn runtime node {node_id}"))?;
        tasks.push(result);
    }

    while let Some(task_result) = tasks.next().await {
        if let Err(e) = task_result {
            error!("launch nodes failed to join one async task of nodes: {}", e);
        }
    }

    info!("Launch Nodes Success");
    Ok(())
}

/// 开启子进程执行节点启动任务
/// 开启两个异步任务分别处理标准输出和标准错误
/// 再开启一个异步任务
async fn spawn_node(
    node: NormalNode,
    descriptor: &Descriptor,
    working_dir: &PathBuf,
    build: bool,
) -> Result<tokio::task::JoinHandle<Result<()>>> {
    debug!("Spawn Node log: {:#?}", node.deploy.log);
    debug!(
        "Spawn Node descriptor: {:#?}",
        serde_yaml::to_string(descriptor).context("failed to serialize descriptor")?
    );
    // 执行的就是当前的exe，只不过设置了不同的参数
    let mut command = tokio::process::Command::new(
        env::current_exe().context("faild to get current exe when spawn node")?,
    );
    // 将描述文件写入环境变量，然后参数就只用设置运行的节点id
    command.env(
        DATAFLOW_DESCRIPTION_ENV,
        serde_yaml::to_string(descriptor).context("failed to serialize descriptor")?,
    );
    command.args([
        "start",
        "--node",
        node.id.as_str(),
        if build == true { "--build" } else { "" },
    ]);
    // 因为是通过环境变量设置的descriptor，所以这里还需要设置该命令的工作目录为描述文件的工作目录
    command.current_dir(working_dir);

    debug!("Spawn Node command: {:#?}", command);
    // 启动一个子进程，这里设置了子进程的标准输入输出，可以在后面通过当前进程获取子进程的标准输入输出来控制子进程
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context(format!("failed to run node {}", node.id))?;

    // 日志通道
    let (tx, log_message_rx) = flume::bounded(10);

    // 子进程标准输入的处理
    let child_stdout = BufReader::new(child.stdout.take().expect("failed to take stdout"));
    let log_file_path = node.deploy.log.clone().unwrap();
    let node_id_clone = node.id.clone();
    let stdout_tx = tx.clone();
    tokio::spawn(async move {
        let log_level_pattern: Regex =
            Regex::new(r"TRACE|INFO|DEBUG|WARN|ERROR").expect("failed to create regex");
        child_log_writer(
            node_id_clone.as_str(),
            log_file_path.to_str().unwrap(),
            child_stdout,
            stdout_tx,
            move |buffer| log_level_pattern.is_match(buffer) && !buffer.ends_with("\n\n"),
        )
        .await
        .expect("failed to handler stdout to log")
    });

    // 子进程标准错误输入的处理
    let child_stderr = BufReader::new(child.stderr.take().expect("failed to take stderr"));
    let log_file_path = node.deploy.log.clone().unwrap();
    let node_id_clone = node.id.clone();
    let stderr_tx = tx.clone();
    tokio::spawn(async move {
        child_log_writer(
            node_id_clone.as_str(),
            log_file_path.to_str().unwrap(),
            child_stderr,
            stderr_tx,
            |buffer| buffer.starts_with("Traceback"),
        )
        .await
        .expect("failed to handler stderr to log")
    });

    // 事件通道
    let (event_tx, event_rx) = flume::bounded(1);

    // 日志落盘的异步任务
    let node_id_clone = node.id.clone();
    let log_file_path = node.deploy.log.clone().unwrap();
    tokio::spawn(async move {
        write_logs_to_file(
            node_id_clone.as_str(),
            log_file_path,
            log_message_rx,
            event_tx,
        )
        .await
        .expect("failed to write logs to file")
    });

    // 等待子进程结束的异步任务
    let node_id_clone = node.id.clone();
    let result = tokio::spawn(async move {
        let status = child.wait().await.context("child process failed")?;
        if status.success() {
            info!("node {} finished", node_id_clone);
            match event_rx.recv_async().await {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow!(
                    "runtime node {} event failed receive: {}",
                    node_id_clone,
                    e
                )), // 在格式化字符串中使用变量
            }
        } else if let Some(code) = status.code() {
            Err(anyhow!(
                "runtime node {} failed with exit code: {}",
                node_id_clone,
                code // 在格式化字符串中使用变量
            ))
        } else {
            Err(anyhow!(
                "runtime node {} failed (unknown exit code)",
                node_id_clone // 在格式化字符串中使用变量
            ))
        }
    });
    Ok(result)
}

/// 子进程的标准输出流管理，将流读取到`String buffer`中，按照`should_continue`的条件，选择将其发送到`tx`channel中
async fn child_log_writer<F, T>(
    node_id: &str,
    log: &str,
    mut child_stream: tokio::io::BufReader<T>,
    tx: flume::Sender<String>,
    should_continue: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Fn(&str) -> bool + Send + 'static,
    T: AsyncRead + Unpin,
{
    let mut buffer = String::new();
    while let Ok(bytes_read) = child_stream.read_line(&mut buffer).await {
        if bytes_read == 0 {
            break;
        }
        if should_continue(&buffer) {
            continue;
        }

        if let Err(e) = tx.send_async(buffer.clone()).await {
            error!("Failed to send logs from {} to {:?}: {}", node_id, log, e);
        }

        buffer.clear();
    }

    Ok(())
}

/// 将日志写入到文件中，从`log_message_rx`中接受msg，and write it to `log_file_path`
async fn write_logs_to_file(
    node_id: &str,
    log_file_path: std::path::PathBuf,
    log_message_rx: flume::Receiver<String>,
    event_rx: flume::Sender<Event>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut log_file = File::create(log_file_path)
        .await
        .context("Failed to create log file")?;
    while let Ok(message) = log_message_rx.recv_async().await {
        log_file.write_all(message.as_bytes()).await?;
        let formatted: String = message.lines().map(|l| format!("      {l}\n")).collect();
        debug!("{} logged:\n{formatted}", node_id);
        log_file.sync_all().await?;
        log_file.flush().await?;
    }
    // 发送日志写入完成的事件
    event_rx.send_async(Event::Logged).await.context(anyhow!(
        "Could not sender Logged Event from write_logs_to_file thread"
    ))?;
    Ok(())
}
