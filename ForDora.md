
# 以下内容的假设是读者是dora-rs的早期使用者或者开发者

首先我们先了解dora的架构

for install:

```
cargo install dora-cli
alias dora='dora-cli'
cargo install dora-coordinator
cargo install dora-daemon
pip install dora-rs ## For Python API

dora --help
```
dora-deamon 用于运行runtime

其中 dora-coordinator 用于运行 coordinator，他会开启一个TCP socket，监听DORA_COORDINATOR_PORT_DEFAULT 定义的端口
它通过feature中 运行 start_inner，
它获取请求，在 handle_requests 中 进行处理，然后得到
根据Event：
#[derive(Debug)]
pub enum Event {
    NewDaemonConnection(TcpStream),
    DaemonConnectError(eyre::Report),
    DaemonHeartbeat { machine_id: String },
    Dataflow { uuid: Uuid, event: DataflowEvent },
    Control(ControlEvent),
    Daemon(DaemonEvent),
    DaemonHeartbeatInterval,
    CtrlC,
}
根据接受到的event进行处理。

dora-cli up 启动 coordinator 和 deamon
up中是这样的，如果有session，那么就连接
没有的话就会创建一个  coordinator，如果要demand 也是直接启动
启动方式也简单，直接调用命令创建：dora-coordinator 及 dora-daemon




dora-cli new 用于创建一个项目模板
dora-cli run 用于运行一个项目，他会首先获取session，通过socket 连接  coordinator 获取
