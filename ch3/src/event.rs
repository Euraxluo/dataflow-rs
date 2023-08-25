#[derive(Debug)]
pub enum Event {
    // NewDaemonConnection(TcpStream),
    // DaemonConnectError(eyre::Report),
    // DaemonHeartbeat { machine_id: String },
    // Dataflow { uuid: Uuid, event: DataflowEvent },
    // Control(ControlEvent),
    // Daemon(DaemonEvent),
    // DaemonHeartbeatInterval,
    CtrlC,
    Logged,
}
