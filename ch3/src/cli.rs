use clap::{ArgAction, Parser, Subcommand};
use env_logger::Env;
use log::Level;
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Debug, Parser)]
#[command(author, bin_name = "dataflow", version, about)]
pub struct Args {
    /// 子命令列表
    #[clap(subcommand)]
    pub command: Command,
    /// log level
    #[arg(short, long,action = ArgAction::Count)]
    verbose: u8,
}

///dataflow command line tool
#[derive(Debug, Subcommand)]
pub enum Command {
    /// 该命令是查看整个dataflow集群的数据流图的
    /// 应该包括静态的和动态的
    /// Print Graphviz representation of the given descriptor file or Show dataflow file as mermaid graph. Use --open to open browser.
    Show {
        /// yaml file dataflow path
        #[arg(short, long, value_name = "FILE")]
        dataflow: PathBuf,
        /// 输出 mermaid 内容和 open互斥
        #[clap(short, long, action, conflicts_with = "open")]
        mermaid: bool,
        /// 打开浏览器查看 mermaid， mermaid互斥
        #[clap(short, long, action, conflicts_with = "mermaid")]
        open: bool,
    },
    /// 该命令会启动一个dataflow
    /// Start the given dataflow path.
    Launch {
        /// yaml description file path
        #[arg(short, long, value_name = "FILE")]
        dataflow: PathBuf,
    },
    /// 该命令会启动指定dataflow中的一个节点
    /// Start one Node of a given dataflow path and given NodeId.
    Start {
        /// yaml description file path
        #[clap(short, long, value_name = "FILE")]
        dataflow: Option<PathBuf>,
        /// start a node
        #[arg(short, long, value_name = "NodeID")]
        node: String,
    },
}

impl Args {
    pub fn init_log(&self) {
        // The logging level is set through the environment variable RUST_LOG,
        // which defaults to the info level
        env_logger::Builder::from_env(Env::default().default_filter_or({
            let level_env = match std::env::var("RUST_LOG") {
                Ok(val) => val,
                Err(_) => "info".to_string(),
            };
            let level_verbose = match self.verbose {
                0 => "ERROR",
                1 => "INFO",
                2 => "Debug",
                _ => "Trace",
            };
            // Converts a string to the corresponding log-level enumeration
            let level = if let (Ok(level1), Ok(level2)) =
                (Level::from_str(&level_env), Level::from_str(&level_verbose))
            {
                // Use the cmp method to compare the sizes of the two log levels
                match level1.cmp(&level2) {
                    std::cmp::Ordering::Less => level2.to_string(),
                    std::cmp::Ordering::Equal => level2.to_string(),
                    std::cmp::Ordering::Greater => level1.to_string(),
                }
            } else {
                level_env
            };
            level
        }))
        .init();
    }
}
