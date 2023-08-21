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
    /// Print Graphviz representation of the given descriptor file or Show dataflow file as mermaid graph. Use --open to open browser.
    Show {
        /// yaml file path
        #[arg(short, long, value_name = "FILE")]
        file: PathBuf,
        /// 输出 mermaid 内容和 open互斥
        #[clap(short, long, action, conflicts_with = "open")]
        mermaid: bool,
        /// 打开浏览器查看 mermaid， mermaid互斥
        #[clap(short, long, action, conflicts_with = "mermaid")]
        open: bool,
    },
    /// Start the given dataflow path. Attach a name to the running dataflow by using --name.
    Start {
        dataflow: PathBuf,
        #[clap(long)]
        name: Option<String>,
        #[clap(long, action)]
        attach: bool,
        #[clap(long, action)]
        hot_reload: bool,
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
            println!("RUST_ENV_LOG: {}", level_env);
            let level_verbose = match self.verbose {
                0 => "ERROR",
                1 => "INFO",
                2 => "Debug",
                _ => "Trace",
            };
            println!("RUST_VERBOSE_LOG: {}", level_verbose);
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
            println!("RUST_LOG: {}", level);
            level
        }))
        .init();
    }
}
