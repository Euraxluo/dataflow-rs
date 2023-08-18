use anyhow::Result;
use ch3::visualize;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

///dataflow command line tool
#[derive(Parser)]
#[command(author, bin_name = "dataflow", version, about)]
struct Command {
    /// Print Graphviz representation of the given descriptor file
    #[command(subcommand)]
    graph: Graph,
}

#[derive(Subcommand)]
enum Graph {
    /// show dataflow file as mermaid graph
    Show {
        /// yaml file path
        #[arg(short, long, value_name = "FILE")]
        file: PathBuf,
        /// 这里使用 conflicts_with表示和open互斥
        #[clap(short, long, action, conflicts_with = "open")]
        mermaid: bool,
        /// 这里使用 conflicts_with表示和mermaid互斥
        #[clap(short, long, action, conflicts_with = "mermaid")]
        open: bool,
    },
}

fn main() -> Result<()> {
    let command = Command::parse();
    match command.graph {
        Graph::Show {
            file,
            mermaid,
            open,
        } => {
            visualize::create(file, mermaid, open).unwrap();
        }
    }

    Ok(())
}
