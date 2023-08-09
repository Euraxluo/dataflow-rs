
use clap::{Parser,Subcommand};
use ch2::{descriptor::Descriptor};
use std::{fs::File, path::PathBuf};
use anyhow::Result;
use anyhow::Context;
use ch2::visualize;

///dataflow command line tool
#[derive(Parser)]
#[command(author, bin_name = "dataflow", version, about)]
struct Command {
    /// Print Graphviz representation of the given descriptor file
    #[command(subcommand)]
    graph:Graph,
}

#[derive(Subcommand)]
enum Graph {
    /// show dataflow file as mermaid graph
    Show {
        /// yaml file path
        #[arg(short, long, value_name = "FILE")]
        file: PathBuf,
        #[clap(short,long, action)]
        mermaid: bool,
        #[clap(short,long, action)]
        open: bool,
    },
}


fn main() ->Result<()>{
    let command = Command::parse();
    match command.graph {
        Graph::Show { file,mermaid,open } => {
            visualize::create(file, mermaid, open).unwrap();
        }
    }

    Ok(())
}
