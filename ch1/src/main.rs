
use clap::{Parser,Subcommand};
use ch1::{descriptor::Descriptor};
use std::{fs::File, path::PathBuf};
use anyhow::Result;
use anyhow::Context;

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
        file: PathBuf
    },
}


fn main() ->Result<()>{
    let command = Command::parse();
    match command.graph {
        Graph::Show { file } => {
            let descriptor_file = File::open(&file)
                .context("failed to open given file")
                .unwrap();

            let descriptor: Descriptor = serde_yaml::from_reader(descriptor_file)
                .context("failed to parse given descriptor")
                .unwrap();
            println!("{:#?}", descriptor);
            let visualized = descriptor
                .visualize_as_mermaid()
                .context("failed to visualize descriptor")
                .unwrap();
            println!("{visualized}");
            println!(
                "Paste the above output on https://mermaid.live/ or in a \
        ```mermaid code block on GitHub to display it."
            );
        }
    }

    Ok(())
}
