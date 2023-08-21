use anyhow::Result;
use ch3::{
    cli::{Args, Command},
    descriptor::visualize::visualize,
    runtime::start,
};
use clap::Parser;

fn main() -> Result<()> {
    let args = Args::parse();
    args.init_log();
    match args.command {
        Command::Show {
            file,
            mermaid,
            open,
        } => visualize(file, mermaid, open).unwrap(),
        Command::Start {
            dataflow,
            name,
            attach,
            hot_reload,
        } => start(dataflow, name, attach, hot_reload).unwrap(),
    }

    Ok(())
}
