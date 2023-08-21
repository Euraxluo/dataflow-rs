use crate::descriptor::{descriptor::Descriptor, validate::check_dataflow};
use anyhow::{anyhow, Context, Result};
use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};

/// Create a visualization of the given dataflow file.
pub fn start(
    dataflow: PathBuf,
    name: Option<String>,
    attach: bool,
    hot_reload: bool,
) -> Result<()> {
    let descriptor = Descriptor::blocking_read(&dataflow)
        .with_context(|| format!("failed to read dataflow at `{}`", dataflow.display()))?;

    let working_dir = dataflow
        .canonicalize()
        .context("failed to canonicalize dataflow path")?
        .parent()
        .ok_or_else(|| anyhow!("dataflow path has no parent dir"))?
        .to_owned();

    check_dataflow(&descriptor, &working_dir).context("failed to validate yaml")?;
    Ok(())
}
