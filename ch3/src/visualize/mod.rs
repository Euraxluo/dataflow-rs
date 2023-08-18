use anyhow::{Context, Result};
use std::{fs::File, io::Write, path::Path};
pub mod mermaid;
use crate::descriptor::Descriptor;
use webbrowser;

/// HTML template for rendering mermaid graphs.
const MERMAID_TEMPLATE: &str = include_str!("mermaid-template.html");

/// Create a visualization of the given dataflow file.
pub fn create(dataflow: std::path::PathBuf, mermaid: bool, open: bool) -> Result<()> {
    if mermaid {
        // 生成mermaid图
        let visualized = visualize_as_mermaid(&dataflow)?;
        println!("{visualized}");
        println!(
            "Paste the above output on https://mermaid.live/ or in a \
            ```mermaid code block on GitHub to display it."
        );
    } else {
        // 将mermaid图嵌入到html
        let html = visualize_as_html(&dataflow)?;
        // 获取当前的工作目录
        let working_dir = std::env::current_dir().expect("failed to get current working dir");
        // 生成文件名
        let graph_filename = match dataflow.file_stem().and_then(|n| n.to_str()) {
            Some(name) => format!("{name}-graph"),
            None => "graph".into(),
        };
        // 这个逻辑就是说当文件名重复时给他增加后缀
        // 我觉得不如随机加一点后缀
        // 或者使用时间戳
        let mut extra = 0;
        let path = loop {
            let adjusted_file_name = if extra == 0 {
                format!("{graph_filename}.html")
            } else {
                format!("{graph_filename}.{extra}.html")
            };
            let path = working_dir.join(&adjusted_file_name);
            if path.exists() {
                extra += 1;
            } else {
                break path;
            }
        };

        // 创建文件并写入html
        let mut file = File::create(&path).context("failed to create graph HTML file")?;
        file.write_all(html.as_bytes())?;

        println!(
            "View graph by opening the following in your browser:\n  file://{}",
            path.display()
        );

        // 是否打开浏览器
        if open {
            webbrowser::open(path.as_os_str().to_str().unwrap())?;
        }
    }
    Ok(())
}

/// 根据dataflow文件生成包含 mermaid图 的html
pub fn visualize_as_html(dataflow: &Path) -> Result<String> {
    let mermaid = visualize_as_mermaid(dataflow)?;
    Ok(MERMAID_TEMPLATE.replacen("____insert____", &mermaid, 1))
}

/// 根据dataflow文件生成mermaid图
pub fn visualize_as_mermaid(dataflow: &Path) -> Result<String> {
    let descriptor = Descriptor::blocking_read(dataflow)
        .with_context(|| format!("failed to read dataflow at `{}`", dataflow.display()))?;

    let visualized = descriptor
        .visualize_as_mermaid()
        .context("failed to visualize descriptor")?;

    Ok(visualized)
}