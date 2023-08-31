#[warn(dead_code)]
pub mod descriptor;
mod mermaid;
pub mod visualize;
pub mod validate;

use uuid::Uuid;

pub type DataflowId = Uuid;