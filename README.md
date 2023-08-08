# dataflow-rs
a repo of a multi-language dataflow implementation ,The way to build web3，rtos，mq or dataflow.


## ch1: 
### dataflow 第一版架构设计

![Alt text](ch1%E6%9E%B6%E6%9E%84%E5%9B%BE.png)


### 首先我们在ch1中先简单写一个dataflow yaml定义的描述文件

最终目标是通过yaml文件生成一个dataflow的图，然后通过图生成一个dataflow的执行计划，然后通过执行计划生成一个dataflow的执行代码，然后通过执行代码执行dataflow。

首先的对于以下这个简单的yaml
我们现在解析并且将其生成为一个dataflow的图
```yaml
sinks:
  - id: sink-1
    input: A
  - id: sink-2
    input: B
sources:
  - id: source-1
    output: C
  - id: source-2
    output: G
operators:
  - id: op-1
    inputs:
      - C
      - E
      - B
    outputs:
      - A
  - id: op-2
    inputs:
      - C
      - F
    outputs:
      - E
  - id: op-3
    inputs:
      - C
      - G
    outputs:
      - B
```

相关代码：
```rust
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Source {
    id: String,
    output: String,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Sink {
    id: String,
    input: String,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Operator {
    id: String,
    inputs: BTreeSet<String>,
    outputs: BTreeSet<String>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Descriptor {
    #[serde(default)]
    sources: HashSet<Source>,
    #[serde(default)]
    sinks: HashSet<Sink>,
    #[serde(default)]
    operators: HashSet<Operator>,
}
```


生成的流程图如下
```mermaid
flowchart TB
  source-2[\source-2/]
  source-1[\source-1/]
  op-1
  op-3
  op-2
  sink-1[/sink-1\]
  sink-2[/sink-2\]
  source-2 -- G --> op-3
  source-1 -- C --> op-1
  source-1 -- C --> op-2
  source-1 -- C --> op-3
  op-1 -- A --> sink-1
  op-3 -- B --> op-1
  op-3 -- B --> sink-2
  op-2 -- E --> op-1
  missing>missing] -- F --> op-2
```

## ch2:
我们让我们的描述文件复杂一点：

```yaml
version: 1.0
deploy:
  machine: worker_node_id
nodes:
  # cpp node
  - id: python_source_image
    custom:
      deploy:
        machine: worker_node_id
        min_workers: 1
        max_wowkers: 2
      source: ./python_source_node.py
      inputs:
        tick:
          source: dataflow/timer/millis/100
          queue_size: 1000
      outputs:
        - image
  - id: python_object_detection
    custom:
      source: ./object_detection.py
      inputs:
        image: python_source_image/image
      outputs:
        - bbox
  - id: python_plot
    custom:
      source: ./plot.py
      inputs:
        image: python_source_image/image
        bbox: python_object_detection/bbox
  # cpp node
  - id: cxx-node-rust-api
    custom:
      source: build/node_rust_api
      inputs:
        tick: dataflow/timer/millis/300
      outputs:
        - counter
  - id: cxx-node-c-api
    custom:
      source: build/node_c_api
      inputs:
        tick: dataflow/timer/millis/300
      outputs:
        - counter
  - id: runtime-node-1
    operators:
      - id: operator-rust-api
        deploy:
          cpu: 1.1
          gpu: 1.2
          memory: 100.3
          min_workers: 1
          max_wowkers: 2
        shared-library: build/operator_rust_api
        inputs:
          counter_1: cxx-node-c-api/counter
          counter_2: cxx-node-rust-api/counter
        outputs:
          - status
  - id: runtime-node-2
    operators:
      - id: operator-c-api
        shared-library: build/operator_c_api
        inputs:
          op_status: runtime-node-1/operator-rust-api/status
        outputs:
          - half-status
  # rust node
  - id: rust-node
    custom:
      build: cargo build -p target
      source: ../../target/debug/so
      inputs:
        tick: dataflow/timer/millis/10
      outputs:
        - random
  - id: runtime-node
    operators:
      - id: rust-operator
        build: cargo build -p target
        shared-library: ../../target/debug/so
        inputs:
          tick: dataflow/timer/millis/100
          random: rust-node/random
        outputs:
          - status
  - id: rust-sink
    custom:
      build: cargo build -p target
      source: ../../target/debug/so
      inputs:
        message: runtime-node/rust-operator/status

```