use super::{BoxError, PubSubCommunicationLayer, Publisher, Subscriber};
use anyhow::{anyhow, Result};
use config::{whatami::WhatAmI, ConnectConfig, EndPoint};
use flume::Receiver;
use std::{str::FromStr, sync::Arc};
use zenoh::prelude::{sync::SyncResolve, *};

/// 基于 Zenoh 实现的 PubSubCommunicationLayer
pub struct ZenohCommunicationLayer {
    /// Zenoh 会话
    session: Arc<zenoh::Session>,
    /// zenoh namespace,用于topic prefix
    namespace: String,
}

#[derive(Clone)]
pub struct ZenohPublisher {
    publisher: zenoh::publication::Publisher<'static>,
}
impl Publisher for ZenohPublisher {
    fn publish(&self, data: &[u8]) -> Result<(), BoxError> {
        self.publisher.put(data).res_sync().map_err(BoxError::from)
    }

    fn dyn_clone(&self) -> Box<dyn Publisher> {
        Box::new(self.clone())
    }
}

pub struct ZenohReceiver(zenoh::subscriber::Subscriber<'static, Receiver<Sample>>);

impl Subscriber for ZenohReceiver {
    fn recv(&mut self) -> Result<Option<Vec<u8>>, BoxError> {
        match self.0.recv() {
            Ok(sample) => Ok(Some(sample.value.payload.contiguous().into_owned())),
            Err(flume::RecvError::Disconnected) => Ok(None),
        }
    }
}

impl ZenohCommunicationLayer {
    /// 初始化 ZenohCommunicationLayer
    pub fn init(endpoints: Vec<String>, mode: String, application: String) -> Result<Self> {
        let mut config = ::zenoh::config::Config::default();
        let _ = config.set_mode(WhatAmI::from_str(&mode).ok());
        config.connect = ConnectConfig {
            endpoints: endpoints
                .iter()
                .filter_map(|e| EndPoint::from_str(e).ok())
                .collect(),
        };
        let session = zenoh::open(config)
            .res_sync()
            .map_err(|e| anyhow!(e))?
            .into_arc();
        Ok(Self {
            session,
            namespace: application,
        })
    }
    /// 根据topic获取完整的 prefix
    fn prefixed(&self, topic: &str) -> String {
        format!("{}/{topic}", self.namespace)
    }
}

impl PubSubCommunicationLayer for ZenohCommunicationLayer {
    fn publisher(&mut self, topic: &str) -> Result<Box<dyn Publisher>, BoxError> {
        let publisher = self
            .session
            .declare_publisher(self.prefixed(topic))
            .congestion_control(CongestionControl::Block)
            .priority(Priority::RealTime)
            .res_sync()
            .map_err(BoxError::from)?;

        Ok(Box::new(ZenohPublisher { publisher }))
    }

    fn subscribe(&mut self, topic: &str) -> Result<Box<dyn Subscriber>, BoxError> {
        let subscriber = self
            .session
            .declare_subscriber(self.prefixed(topic))
            .reliable()
            .res_sync()
            .map_err(BoxError::from)?;

        Ok(Box::new(ZenohReceiver(subscriber)))
    }
}
