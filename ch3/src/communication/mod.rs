pub mod pub_sub;
type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub trait PubSubCommunicationLayer: Send + Sync {
    fn publisher(&mut self, topic: &str) -> Result<Box<dyn Publisher>, BoxError>;
    fn subscribe(&mut self, topic: &str) -> Result<Box<dyn Subscriber>, BoxError>;
}

pub trait Publisher: Send + Sync {
    fn dyn_clone(&self) -> Box<dyn Publisher>;
    fn publish(&self, data: &[u8]) -> Result<(), BoxError>;
}

pub trait Subscriber: Send + Sync {
    fn recv(&mut self) -> Result<Option<Vec<u8>>, BoxError>;
}
