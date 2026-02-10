use cruster::{entity, entity_impl};

struct BadState {
    value: std::sync::Mutex<i32>,
}

#[entity]
#[derive(Clone)]
struct BadStateEntity;

#[entity_impl]
#[state(BadState)]
impl BadStateEntity {
    #[rpc]
    async fn ping(&self) -> Result<(), cruster::error::ClusterError> {
        Ok(())
    }
}

fn main() {}
