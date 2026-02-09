use cruster::{entity, entity_impl};
use cruster::entity::Entity;

#[entity]
#[derive(Clone)]
struct LoggingEntity;

// Using traits(...) on entity_impl should produce an error directing to rpc_groups
#[entity_impl(traits(SomeGroup))]
impl LoggingEntity {
    #[rpc]
    async fn ping(&self) -> Result<String, cruster::error::ClusterError> {
        Ok("pong".to_string())
    }
}

fn assert_entity<T: Entity>() {}

fn main() {
    assert_entity::<LoggingEntity>();
}
