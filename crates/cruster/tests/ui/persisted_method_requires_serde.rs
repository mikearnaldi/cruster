use cruster::{entity, entity_impl};

struct NotSerializable {
    value: std::cell::Cell<i32>,
}

#[entity]
#[derive(Clone)]
struct BadEntity;

#[entity_impl]
impl BadEntity {
    #[workflow]
    async fn bad(&self, value: NotSerializable) -> Result<(), cruster::error::ClusterError> {
        let _ = value;
        Ok(())
    }
}

fn main() {}
