//! Singleton registration helper.
//!
//! Provides a convenience function for registering singletons with the cluster.
//! A singleton runs on exactly one runner — the one that owns the shard
//! computed by hashing the singleton name within its shard group.
//!
//! Ported from Effect's `Singleton.ts`.

use crate::error::ClusterError;
use crate::sharding::Sharding;
use futures::future::BoxFuture;
use std::future::Future;
use std::sync::Arc;

/// Register a singleton that runs on exactly one runner in the cluster.
///
/// The `run` closure is executed only on the runner that owns the shard
/// computed from the singleton name and shard group (default group when not
/// specified). If that runner goes down, the singleton migrates to whichever
/// runner acquires the shard.
///
/// # Examples
///
/// ```text
/// use cruster::singleton::register_singleton;
///
/// register_singleton(&*sharding, "leader-election", || async {
///     // This code runs on exactly one node
///     Ok(())
/// }).await?;
/// ```
pub async fn register_singleton<F, Fut>(
    sharding: &dyn Sharding,
    name: &str,
    run: F,
) -> Result<(), ClusterError>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), ClusterError>> + Send + 'static,
{
    sharding
        .register_singleton(
            name,
            None,
            Arc::new(move || -> BoxFuture<'static, Result<(), ClusterError>> { Box::pin(run()) }),
        )
        .await
}

/// Builder for configuring and registering a singleton.
///
/// Provides a fluent API for singleton registration with optional configuration.
pub struct SingletonBuilder<F> {
    name: String,
    run: F,
}

impl<F, Fut> SingletonBuilder<F>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), ClusterError>> + Send + 'static,
{
    /// Create a new singleton builder.
    pub fn new(name: impl Into<String>, run: F) -> Self {
        Self {
            name: name.into(),
            run,
        }
    }

    /// Register this singleton with the given sharding instance.
    pub async fn register(self, sharding: &dyn Sharding) -> Result<(), ClusterError> {
        register_singleton(sharding, &self.name, self.run).await
    }
}

/// Create a singleton builder with the given name and run function.
///
/// # Examples
///
/// ```text
/// use cruster::singleton::singleton;
///
/// singleton("metrics-aggregator", || async {
///     loop {
///         // aggregate metrics
///         tokio::time::sleep(std::time::Duration::from_secs(10)).await;
///     }
/// })
/// .register(&*sharding)
/// .await?;
/// ```
pub fn singleton<F, Fut>(name: impl Into<String>, run: F) -> SingletonBuilder<F>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), ClusterError>> + Send + 'static,
{
    SingletonBuilder::new(name, run)
}

/// A boxed singleton run function (reusable across re-spawns).
pub type SingletonRun = Arc<dyn Fn() -> BoxFuture<'static, Result<(), ClusterError>> + Send + Sync>;

/// Register multiple singletons at once.
///
/// Registers all singletons sequentially, returning on the first error.
pub async fn register_singletons(
    sharding: &dyn Sharding,
    singletons: Vec<(String, SingletonRun)>,
) -> Result<(), ClusterError> {
    for (name, run) in singletons {
        sharding.register_singleton(&name, None, run).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::TestCluster;
    use std::sync::Arc;

    #[tokio::test]
    async fn register_singleton_via_helper() {
        let cluster = TestCluster::new().await;
        let executed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executed_clone = executed.clone();

        register_singleton(cluster.sharding().as_ref(), "test-singleton", move || {
            let e = executed_clone.clone();
            async move {
                e.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();

        // Give the singleton time to execute
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(executed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn singleton_builder_api() {
        let cluster = TestCluster::new().await;
        let executed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executed_clone = executed.clone();

        singleton("builder-singleton", move || {
            let e = executed_clone.clone();
            async move {
                e.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        })
        .register(cluster.sharding().as_ref())
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(executed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn register_multiple_singletons() {
        let cluster = TestCluster::new().await;
        let count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let mut singletons: Vec<(String, SingletonRun)> = Vec::new();

        for i in 0..3 {
            let c = count.clone();
            singletons.push((
                format!("singleton-{i}"),
                Arc::new(move || -> BoxFuture<'static, Result<(), ClusterError>> {
                    let c = c.clone();
                    Box::pin(async move {
                        c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(())
                    })
                }),
            ));
        }

        register_singletons(cluster.sharding().as_ref(), singletons)
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 3);
    }
}
