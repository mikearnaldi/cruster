//! Singleton registration helper.
//!
//! Provides a convenience function for registering singletons with the cluster.
//! A singleton runs on exactly one runner — the one that owns the shard
//! computed by hashing the singleton name within its shard group.
//!
//! Singletons receive a [`SingletonContext`] which includes a cancellation token
//! for graceful shutdown when the singleton's shard migrates to another node.
//!
//! Ported from Effect's `Singleton.ts`.

use crate::error::ClusterError;
use crate::sharding::Sharding;
use futures::future::BoxFuture;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Context passed to singleton factory functions.
///
/// Contains resources the singleton can use during its lifetime, including
/// a cancellation token for graceful shutdown.
///
/// # Example
///
/// ```text
/// use cruster::singleton::{register_singleton, SingletonContext};
///
/// register_singleton(&*sharding, "leader-election", |ctx: SingletonContext| async move {
///     // Opt-in to graceful shutdown - singleton will wait for this function to return
///     let cancel = ctx.cancellation();
///     
///     loop {
///         tokio::select! {
///             _ = cancel.cancelled() => {
///                 // Graceful shutdown: commit pending work, close connections
///                 tracing::info!("singleton shutting down gracefully");
///                 break;
///             }
///             _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
///                 // Do periodic work
///             }
///         }
///     }
///     Ok(())
/// }).await?;
/// ```
pub struct SingletonContext {
    /// Inner cancellation token.
    inner: CancellationToken,
    /// Flag indicating whether the singleton has opted in to manage cancellation.
    /// This is shared with the spawner to determine shutdown behavior.
    pub(crate) managed: Arc<AtomicBool>,
}

impl SingletonContext {
    /// Creates a new singleton context with an externally provided managed flag.
    /// This allows the caller to share the managed flag with other components
    /// (e.g., `SingletonEntry`) so they can check if the singleton opted in
    /// to manage its own cancellation.
    pub(crate) fn new(cancellation: CancellationToken, managed: Arc<AtomicBool>) -> Self {
        Self {
            inner: cancellation,
            managed,
        }
    }

    /// Returns a cancellation token for graceful shutdown.
    ///
    /// **By calling this method, the singleton commits to observing the token
    /// and returning when cancelled.** The runtime will wait for the singleton
    /// to complete gracefully instead of force-cancelling it.
    ///
    /// If this method is never called, the singleton will be force-cancelled
    /// when shutdown is requested.
    ///
    /// # Example
    ///
    /// ```text
    /// |ctx: SingletonContext| async move {
    ///     let cancel = ctx.cancellation();
    ///     
    ///     loop {
    ///         tokio::select! {
    ///             _ = cancel.cancelled() => break,
    ///             _ = do_work() => {}
    ///         }
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn cancellation(&self) -> CancellationToken {
        self.managed.store(true, Ordering::Release);
        self.inner.clone()
    }

    /// Check if cancellation has been requested without opting in to manage it.
    ///
    /// This is useful for quick checks without committing to graceful shutdown.
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }
}

/// Register a singleton that runs on exactly one runner in the cluster.
///
/// The `run` closure is executed only on the runner that owns the shard
/// computed from the singleton name and shard group (default group when not
/// specified). If that runner goes down, the singleton migrates to whichever
/// runner acquires the shard.
///
/// The closure receives a [`SingletonContext`] with a cancellation token that
/// is triggered when the singleton should shut down gracefully.
///
/// # Examples
///
/// ```text
/// use cruster::singleton::{register_singleton, SingletonContext};
///
/// register_singleton(&*sharding, "leader-election", |ctx: SingletonContext| async move {
///     loop {
///         tokio::select! {
///             _ = ctx.cancellation.cancelled() => {
///                 tracing::info!("shutting down");
///                 break;
///             }
///             _ = do_work() => {}
///         }
///     }
///     Ok(())
/// }).await?;
/// ```
pub async fn register_singleton<F, Fut>(
    sharding: &dyn Sharding,
    name: &str,
    run: F,
) -> Result<(), ClusterError>
where
    F: Fn(SingletonContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), ClusterError>> + Send + 'static,
{
    sharding
        .register_singleton(
            name,
            None,
            Arc::new(move |ctx| -> BoxFuture<'static, Result<(), ClusterError>> {
                Box::pin(run(ctx))
            }),
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
    F: Fn(SingletonContext) -> Fut + Send + Sync + 'static,
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
/// use cruster::singleton::{singleton, SingletonContext};
///
/// singleton("metrics-aggregator", |ctx: SingletonContext| async move {
///     loop {
///         tokio::select! {
///             _ = ctx.cancellation.cancelled() => break,
///             _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
///                 // aggregate metrics
///             }
///         }
///     }
///     Ok(())
/// })
/// .register(&*sharding)
/// .await?;
/// ```
pub fn singleton<F, Fut>(name: impl Into<String>, run: F) -> SingletonBuilder<F>
where
    F: Fn(SingletonContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), ClusterError>> + Send + 'static,
{
    SingletonBuilder::new(name, run)
}

/// A boxed singleton run function (reusable across re-spawns).
///
/// The function receives a [`SingletonContext`] and returns a future that
/// completes when the singleton finishes (or should be restarted on error).
pub type SingletonRun =
    Arc<dyn Fn(SingletonContext) -> BoxFuture<'static, Result<(), ClusterError>> + Send + Sync>;

/// Register multiple singletons at once.
///
/// Registers all singletons sequentially, returning on the first error.
/// Each singleton's factory receives a [`SingletonContext`] for graceful shutdown.
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
    use crate::config::ShardingConfig;
    use crate::metrics::ClusterMetrics;
    use crate::sharding_impl::ShardingImpl;
    use crate::storage::noop_runners::NoopRunners;
    use std::sync::Arc;

    /// Create a minimal ShardingImpl with NoopRunners for unit tests.
    async fn test_sharding() -> Arc<ShardingImpl> {
        let config = Arc::new(ShardingConfig::default());
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s =
            ShardingImpl::new(config, Arc::new(NoopRunners), None, None, None, metrics).unwrap();
        s.acquire_all_shards().await;
        s
    }

    #[tokio::test]
    async fn register_singleton_via_helper() {
        let sharding = test_sharding().await;
        let executed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executed_clone = executed.clone();

        register_singleton(
            sharding.as_ref(),
            "test-singleton",
            move |_ctx: SingletonContext| {
                let e = executed_clone.clone();
                async move {
                    e.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .await
        .unwrap();

        // Give the singleton time to execute
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(executed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn singleton_builder_api() {
        let sharding = test_sharding().await;
        let executed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executed_clone = executed.clone();

        singleton("builder-singleton", move |_ctx: SingletonContext| {
            let e = executed_clone.clone();
            async move {
                e.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        })
        .register(sharding.as_ref())
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(executed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn register_multiple_singletons() {
        let sharding = test_sharding().await;
        let count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let mut singletons: Vec<(String, SingletonRun)> = Vec::new();

        for i in 0..3 {
            let c = count.clone();
            singletons.push((
                format!("singleton-{i}"),
                Arc::new(
                    move |_ctx| -> BoxFuture<'static, Result<(), ClusterError>> {
                        let c = c.clone();
                        Box::pin(async move {
                            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            Ok(())
                        })
                    },
                ),
            ));
        }

        register_singletons(sharding.as_ref(), singletons)
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn singleton_receives_cancellation_token() {
        let sharding = test_sharding().await;
        let token_received = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let token_received_clone = token_received.clone();

        register_singleton(
            sharding.as_ref(),
            "cancellation-test",
            move |ctx: SingletonContext| {
                let t = token_received_clone.clone();
                async move {
                    // Verify we received a valid cancellation token
                    // (it should not be cancelled initially)
                    if !ctx.is_cancelled() {
                        t.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                    Ok(())
                }
            },
        )
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(token_received.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn singleton_not_managing_cancellation_is_force_cancelled() {
        let sharding = test_sharding().await;

        register_singleton(
            sharding.as_ref(),
            "unmanaged-singleton",
            move |_ctx: SingletonContext| async move {
                // This singleton does NOT call ctx.cancellation()
                // so it will be force-cancelled on shutdown
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                #[allow(unreachable_code)]
                Ok(())
            },
        )
        .await
        .unwrap();

        // Give the singleton time to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Shutdown should complete quickly because singleton is force-cancelled
        let start = std::time::Instant::now();
        sharding.shutdown().await.unwrap();
        let elapsed = start.elapsed();

        // Should complete in well under 1 second (the singleton's sleep duration)
        assert!(
            elapsed < std::time::Duration::from_millis(500),
            "shutdown took too long ({:?}), singleton may not have been force-cancelled",
            elapsed
        );
    }

    #[tokio::test]
    async fn singleton_managing_cancellation_waits_for_graceful_shutdown() {
        let sharding = test_sharding().await;
        let cleanup_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cleanup_ran_clone = cleanup_ran.clone();

        register_singleton(
            sharding.as_ref(),
            "managed-singleton",
            move |ctx: SingletonContext| {
                let cleanup = cleanup_ran_clone.clone();
                async move {
                    // This singleton DOES call ctx.cancellation()
                    // so the runtime will wait for it to finish
                    let cancel = ctx.cancellation();

                    loop {
                        tokio::select! {
                            _ = cancel.cancelled() => {
                                // Graceful shutdown - do cleanup
                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                cleanup.store(true, std::sync::atomic::Ordering::SeqCst);
                                break;
                            }
                            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                                // Keep running
                            }
                        }
                    }
                    Ok(())
                }
            },
        )
        .await
        .unwrap();

        // Give the singleton time to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Shutdown should wait for graceful cleanup
        sharding.shutdown().await.unwrap();

        // Cleanup SHOULD have run
        assert!(
            cleanup_ran.load(std::sync::atomic::Ordering::SeqCst),
            "cleanup should run when singleton manages cancellation"
        );
    }
}
