use crate::entity_manager::EntityManager;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument};

/// Minimum reaper check interval (5 seconds), matching TS `entityReaper.ts:28`.
const MIN_RESOLUTION_MS: u64 = 5_000;

/// Default reaper check interval before any entity types are registered (30 seconds),
/// matching TS `entityReaper.ts:16`.
const DEFAULT_RESOLUTION_MS: u64 = 30_000;

/// Background task that periodically scans entity managers for idle instances
/// and removes them.
///
/// The check interval dynamically adapts to the shortest registered idle time,
/// floored at 5 seconds, matching the TS `entityReaper.ts` behavior:
/// `currentResolution = max(min(currentResolution, maxIdleTime), 5000)`.
pub struct EntityReaper {
    managers: RwLock<Vec<RegisteredManager>>,
    cancel: CancellationToken,
    /// Current reaper check interval in milliseconds.
    /// Dynamically updated on each `register()` call.
    current_resolution_ms: AtomicU64,
    /// Latch that opens when the first entity type is registered.
    /// The reaper loop waits on this before starting to check.
    latch: Notify,
}

struct RegisteredManager {
    manager: Arc<EntityManager>,
    max_idle: Duration,
}

impl EntityReaper {
    /// Create a new entity reaper.
    pub fn new(cancel: CancellationToken) -> Self {
        Self {
            managers: RwLock::new(Vec::new()),
            cancel,
            current_resolution_ms: AtomicU64::new(DEFAULT_RESOLUTION_MS),
            latch: Notify::new(),
        }
    }

    /// Register an entity manager to be reaped with the given max idle duration.
    ///
    /// Updates the reaper check interval to `max(min(current, max_idle), 5s)`,
    /// matching the TS `entityReaper.ts:28` behavior.
    pub async fn register(&self, manager: Arc<EntityManager>, max_idle: Duration) {
        let max_idle_ms = max_idle.as_millis() as u64;
        // Update resolution: min of current and new idle time, floored at MIN_RESOLUTION_MS.
        loop {
            let current = self.current_resolution_ms.load(Ordering::Acquire);
            let new_resolution = current.min(max_idle_ms).max(MIN_RESOLUTION_MS);
            if new_resolution == current
                || self
                    .current_resolution_ms
                    .compare_exchange_weak(
                        current,
                        new_resolution,
                        Ordering::Release,
                        Ordering::Acquire,
                    )
                    .is_ok()
            {
                break;
            }
        }

        let mut managers = self.managers.write().await;
        let was_empty = managers.is_empty();
        managers.push(RegisteredManager { manager, max_idle });

        // Open the latch when the first entity is registered.
        if was_empty {
            self.latch.notify_waiters();
        }
    }

    /// Run the reaper loop. This blocks until the cancellation token is cancelled.
    ///
    /// The reaper waits until the first entity type is registered, then periodically
    /// checks all registered managers at the dynamically-computed interval.
    /// The interval adapts to the shortest registered idle time, floored at 5 seconds.
    pub async fn run(&self) {
        // Wait for first registration (latch). Check if already registered to avoid
        // missing notifications that happened before `run()` started.
        if self.managers.read().await.is_empty() {
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = self.latch.notified() => {}
            }
        }

        loop {
            let resolution_ms = self.current_resolution_ms.load(Ordering::Acquire);
            let interval = Duration::from_millis(resolution_ms);
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = tokio::time::sleep(interval) => {
                    self.reap_all().await;
                }
            }
        }
    }

    /// Get the current reaper check interval.
    pub fn current_resolution(&self) -> Duration {
        Duration::from_millis(self.current_resolution_ms.load(Ordering::Acquire))
    }

    /// Perform a single reap pass across all registered managers.
    #[instrument(level = "debug", skip(self))]
    pub async fn reap_all(&self) -> usize {
        let managers = self.managers.read().await;
        let mut total_reaped = 0;
        for entry in managers.iter() {
            let reaped = entry.manager.reap_idle(entry.max_idle).await;
            if reaped > 0 {
                debug!(
                    entity_type = %entry.manager.entity().entity_type(),
                    reaped,
                    "reaped idle entities"
                );
            }
            total_reaped += reaped;
        }
        total_reaped
    }

    /// Get the number of registered managers.
    pub async fn manager_count(&self) -> usize {
        self.managers.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ShardingConfig;
    use crate::entity::{Entity, EntityContext, EntityHandler};
    use crate::envelope::EnvelopeRequest;
    use crate::error::ClusterError;
    use crate::message::IncomingMessage;
    use crate::snowflake::{Snowflake, SnowflakeGenerator};
    use crate::types::{EntityAddress, EntityId, EntityType, RunnerAddress, ShardId};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    struct SimpleEntity;

    #[async_trait]
    impl Entity for SimpleEntity {
        fn entity_type(&self) -> EntityType {
            EntityType::new("Simple")
        }

        async fn spawn(&self, _ctx: EntityContext) -> Result<Box<dyn EntityHandler>, ClusterError> {
            Ok(Box::new(SimpleHandler))
        }
    }

    struct SimpleHandler;

    #[async_trait]
    impl EntityHandler for SimpleHandler {
        async fn handle_request(
            &self,
            _tag: &str,
            _payload: &[u8],
            _headers: &HashMap<String, String>,
        ) -> Result<Vec<u8>, ClusterError> {
            Ok(vec![])
        }
    }

    fn make_manager() -> Arc<EntityManager> {
        Arc::new(EntityManager::new(
            Arc::new(SimpleEntity),
            Arc::new(ShardingConfig::default()),
            RunnerAddress::new("127.0.0.1", 9000),
            Arc::new(SnowflakeGenerator::new()),
            None,
        ))
    }

    fn test_request(entity_id: &str) -> (IncomingMessage, mpsc::Receiver<crate::reply::Reply>) {
        let addr = EntityAddress {
            shard_id: ShardId::new("default", 0),
            entity_type: EntityType::new("Simple"),
            entity_id: EntityId::new(entity_id),
        };
        let req = EnvelopeRequest {
            request_id: Snowflake(1),
            address: addr,
            tag: "test".to_string(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        };
        let (tx, rx) = mpsc::channel(1);
        (
            IncomingMessage::Request {
                request: req,
                reply_tx: tx,
            },
            rx,
        )
    }

    #[tokio::test]
    async fn register_adds_manager() {
        let reaper = EntityReaper::new(CancellationToken::new());
        assert_eq!(reaper.manager_count().await, 0);

        reaper
            .register(make_manager(), Duration::from_secs(60))
            .await;
        assert_eq!(reaper.manager_count().await, 1);
    }

    #[tokio::test]
    async fn reap_all_removes_idle_entities() {
        let reaper = EntityReaper::new(CancellationToken::new());
        let mgr = make_manager();

        // Spawn an entity
        let (msg, mut rx) = test_request("e-1");
        mgr.send_local(msg).await.unwrap();
        rx.recv().await.unwrap();
        assert_eq!(mgr.active_count(), 1);

        // Register with very short idle time
        reaper.register(mgr.clone(), Duration::from_millis(1)).await;

        // Wait for entity to become idle
        tokio::time::sleep(Duration::from_millis(10)).await;

        let reaped = reaper.reap_all().await;
        assert_eq!(reaped, 1);
        assert_eq!(mgr.active_count(), 0);
    }

    #[tokio::test]
    async fn reap_all_skips_non_idle_entities() {
        let reaper = EntityReaper::new(CancellationToken::new());
        let mgr = make_manager();

        // Spawn an entity
        let (msg, mut rx) = test_request("e-1");
        mgr.send_local(msg).await.unwrap();
        rx.recv().await.unwrap();

        // Register with long idle time
        reaper
            .register(mgr.clone(), Duration::from_secs(3600))
            .await;

        let reaped = reaper.reap_all().await;
        assert_eq!(reaped, 0);
        assert_eq!(mgr.active_count(), 1);
    }

    #[tokio::test]
    async fn run_loop_cancels_cleanly() {
        let cancel = CancellationToken::new();
        let reaper = Arc::new(EntityReaper::new(cancel.clone()));

        // Register an entity so the latch opens
        reaper
            .register(make_manager(), Duration::from_secs(60))
            .await;

        let reaper_clone = Arc::clone(&reaper);
        let handle = tokio::spawn(async move {
            reaper_clone.run().await;
        });

        // Let it run a few cycles
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();

        // Should complete without hanging
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("reaper should stop")
            .expect("task should not panic");
    }

    #[tokio::test]
    async fn multiple_managers_reaped() {
        let reaper = EntityReaper::new(CancellationToken::new());

        let mgr1 = make_manager();
        let mgr2 = make_manager();

        // Spawn entities in both managers
        let (msg1, mut rx1) = test_request("e-1");
        mgr1.send_local(msg1).await.unwrap();
        rx1.recv().await.unwrap();

        let (msg2, mut rx2) = test_request("e-2");
        mgr2.send_local(msg2).await.unwrap();
        rx2.recv().await.unwrap();

        reaper
            .register(mgr1.clone(), Duration::from_millis(1))
            .await;
        reaper
            .register(mgr2.clone(), Duration::from_millis(1))
            .await;

        tokio::time::sleep(Duration::from_millis(10)).await;

        let reaped = reaper.reap_all().await;
        assert_eq!(reaped, 2);
        assert_eq!(mgr1.active_count(), 0);
        assert_eq!(mgr2.active_count(), 0);
    }

    #[tokio::test]
    async fn dynamic_resolution_adapts_to_shortest_idle_time() {
        let reaper = EntityReaper::new(CancellationToken::new());

        // Default resolution is 30s
        assert_eq!(reaper.current_resolution(), Duration::from_secs(30));

        // Register with 20s idle → resolution becomes 20s
        reaper
            .register(make_manager(), Duration::from_secs(20))
            .await;
        assert_eq!(reaper.current_resolution(), Duration::from_secs(20));

        // Register with 10s idle → resolution becomes 10s
        reaper
            .register(make_manager(), Duration::from_secs(10))
            .await;
        assert_eq!(reaper.current_resolution(), Duration::from_secs(10));

        // Register with 60s idle → resolution stays 10s (min wins)
        reaper
            .register(make_manager(), Duration::from_secs(60))
            .await;
        assert_eq!(reaper.current_resolution(), Duration::from_secs(10));
    }

    #[tokio::test]
    async fn dynamic_resolution_floored_at_5_seconds() {
        let reaper = EntityReaper::new(CancellationToken::new());

        // Register with 1s idle → resolution floors at 5s
        reaper
            .register(make_manager(), Duration::from_secs(1))
            .await;
        assert_eq!(reaper.current_resolution(), Duration::from_secs(5));

        // Register with 100ms idle → still 5s floor
        reaper
            .register(make_manager(), Duration::from_millis(100))
            .await;
        assert_eq!(reaper.current_resolution(), Duration::from_secs(5));
    }

    #[tokio::test]
    async fn run_waits_for_first_registration() {
        let cancel = CancellationToken::new();
        let reaper = Arc::new(EntityReaper::new(cancel.clone()));

        let reaper_clone = Arc::clone(&reaper);
        let handle = tokio::spawn(async move {
            reaper_clone.run().await;
        });

        // Give the run loop time to start — it should be blocked on latch
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Register opens the latch
        reaper
            .register(make_manager(), Duration::from_secs(60))
            .await;

        // Let it run briefly
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();

        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("reaper should stop")
            .expect("task should not panic");
    }
}
