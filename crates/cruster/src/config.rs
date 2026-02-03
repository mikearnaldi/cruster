use crate::error::ClusterError;
use crate::shard_assigner::ShardAssignmentStrategy;
use crate::types::RunnerAddress;
use std::time::Duration;

/// Configuration for the sharding system.
#[derive(Debug, Clone)]
pub struct ShardingConfig {
    /// Address this runner advertises to other runners.
    pub runner_address: RunnerAddress,
    /// Address this runner listens on (defaults to runner_address).
    pub runner_listen_address: Option<RunnerAddress>,
    /// Weight of this runner for shard assignment. Default: 1.
    pub runner_weight: i32,
    /// Shard groups to participate in. Default: ["default"].
    pub shard_groups: Vec<String>,
    /// Number of shards per group. Default: 300.
    pub shards_per_group: i32,
    /// TTL for runner lock in storage. Default: 30s.
    pub runner_lock_ttl: Duration,
    /// How often to refresh runner lock. Default: 10s.
    pub runner_lock_refresh_interval: Duration,
    /// Max idle time before an entity is reaped. Default: 60s.
    pub entity_max_idle_time: Duration,
    /// How long to wait for entity termination. Default: 15s.
    pub entity_termination_timeout: Duration,
    /// Mailbox capacity per entity. Default: 100.
    pub entity_mailbox_capacity: usize,
    /// Maximum number of concurrent requests per entity instance. Default: 1 (serial).
    /// 0 = unbounded concurrency. Per-entity `Entity::concurrency()` overrides this.
    /// When > 1, requests are dispatched concurrently under a read lock; crash
    /// recovery takes a write lock to swap the handler and replays all in-flight requests.
    pub entity_max_concurrent_requests: usize,
    /// How often to poll storage for unprocessed messages. Default: 500ms.
    pub storage_poll_interval: Duration,
    /// Max inbox size from storage polling. Default: 1000.
    pub storage_inbox_size: usize,
    /// How often to poll for runner changes. Default: 5s.
    pub runner_poll_interval: Duration,
    /// Debounce interval for shard rebalancing. Default: 3s.
    pub shard_rebalance_debounce: Duration,
    /// Retry interval for shard rebalancing. Default: 2s.
    pub shard_rebalance_retry_interval: Duration,
    /// Maximum number of consecutive crash recovery attempts for an entity
    /// before giving up and dropping the entity instance. 0 = unlimited
    /// retries (matching TS behavior). Default: 0 (unlimited).
    pub entity_crash_max_retries: u32,
    /// Initial backoff duration between crash recovery attempts. Uses
    /// exponential backoff with factor 1.5, capped at
    /// `entity_crash_max_backoff`. Default: 500ms.
    pub entity_crash_initial_backoff: Duration,
    /// Maximum backoff duration between crash recovery attempts. Default: 10s.
    pub entity_crash_max_backoff: Duration,
    /// Maximum number of delivery attempts for a persisted message before
    /// it is marked as dead-lettered (processed with a failure reply).
    /// 0 = unlimited retries. Default: 10.
    pub storage_message_max_retries: u32,
    /// Maximum consecutive lock refresh failures before a shard is released.
    /// When a shard lock fails to refresh this many times in a row, the shard
    /// is removed from owned shards and its entities are interrupted to prevent
    /// split-brain (another runner may have already acquired the expired lock).
    /// Default: 3.
    pub shard_lock_refresh_max_failures: u32,
    /// How long to wait for an entity type to be registered before returning
    /// an error. During startup, messages may arrive before all entity types
    /// are registered. This timeout allows the router to wait for late
    /// registrations instead of immediately failing. Default: 5s.
    pub entity_registration_timeout: Duration,
    /// Retry interval for delivering messages to entities with full mailboxes.
    /// When a storage-polled message encounters `MailboxFull`, a per-entity
    /// resumption task retries delivery at this interval. Default: 100ms.
    pub send_retry_interval: Duration,
    /// Base backoff duration for singleton crash recovery. When a singleton
    /// fails (returns Err or panics), re-spawn is delayed by
    /// `base * 2^min(consecutive_failures - 1, 10)`. Default: 1s.
    pub singleton_crash_backoff_base: Duration,
    /// Number of retry attempts for `send()` and `notify()` when routing
    /// fails with `EntityNotAssignedToRunner` or `RunnerUnavailable` during
    /// shard rebalancing. 0 = no retries. Default: 3.
    pub send_retry_count: u32,
    /// Maximum number of MailboxFull retries in the storage resumption task
    /// before giving up on a message. When exhausted, a failure reply is saved
    /// to storage and the message is considered dead-lettered.
    /// 0 = unlimited (retry indefinitely until shard is lost or shutdown).
    /// Default: 0 (matches TS behavior of infinite retry).
    pub storage_resumption_max_retries: u32,
    /// Timeout for establishing gRPC connections to remote runners. Prevents
    /// blocking indefinitely when a runner host is unresponsive. Default: 5s.
    pub grpc_connect_timeout: Duration,
    /// Guard interval for the `last_read` dedup mechanism in SQL message storage.
    /// After a message is read by `unprocessed_messages`, it will not be re-read
    /// until this interval has elapsed, preventing duplicate dispatch of messages
    /// that are still being processed. Default: 10 minutes (matches TS).
    pub last_read_guard_interval: Duration,
    /// Strategy for assigning shards to runners. Default: Rendezvous.
    ///
    /// - `Rendezvous`: Best distribution (each node gets exactly 1/n shards, ±1),
    ///   optimal rebalancing. Recommended for clusters with < 1000 nodes.
    pub shard_assignment_strategy: ShardAssignmentStrategy,

    /// Window before detaching from the cluster after a storage error or keep-alive
    /// failure. If connectivity is restored within this window, the runner stays
    /// attached. If not, owned shards are cleared and entities are interrupted.
    /// Default: 200ms.
    pub detachment_window: Duration,

    /// Duration of sustained healthy status required before re-attaching to the
    /// cluster after detachment. The runner must observe healthy keep-alive and
    /// storage operations for this duration before it can acquire shards again.
    /// Default: 500ms.
    pub detachment_recover_window: Duration,

    /// Whether detachment is enabled. When false, storage errors are logged but
    /// do not trigger detachment. Use this to preserve current behavior during
    /// rollout. Default: false.
    pub detachment_enabled: bool,

    /// Number of consecutive keep-alive failures before triggering detachment.
    /// When the lease keep-alive fails this many times in a row, the runner
    /// detaches from the cluster to prevent split-brain. Default: 3.
    pub keepalive_failure_threshold: u32,

    /// Interval between acquire retries for shards held by other runners.
    /// After the initial acquire_batch, shards that were not acquired due to
    /// being held elsewhere are retried at this interval. Default: 200ms.
    pub acquire_retry_interval: Duration,

    /// Maximum duration to retry acquiring shards before giving up for this
    /// rebalance cycle. Retries continue until either all shards are acquired
    /// or this window elapses. 0 = no retries (single attempt). Default: 2s.
    pub acquire_retry_window: Duration,
}

impl ShardingConfig {
    /// Validate configuration values. Returns an error message if any value is invalid.
    ///
    /// Checks:
    /// - `shards_per_group >= 1` (prevents division-by-zero in shard assignment)
    /// - `runner_weight >= 0` (negative weight has no meaning)
    /// - `entity_mailbox_capacity >= 1` (zero-capacity mailbox can never accept messages)
    /// - `storage_inbox_size >= 1` (zero inbox can never process messages)
    /// - All durations are non-zero where required
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.shards_per_group < 1 {
            return Err(ClusterError::InvalidConfig {
                reason: format!(
                    "shards_per_group must be >= 1, got {}",
                    self.shards_per_group
                ),
            });
        }
        if self.runner_weight < 0 {
            return Err(ClusterError::InvalidConfig {
                reason: format!("runner_weight must be >= 0, got {}", self.runner_weight),
            });
        }
        if self.entity_mailbox_capacity == 0 {
            return Err(ClusterError::InvalidConfig {
                reason: "entity_mailbox_capacity must be >= 1".to_string(),
            });
        }
        if self.storage_inbox_size == 0 {
            return Err(ClusterError::InvalidConfig {
                reason: "storage_inbox_size must be >= 1".to_string(),
            });
        }
        if self.runner_lock_ttl.is_zero() {
            return Err(ClusterError::InvalidConfig {
                reason: "runner_lock_ttl must be > 0".to_string(),
            });
        }
        if self.runner_lock_refresh_interval.is_zero() {
            return Err(ClusterError::InvalidConfig {
                reason: "runner_lock_refresh_interval must be > 0".to_string(),
            });
        }
        if self.entity_max_idle_time.is_zero() {
            return Err(ClusterError::InvalidConfig {
                reason: "entity_max_idle_time must be > 0".to_string(),
            });
        }
        if self.entity_termination_timeout.is_zero() {
            return Err(ClusterError::InvalidConfig {
                reason: "entity_termination_timeout must be > 0".to_string(),
            });
        }
        if self.storage_poll_interval.is_zero() {
            return Err(ClusterError::InvalidConfig {
                reason: "storage_poll_interval must be > 0".to_string(),
            });
        }
        if self.runner_poll_interval.is_zero() {
            return Err(ClusterError::InvalidConfig {
                reason: "runner_poll_interval must be > 0".to_string(),
            });
        }
        if self.shard_rebalance_debounce.is_zero() {
            return Err(ClusterError::InvalidConfig {
                reason: "shard_rebalance_debounce must be > 0".to_string(),
            });
        }
        if self.shard_rebalance_retry_interval.is_zero() {
            return Err(ClusterError::InvalidConfig {
                reason: "shard_rebalance_retry_interval must be > 0".to_string(),
            });
        }
        if self.entity_registration_timeout.is_zero() {
            return Err(ClusterError::InvalidConfig {
                reason: "entity_registration_timeout must be > 0".to_string(),
            });
        }
        if self.send_retry_interval.is_zero() {
            return Err(ClusterError::InvalidConfig {
                reason: "send_retry_interval must be > 0".to_string(),
            });
        }
        if self.singleton_crash_backoff_base.is_zero() {
            return Err(ClusterError::InvalidConfig {
                reason: "singleton_crash_backoff_base must be > 0".to_string(),
            });
        }
        if self.entity_crash_initial_backoff.is_zero() {
            return Err(ClusterError::InvalidConfig {
                reason: "entity_crash_initial_backoff must be > 0".to_string(),
            });
        }
        if self.entity_crash_max_backoff.is_zero() {
            return Err(ClusterError::InvalidConfig {
                reason: "entity_crash_max_backoff must be > 0".to_string(),
            });
        }
        if self.shard_groups.is_empty() {
            return Err(ClusterError::InvalidConfig {
                reason: "shard_groups must not be empty".to_string(),
            });
        }
        if self.grpc_connect_timeout.is_zero() {
            return Err(ClusterError::InvalidConfig {
                reason: "grpc_connect_timeout must be > 0".to_string(),
            });
        }
        Ok(())
    }
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            runner_address: RunnerAddress::new("127.0.0.1", 9000),
            runner_listen_address: None,
            runner_weight: 1,
            shard_groups: vec!["default".to_string()],
            shards_per_group: 2048,
            runner_lock_ttl: Duration::from_secs(30),
            runner_lock_refresh_interval: Duration::from_secs(10),
            entity_max_idle_time: Duration::from_secs(60),
            entity_termination_timeout: Duration::from_secs(15),
            entity_mailbox_capacity: 100,
            entity_max_concurrent_requests: 1,
            storage_poll_interval: Duration::from_millis(500),
            storage_inbox_size: 1000,
            runner_poll_interval: Duration::from_secs(5),
            shard_rebalance_debounce: Duration::from_secs(3),
            shard_rebalance_retry_interval: Duration::from_secs(2),
            entity_crash_max_retries: 0,
            entity_crash_initial_backoff: Duration::from_millis(500),
            entity_crash_max_backoff: Duration::from_secs(10),
            storage_message_max_retries: 10,
            shard_lock_refresh_max_failures: 3,
            entity_registration_timeout: Duration::from_secs(5),
            send_retry_interval: Duration::from_millis(100),
            singleton_crash_backoff_base: Duration::from_secs(1),
            send_retry_count: 3,
            storage_resumption_max_retries: 0,
            grpc_connect_timeout: Duration::from_secs(5),
            last_read_guard_interval: Duration::from_secs(600),
            shard_assignment_strategy: ShardAssignmentStrategy::default(),
            detachment_window: Duration::from_millis(200),
            detachment_recover_window: Duration::from_millis(500),
            detachment_enabled: false,
            keepalive_failure_threshold: 3,
            acquire_retry_interval: Duration::from_millis(200),
            acquire_retry_window: Duration::from_secs(2),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_values() {
        let config = ShardingConfig::default();
        assert_eq!(config.runner_weight, 1);
        assert_eq!(config.shard_groups, vec!["default".to_string()]);
        assert_eq!(config.shards_per_group, 2048);
        assert_eq!(config.runner_lock_ttl, Duration::from_secs(30));
        assert_eq!(config.entity_max_idle_time, Duration::from_secs(60));
        assert_eq!(config.entity_mailbox_capacity, 100);
        assert_eq!(config.entity_max_concurrent_requests, 1);
        assert_eq!(config.entity_crash_max_retries, 0);
        assert_eq!(
            config.entity_crash_initial_backoff,
            Duration::from_millis(500)
        );
        assert_eq!(config.entity_crash_max_backoff, Duration::from_secs(10));
        assert_eq!(config.entity_registration_timeout, Duration::from_secs(5));
    }

    #[test]
    fn custom_config() {
        let config = ShardingConfig {
            shards_per_group: 600,
            entity_mailbox_capacity: 200,
            ..Default::default()
        };
        assert_eq!(config.shards_per_group, 600);
        assert_eq!(config.entity_mailbox_capacity, 200);
        // Other fields keep defaults
        assert_eq!(config.runner_weight, 1);
    }

    #[test]
    fn default_config_is_valid() {
        ShardingConfig::default().validate().unwrap();
    }

    #[test]
    fn validate_shards_per_group_zero() {
        let config = ShardingConfig {
            shards_per_group: 0,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("shards_per_group"), "got: {msg}");
    }

    #[test]
    fn validate_negative_runner_weight() {
        let config = ShardingConfig {
            runner_weight: -1,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("runner_weight"), "got: {msg}");
    }

    #[test]
    fn validate_zero_mailbox_capacity() {
        let config = ShardingConfig {
            entity_mailbox_capacity: 0,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("entity_mailbox_capacity"), "got: {msg}");
    }

    #[test]
    fn validate_zero_duration() {
        let config = ShardingConfig {
            runner_lock_ttl: Duration::ZERO,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("runner_lock_ttl"), "got: {msg}");
    }

    #[test]
    fn validate_zero_storage_inbox_size() {
        let config = ShardingConfig {
            storage_inbox_size: 0,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("storage_inbox_size"), "got: {msg}");
    }

    #[test]
    fn validate_runner_weight_zero_is_valid() {
        // Weight 0 = drain mode, valid
        let config = ShardingConfig {
            runner_weight: 0,
            ..Default::default()
        };
        config.validate().unwrap();
    }
}
