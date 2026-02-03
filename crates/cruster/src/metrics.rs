use prometheus::{IntCounter, IntGauge, Opts, Registry};

/// Cluster-level prometheus metrics.
pub struct ClusterMetrics {
    /// Number of active entity instances.
    pub entities: IntGauge,
    /// Number of registered singletons.
    pub singletons: IntGauge,
    /// Number of known runners.
    pub runners: IntGauge,
    /// Number of healthy runners.
    pub runners_healthy: IntGauge,
    /// Number of shards owned by this runner.
    pub shards: IntGauge,
    /// Whether the runner is detached from the cluster (0 = attached, 1 = detached).
    pub sharding_detached: IntGauge,
    /// Total number of lease keep-alive failures.
    pub lease_keepalive_failures: IntCounter,
    /// Current consecutive keep-alive failure streak.
    pub lease_keepalive_failure_streak: IntGauge,
    /// Total number of acquire retry attempts for shards held by other runners.
    pub acquire_retry_attempts: IntCounter,
    /// Number of times the acquire retry window was exhausted without acquiring all shards.
    pub acquire_retry_window_exhausted: IntCounter,
}

impl ClusterMetrics {
    /// Create metrics and register them with the given prometheus registry.
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let entities = IntGauge::with_opts(Opts::new(
            "cluster_entities",
            "Number of active entity instances",
        ))?;
        let singletons = IntGauge::with_opts(Opts::new(
            "cluster_singletons",
            "Number of registered singletons",
        ))?;
        let runners = IntGauge::with_opts(Opts::new("cluster_runners", "Number of known runners"))?;
        let runners_healthy = IntGauge::with_opts(Opts::new(
            "cluster_runners_healthy",
            "Number of healthy runners",
        ))?;
        let shards = IntGauge::with_opts(Opts::new(
            "cluster_shards",
            "Number of shards owned by this runner",
        ))?;
        let sharding_detached = IntGauge::with_opts(Opts::new(
            "cluster_sharding_detached",
            "Whether the runner is detached from the cluster (0 = attached, 1 = detached)",
        ))?;
        let lease_keepalive_failures = IntCounter::with_opts(Opts::new(
            "cluster_lease_keepalive_failures_total",
            "Total number of lease keep-alive failures",
        ))?;
        let lease_keepalive_failure_streak = IntGauge::with_opts(Opts::new(
            "cluster_lease_keepalive_failure_streak",
            "Current consecutive keep-alive failure streak",
        ))?;
        let acquire_retry_attempts = IntCounter::with_opts(Opts::new(
            "cluster_acquire_retry_attempts_total",
            "Total number of acquire retry attempts for shards held by other runners",
        ))?;
        let acquire_retry_window_exhausted = IntCounter::with_opts(Opts::new(
            "cluster_acquire_retry_window_exhausted_total",
            "Number of times the acquire retry window was exhausted",
        ))?;

        registry.register(Box::new(entities.clone()))?;
        registry.register(Box::new(singletons.clone()))?;
        registry.register(Box::new(runners.clone()))?;
        registry.register(Box::new(runners_healthy.clone()))?;
        registry.register(Box::new(shards.clone()))?;
        registry.register(Box::new(sharding_detached.clone()))?;
        registry.register(Box::new(lease_keepalive_failures.clone()))?;
        registry.register(Box::new(lease_keepalive_failure_streak.clone()))?;
        registry.register(Box::new(acquire_retry_attempts.clone()))?;
        registry.register(Box::new(acquire_retry_window_exhausted.clone()))?;

        Ok(Self {
            entities,
            singletons,
            runners,
            runners_healthy,
            shards,
            sharding_detached,
            lease_keepalive_failures,
            lease_keepalive_failure_streak,
            acquire_retry_attempts,
            acquire_retry_window_exhausted,
        })
    }

    /// Create metrics without registering (for testing).
    pub fn unregistered() -> Self {
        Self {
            entities: IntGauge::new("cluster_entities", "entities").expect("valid metric name"),
            singletons: IntGauge::new("cluster_singletons", "singletons")
                .expect("valid metric name"),
            runners: IntGauge::new("cluster_runners", "runners").expect("valid metric name"),
            runners_healthy: IntGauge::new("cluster_runners_healthy", "healthy")
                .expect("valid metric name"),
            shards: IntGauge::new("cluster_shards", "shards").expect("valid metric name"),
            sharding_detached: IntGauge::new("cluster_sharding_detached", "detached")
                .expect("valid metric name"),
            lease_keepalive_failures: IntCounter::new(
                "cluster_lease_keepalive_failures_total",
                "failures",
            )
            .expect("valid metric name"),
            lease_keepalive_failure_streak: IntGauge::new(
                "cluster_lease_keepalive_failure_streak",
                "streak",
            )
            .expect("valid metric name"),
            acquire_retry_attempts: IntCounter::new(
                "cluster_acquire_retry_attempts_total",
                "retries",
            )
            .expect("valid metric name"),
            acquire_retry_window_exhausted: IntCounter::new(
                "cluster_acquire_retry_window_exhausted_total",
                "exhausted",
            )
            .expect("valid metric name"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unregistered_metrics_work() {
        let m = ClusterMetrics::unregistered();
        m.entities.set(5);
        assert_eq!(m.entities.get(), 5);
    }

    #[test]
    fn registered_metrics_work() {
        let r = Registry::new();
        let m = ClusterMetrics::new(&r).unwrap();
        m.shards.set(10);
        assert_eq!(m.shards.get(), 10);
    }
}
