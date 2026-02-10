//! Cluster cron job scheduling.
//!
//! Distributed cron jobs that run as cluster singletons. The singleton triggers
//! the initial run, and each execution self-schedules the next one.
//!
//! Ported from Effect's `ClusterCron.ts`.

use crate::error::ClusterError;
use crate::sharding::Sharding;
use chrono::{DateTime, Utc};
use cron::Schedule;
use futures::future::BoxFuture;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing;

/// A parsed cron schedule wrapping the `cron` crate's [`Schedule`].
#[derive(Debug, Clone)]
pub struct CronSchedule {
    schedule: Schedule,
    expression: String,
}

impl CronSchedule {
    /// Parse a cron expression string into a `CronSchedule`.
    ///
    /// Uses standard cron syntax with seconds field (7 fields):
    /// `sec min hour day_of_month month day_of_week year`
    ///
    /// # Examples
    ///
    /// ```
    /// use cruster::cron::CronSchedule;
    ///
    /// // Every 5 minutes
    /// let schedule = CronSchedule::parse("0 */5 * * * * *").unwrap();
    /// assert!(schedule.next_after(chrono::Utc::now()).is_some());
    /// ```
    pub fn parse(expression: &str) -> Result<Self, ClusterError> {
        let schedule =
            Schedule::from_str(expression).map_err(|e| ClusterError::MalformedMessage {
                reason: format!("invalid cron expression '{expression}': {e}"),
                source: Some(Box::new(e)),
            })?;
        Ok(Self {
            schedule,
            expression: expression.to_string(),
        })
    }

    /// Get the next occurrence after the given datetime.
    pub fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.schedule.after(&after).next()
    }

    /// Get the cron expression string.
    pub fn expression(&self) -> &str {
        &self.expression
    }
}

impl std::fmt::Display for CronSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CronSchedule({})", self.expression)
    }
}

/// Type alias for a cron handler function.
pub type CronHandler =
    dyn Fn(DateTime<Utc>) -> BoxFuture<'static, Result<(), ClusterError>> + Send + Sync;

/// Configuration for a cluster cron job.
pub struct ClusterCron {
    /// Name of the cron job (must be unique within the cluster).
    pub name: String,
    /// Cron schedule for when to run.
    pub schedule: CronSchedule,
    /// Handler function called at each scheduled time.
    /// Receives the scheduled datetime (may differ from wall clock if delayed).
    pub handler: Box<CronHandler>,
    /// Whether to calculate the next run from the previous scheduled time
    /// (true) or from the current wall-clock time (false). Default: false.
    pub calculate_next_from_previous: bool,
    /// Skip execution if the scheduled time is older than this duration.
    /// Prevents running stale jobs after prolonged downtime. Default: 1 day.
    pub skip_if_older_than: Duration,
}

impl ClusterCron {
    /// Create a new cron job builder.
    pub fn new(
        name: impl Into<String>,
        schedule: CronSchedule,
        handler: impl Fn(DateTime<Utc>) -> BoxFuture<'static, Result<(), ClusterError>>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            schedule,
            handler: Box::new(handler),
            calculate_next_from_previous: false,
            skip_if_older_than: Duration::from_secs(86400), // 1 day
        }
    }

    /// Set whether to calculate next run from the previous scheduled time.
    pub fn with_calculate_next_from_previous(mut self, value: bool) -> Self {
        self.calculate_next_from_previous = value;
        self
    }

    /// Set the maximum age for a scheduled run to be executed.
    pub fn with_skip_if_older_than(mut self, duration: Duration) -> Self {
        self.skip_if_older_than = duration;
        self
    }

    /// Register this cron job with the cluster as a singleton.
    ///
    /// The singleton runs a loop that:
    /// 1. Computes the next scheduled time from the cron expression.
    /// 2. Sleeps until that time.
    /// 3. Calls the handler with the scheduled datetime.
    /// 4. On completion (success or failure), schedules the next run.
    /// 5. Skips execution if the scheduled time is too far in the past.
    ///
    /// The singleton is automatically cancelled on cluster shutdown.
    pub async fn register(self, sharding: &dyn Sharding) -> Result<(), ClusterError> {
        let name = format!("ClusterCron/{}", self.name);
        let cron_name = self.name.clone();
        let schedule = self.schedule.clone();
        let handler = Arc::new(self.handler);
        let calculate_next_from_previous = self.calculate_next_from_previous;
        let skip_if_older_than = self.skip_if_older_than;

        sharding
            .register_singleton(
                &name,
                None,
                Arc::new(
                    move |_ctx| -> BoxFuture<'static, Result<(), ClusterError>> {
                        let cron_name = cron_name.clone();
                        let schedule = schedule.clone();
                        let handler = Arc::clone(&handler);
                        Box::pin(async move {
                            cron_loop(
                                &cron_name,
                                &schedule,
                                handler,
                                calculate_next_from_previous,
                                skip_if_older_than,
                            )
                            .await
                        })
                    },
                ),
            )
            .await
    }
}

/// Internal cron execution loop.
async fn cron_loop(
    name: &str,
    schedule: &CronSchedule,
    handler: Arc<Box<CronHandler>>,
    calculate_next_from_previous: bool,
    skip_if_older_than: Duration,
) -> Result<(), ClusterError> {
    let mut reference_time = Utc::now();

    loop {
        let next = match schedule.next_after(reference_time) {
            Some(t) => t,
            None => {
                tracing::warn!(cron = name, "no more scheduled times, stopping cron job");
                return Ok(());
            }
        };

        // Sleep until the scheduled time
        let now = Utc::now();
        if next > now {
            let delay = match (next - now).to_std() {
                Ok(d) => d,
                Err(_) => {
                    tracing::warn!(
                        cron = name,
                        duration = ?(next - now),
                        fallback = ?Duration::ZERO,
                        "cron delay conversion failed (negative duration), clamping to zero"
                    );
                    Duration::ZERO
                }
            };
            tokio::time::sleep(delay).await;
        }

        // Check if the scheduled time is too old
        let now = Utc::now();
        let age = now.signed_duration_since(next);
        let skip_threshold = match chrono::Duration::from_std(skip_if_older_than) {
            Ok(d) => d,
            Err(_) => {
                tracing::warn!(
                    cron = name,
                    skip_if_older_than = ?skip_if_older_than,
                    fallback = "1 day",
                    "skip_if_older_than conversion failed, using 1 day fallback"
                );
                chrono::Duration::days(1)
            }
        };

        if age > skip_threshold {
            tracing::warn!(
                cron = name,
                scheduled = %next,
                "skipping cron execution: scheduled time is too old"
            );
        } else {
            tracing::info!(cron = name, scheduled = %next, "executing cron job");
            match handler(next).await {
                Ok(()) => {
                    tracing::debug!(cron = name, "cron job completed successfully");
                }
                Err(e) => {
                    tracing::warn!(cron = name, error = %e, "cron job failed, scheduling next run");
                }
            }
        }

        // Calculate reference for next run
        if calculate_next_from_previous {
            reference_time = next;
        } else {
            reference_time = Utc::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ShardingConfig;
    use crate::metrics::ClusterMetrics;
    use crate::sharding_impl::ShardingImpl;
    use crate::storage::noop_runners::NoopRunners;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Create a minimal ShardingImpl with NoopRunners for unit tests.
    async fn test_sharding() -> Arc<ShardingImpl> {
        let config = Arc::new(ShardingConfig::default());
        let metrics = Arc::new(ClusterMetrics::unregistered());
        let s = ShardingImpl::new(
            config,
            Arc::new(NoopRunners),
            None,
            None,
            None,
            metrics,
        )
        .unwrap();
        s.acquire_all_shards().await;
        s
    }

    #[test]
    fn parse_valid_cron_expression() {
        let schedule = CronSchedule::parse("0 */5 * * * * *").unwrap();
        assert_eq!(schedule.expression(), "0 */5 * * * * *");
        assert!(schedule.next_after(Utc::now()).is_some());
    }

    #[test]
    fn parse_invalid_cron_expression() {
        let result = CronSchedule::parse("not a cron");
        assert!(result.is_err());
        match result.unwrap_err() {
            ClusterError::MalformedMessage { reason, .. } => {
                assert!(reason.contains("invalid cron expression"));
            }
            other => panic!("expected MalformedMessage, got {other:?}"),
        }
    }

    #[test]
    fn cron_schedule_next_after() {
        // Every second
        let schedule = CronSchedule::parse("* * * * * * *").unwrap();
        let now = Utc::now();
        let next = schedule.next_after(now).unwrap();
        assert!(next > now);
        // Should be within 2 seconds
        let diff = next.signed_duration_since(now);
        assert!(diff.num_seconds() <= 2);
    }

    #[test]
    fn cron_schedule_display() {
        let schedule = CronSchedule::parse("0 0 * * * * *").unwrap();
        assert_eq!(schedule.to_string(), "CronSchedule(0 0 * * * * *)");
    }

    #[test]
    fn cluster_cron_builder() {
        let cron = ClusterCron::new(
            "test-job",
            CronSchedule::parse("* * * * * * *").unwrap(),
            |_dt| Box::pin(async { Ok(()) }),
        )
        .with_calculate_next_from_previous(true)
        .with_skip_if_older_than(Duration::from_secs(3600));

        assert_eq!(cron.name, "test-job");
        assert!(cron.calculate_next_from_previous);
        assert_eq!(cron.skip_if_older_than, Duration::from_secs(3600));
    }

    #[tokio::test]
    async fn cron_registers_as_singleton() {
        let sharding = test_sharding().await;

        let cron = ClusterCron::new(
            "register-test",
            CronSchedule::parse("* * * * * * *").unwrap(),
            |_dt| Box::pin(async { Ok(()) }),
        );

        // Should register without error
        cron.register(sharding.as_ref()).await.unwrap();
    }

    #[tokio::test]
    async fn cron_executes_handler() {
        let sharding = test_sharding().await;
        let count = Arc::new(AtomicU32::new(0));
        let count_clone = count.clone();

        // Every second
        let cron = ClusterCron::new(
            "exec-test",
            CronSchedule::parse("* * * * * * *").unwrap(),
            move |_dt| {
                let c = count_clone.clone();
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            },
        );

        cron.register(sharding.as_ref()).await.unwrap();

        // Wait for at least one execution (cron fires every second)
        tokio::time::sleep(Duration::from_millis(1500)).await;

        assert!(
            count.load(Ordering::SeqCst) >= 1,
            "cron should have executed at least once"
        );
    }

    #[test]
    fn cron_schedule_clone() {
        let schedule = CronSchedule::parse("0 0 * * * * *").unwrap();
        let cloned = schedule.clone();
        assert_eq!(schedule.expression(), cloned.expression());
    }
}
