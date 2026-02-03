# Shard Assignment: Detachment, Lease, and Reacquire

## Status

In Progress.

## Completed Tasks

- [x] **Task 1: Introduce Detachment State** - Added `detachment.rs` module with
  `DetachedState` struct, `DetachmentReason` enum, and integration into
  `ShardingImpl`. Added config options `detachment_window`,
  `detachment_recover_window`, and `detachment_enabled`. Added
  `sharding_detached` metric.

- [x] **Task 2: Wire Keep-Alive Health to Sharding** - Extended etcd runner
  storage keep-alive loop to publish `LeaseHealth` updates via broadcast
  channel. Added `lease_health_receiver()` method to `RunnerStorage` trait.
  `ShardingImpl` now subscribes to lease health during `start()` and triggers
  detachment when failure streak exceeds `keepalive_failure_threshold`. Added
  metrics `lease_keepalive_failures` (counter) and `lease_keepalive_failure_streak`
  (gauge). Logs transitions between healthy and degraded states.

- [x] **Task 3: Detachment on Storage Errors** - Modified `rebalance_shards` to
  trigger detachment when `get_runners` or `acquire_batch` return storage errors.
  Modified `lock_refresh_loop` to trigger detachment when `refresh_batch` returns
  a top-level error. Added `signal_healthy()` calls after successful storage
  operations to support re-attachment.

## Problem Statement

The cluster currently allows windows where a runner continues executing shard
entities after its lock has expired or storage connectivity has been lost. This
violates the single-runner principle and allows a shard to run concurrently in
multiple places.

Key issues observed today:
- Lock refresh only verifies ownership; it does not extend TTL.
- If storage connectivity is lost, the runner keeps its owned shard set and
  continues processing.
- Shard acquisition is attempted once per rebalance cycle; assigned shards that
  are temporarily held elsewhere can remain unowned for too long.

## Goals

1. Detachment must be detected quickly and reliably.
2. When detached, a runner must stop executing shard entities immediately.
3. Lease extension must be robust and observable.
4. Assigned shards must be actively reacquired until successful.
5. Behavior should be deterministic and auditable via logs and metrics.

## Non-Goals

- Fencing tokens are not used because entities can call arbitrary endpoints.
- We do not attempt to prevent external side-effects beyond detachment.

## Definitions

- Detachment: The runner is not safe to execute shards because its storage
  lease is uncertain or expired.
- Lease health: Ongoing ability to refresh the runner lease in the storage
  backend (etcd).
- Assigned shards: Shards that deterministic assignment maps to this runner.

## Current Behavior (Reference)

- `lock_refresh_loop` checks ownership and removes shards if the storage says
  the lock is lost.
- Lease TTL is extended by etcd keep-alive, independent of shard refresh.
- `rebalance_shards` computes desired assignments and attempts to acquire
  assigned shards once per cycle.

## Proposed Behavior

### Detachment

Add a sharding-level detachment state that is triggered on any storage signal
indicating the runner may be detached. While detached:

- `owned_shards` is cleared.
- All shard entities are interrupted.
- Shard acquisition and refresh loops pause or no-op.
- The runner is treated as having zero shards until re-attached.

Detachment triggers (fast threshold):

- Any storage connectivity error starts a short detachment timer (100-200ms).
- If connectivity is not restored within the window, detach immediately.
- Keep-alive failure streak uses the same window and detaches if the lease
  cannot be confirmed healthy within that window.

Re-attachment triggers:

- Keep-alive is healthy for a sustained window.
- A successful storage call is observed after detachment (e.g., `get_runners`).

### Lease Extension

Strengthen lease keep-alive observability and feedback into sharding:

- Emit structured metrics for keep-alive success/failure rate and failure
  streak length.
- Emit a log on every keep-alive streak transition (healthy -> degraded, and
  degraded -> healthy).
- Keep-alive task publishes status into a shared channel/atomic used by
  sharding to determine detachment.

### Reacquire Behavior

Assigned shards should be aggressively reacquired:

- On each rebalance cycle, after computing `to_acquire`, retry immediately for
  shards still not acquired.
- Prefer watching shard lock keys for deletes and act on deletion rather than
  waiting for the next polling interval.
- If acquisition fails due to storage errors, detach immediately.
- If acquisition fails only because shards are held elsewhere, keep retrying
  at a short interval and use watch-based wakeups to avoid polling delays.

### Ordering and Safety

- Detachment has higher priority than any acquisition or refresh action.
- Any transition to detached state must interrupt entities before further
  cluster operations continue.

## Configuration

Add or reuse the following settings (defaults to be tuned):

- `runner_lock_refresh_interval` (existing): how often to verify locks.
- `shard_rebalance_retry_interval` (existing): standard rebalance cadence.
- `shard_rebalance_debounce` (existing): avoid thrashing on topology changes.
- `lease_ttl` (existing in etcd runner storage): TTL for registration lease.
- `detachment_window` (new): short window (100-200ms) to confirm connectivity
  before detaching.
- `detachment_recover_window` (new): duration of healthy status required to
  re-attach.
- `acquire_retry_interval` (new): short interval between acquire retries.
- `acquire_watch_enabled` (new): watch shard lock key deletions to trigger
  immediate re-acquire.

## Observability

Metrics:
- `sharding.detached` (gauge, 0/1)
- `sharding.lease_keepalive_failures` (counter)
- `sharding.lease_keepalive_failure_streak` (gauge)
- `sharding.acquire_retry_attempts` (counter)
- `sharding.acquire_retry_window_exhausted` (counter)

Logs:
- Detach and attach transitions (info/warn).
- Keep-alive streak transitions (warn on degrade, info on recover).
- Acquire retry exhaustion (warn).

## Implementation Plan

### 1) Introduce Detachment State

- Add a shared `DetachedState` to `ShardingImpl` (atomic + timestamp + reason).
- Provide methods:
  - `detach(reason)`
  - `is_detached()`
  - `maybe_reattach()`

### 2) Wire Keep-Alive Health to Sharding

- Extend etcd runner storage keep-alive loop to publish health updates (e.g.,
  `LeaseHealth { healthy: bool, failure_streak: u32 }`).
- Sharding listens and triggers `detach` when thresholds are exceeded.

### 3) Detachment on Storage Errors

- In `rebalance_shards`, if `get_runners` errors or `acquire_batch` returns
  storage failures, call `detach` and return early.
- In `lock_refresh_loop`, if `refresh_batch` returns a top-level error,
  call `detach` and return early.

### 4) Reacquire Retry Window

- After initial `acquire_batch`, retry within `acquire_retry_window` for any
  shards still assigned to us but not acquired.
- Retry only for shards held by other runners; storage errors should detach.
- If retries are exhausted, reduce rebalance interval for the next cycle.

### 5) Pause While Detached

- In `shard_acquisition_loop` and `lock_refresh_loop`, short-circuit while
  detached and sleep on a small interval or wait for health recovery signal.

### 6) Tests

Add tests to `crates/cruster/src/sharding_impl.rs`:

- Detach after detachment window on `get_runners` error.
- Detach after detachment window on `refresh_batch` error.
- Detach after detachment window on keep-alive failure streak.
- Reacquire retries pick up immediately after lock deletion (watch-driven).
- While detached, `owned_shards` stays empty and entities are interrupted.

### 7) Rollout Strategy

- Add config defaults that preserve current behavior unless enabled.
- Start by enabling metrics and logs, then enable detachment and retry window
  in staging before production.

## Open Questions

- Should detachment be immediate on any storage error, or only after a short
  failure window?
- How aggressive should acquire retries be under large shard counts?
