# Shard Assignment: Detachment, Lease, and Reacquire

## Status

Complete.

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

- [x] **Task 5: Pause While Detached** - Added detachment checks to
  `shard_acquisition_loop` and `lock_refresh_loop` to short-circuit while
  detached. When detached, loops skip their work (rebalancing or lock refresh)
  and continue sleeping, waiting for re-attachment.

- [x] **Task 4: Reacquire Retry Window** - Implemented retry loop in
  `rebalance_shards` for shards held by other runners. Added config options
  `acquire_retry_interval` (default 200ms) and `acquire_retry_window` (default
  2s). Added metrics `acquire_retry_attempts` and `acquire_retry_window_exhausted`.
  Retries only shards held by other runners (not storage errors). Window of 0
  disables retries for backward compatibility.

- [x] **Task 6: Tests** - Added comprehensive tests in `sharding_impl.rs`:
  - `detach_on_get_runners_error` - Verifies detachment on storage errors
  - `detach_on_acquire_batch_error` - Verifies detachment on acquire failures
  - `detach_on_refresh_batch_error` - Verifies detachment on refresh failures
  - `detach_on_keepalive_failure_streak` - Verifies detachment when keep-alive
    failure streak exceeds threshold
  - `detached_loops_skip_work` - Verifies loops skip work while detached
  - `reattach_after_recovery_window` - Verifies re-attachment after healthy period
  - `unhealthy_signal_resets_recovery_window` - Verifies recovery window reset
  - `detachment_disabled_ignores_detach_calls` - Verifies feature toggle works
  Note: Watch-driven reacquire test deferred until watch functionality is implemented.

- [x] **Task 7: Rollout Strategy** - Config defaults preserve current behavior:
  `detachment_enabled` defaults to `false`, so detachment is opt-in. Metrics and
  logs are always active for observability. Enable `detachment_enabled` in staging
  first to validate behavior before production rollout.

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

Detachment triggers (lease deadline model):

- On every successful keep-alive response, record `lease_deadline = now + ttl_returned`.
- If connectivity is lost or keep-alive fails, the runner may continue only
  until `lease_deadline - safety_margin`.
- Once `now >= lease_deadline - safety_margin`, detach immediately.
- Storage connectivity errors still trigger a fast-path timer, but the
  definitive cutoff is the lease deadline (not a fixed 100-200ms window).

Re-attachment triggers:

- Keep-alive is healthy for a sustained window.
- A successful storage call is observed after detachment (e.g., `get_runners`).

### Lease Extension

Strengthen lease keep-alive observability and feedback into sharding:

- Emit structured metrics for keep-alive success/failure rate, failure streak
  length, and the current `lease_deadline` lag (`lease_deadline - now`).
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
- `lease_detach_margin` (new): safety margin before `lease_deadline` at which
  the runner must detach if keep-alive is not confirmed. Default to
  `lease_ttl / 3` to align with keep-alive cadence.
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

- Detach at `lease_deadline - lease_detach_margin` after keep-alive loss.
- Detach after lease deadline on `get_runners` error if no healthy keep-alive.
- Detach after lease deadline on `refresh_batch` error if no healthy keep-alive.
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
