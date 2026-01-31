use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// A resource that survives entity restarts (shard movement, crash recovery).
///
/// When an entity handler is respawned due to a crash, the `EntityResource` is
/// NOT closed — only the handler is recreated. The resource is only closed on
/// explicit `close()` or when the entity is reaped.
///
/// While an `EntityResource` is active (not closed), the entity's `on_idle()`
/// should return `true` to prevent idle reaping. Use `is_active()` to check.
///
/// # Example
///
/// ```text
/// let resource = EntityResource::new(MyConnection::new().await?);
/// // Use the resource
/// let conn = resource.read().await;
/// conn.query("SELECT 1").await?;
/// // Check if active for keep-alive
/// if resource.is_active() {
///     // Entity should not be reaped
/// }
/// // Explicit close when done
/// resource.close().await;
/// ```
pub struct EntityResource<T> {
    value: Arc<RwLock<Option<T>>>,
    active: Arc<AtomicBool>,
    close_token: CancellationToken,
}

impl<T> EntityResource<T> {
    /// Create a new entity resource wrapping the given value.
    pub fn new(value: T) -> Self {
        Self {
            value: Arc::new(RwLock::new(Some(value))),
            active: Arc::new(AtomicBool::new(true)),
            close_token: CancellationToken::new(),
        }
    }

    /// Returns `true` if the resource has not been closed.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Read access to the resource value.
    ///
    /// Returns `None` if the resource has been closed.
    pub async fn read(&self) -> Option<tokio::sync::RwLockReadGuard<'_, Option<T>>> {
        let guard = self.value.read().await;
        if guard.is_some() {
            Some(guard)
        } else {
            None
        }
    }

    /// Write access to the resource value.
    ///
    /// Returns `None` if the resource has been closed.
    pub async fn write(&self) -> Option<tokio::sync::RwLockWriteGuard<'_, Option<T>>> {
        let guard = self.value.write().await;
        if guard.is_some() {
            Some(guard)
        } else {
            None
        }
    }

    /// Close the resource, dropping the inner value and marking it inactive.
    ///
    /// After closing, `is_active()` returns `false` and `read()`/`write()`
    /// return `None`. The entity can then be reaped on idle.
    pub async fn close(&self) {
        let mut guard = self.value.write().await;
        *guard = None;
        self.active.store(false, Ordering::Release);
        self.close_token.cancel();
    }

    /// Returns a `CancellationToken` that is cancelled when the resource is
    /// closed. Useful for tying background tasks to the resource lifetime.
    pub fn close_token(&self) -> CancellationToken {
        self.close_token.clone()
    }
}

impl<T> Clone for EntityResource<T> {
    fn clone(&self) -> Self {
        Self {
            value: Arc::clone(&self.value),
            active: Arc::clone(&self.active),
            close_token: self.close_token.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn new_resource_is_active() {
        let resource = EntityResource::new(42i32);
        assert!(resource.is_active());
    }

    #[tokio::test]
    async fn read_returns_value() {
        let resource = EntityResource::new(42i32);
        let guard = resource.read().await.unwrap();
        assert_eq!(*guard, Some(42));
    }

    #[tokio::test]
    async fn write_allows_mutation() {
        let resource = EntityResource::new(42i32);
        {
            let mut guard = resource.write().await.unwrap();
            *guard = Some(99);
        }
        let guard = resource.read().await.unwrap();
        assert_eq!(*guard, Some(99));
    }

    #[tokio::test]
    async fn close_marks_inactive() {
        let resource = EntityResource::new(42i32);
        resource.close().await;
        assert!(!resource.is_active());
    }

    #[tokio::test]
    async fn read_after_close_returns_none() {
        let resource = EntityResource::new(42i32);
        resource.close().await;
        assert!(resource.read().await.is_none());
    }

    #[tokio::test]
    async fn write_after_close_returns_none() {
        let resource = EntityResource::new(42i32);
        resource.close().await;
        assert!(resource.write().await.is_none());
    }

    #[tokio::test]
    async fn close_token_is_cancelled_on_close() {
        let resource = EntityResource::new(42i32);
        let token = resource.close_token();
        assert!(!token.is_cancelled());
        resource.close().await;
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn clone_shares_state() {
        let resource = EntityResource::new(42i32);
        let cloned = resource.clone();
        assert!(cloned.is_active());
        resource.close().await;
        assert!(!cloned.is_active());
        assert!(cloned.read().await.is_none());
    }

    #[tokio::test]
    async fn close_is_idempotent() {
        let resource = EntityResource::new(42i32);
        resource.close().await;
        resource.close().await;
        assert!(!resource.is_active());
    }
}
