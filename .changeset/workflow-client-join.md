---
default: patch
---

feat: add `join()` to workflow clients

Workflow clients now support `join(execution_id)` which blocks until the
workflow result is available, unlike `poll()` which returns `Option`
immediately. Backed by a new `await_reply` method on `Sharding` that
uses `register_reply_handler` for real-time push notification.
