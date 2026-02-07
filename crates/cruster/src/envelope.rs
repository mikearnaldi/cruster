use crate::snowflake::Snowflake;
use crate::types::EntityAddress;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Tagged enum for messages on the wire between runners.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Envelope {
    Request(EnvelopeRequest),
    AckChunk(AckChunk),
    Interrupt(Interrupt),
}

/// Header key indicating a streaming request.
pub const STREAM_HEADER_KEY: &str = "x-cruster-stream";
/// Header value indicating a streaming request.
pub const STREAM_HEADER_VALUE: &str = "1";
/// Header key carrying the envelope's request ID into the handler.
///
/// Injected by the entity manager before dispatching so that macro-generated
/// workflow code can scope activity journals per workflow execution.
pub const REQUEST_ID_HEADER_KEY: &str = "x-cruster-request-id";

/// A request envelope sent between runners.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvelopeRequest {
    pub request_id: Snowflake,
    pub address: EntityAddress,
    /// RPC method name.
    pub tag: String,
    /// MessagePack-encoded request body.
    pub payload: Vec<u8>,
    pub headers: HashMap<String, String>,
    pub span_id: Option<String>,
    pub trace_id: Option<String>,
    pub sampled: Option<bool>,
    /// Whether this message should be persisted to storage before delivery.
    #[serde(default)]
    pub persisted: bool,
    /// Uninterruptibility mode for this request.
    #[serde(default)]
    pub uninterruptible: crate::schema::Uninterruptible,
    /// Optional scheduled delivery time. When set, the message should not be
    /// delivered until this time. `MessageStorage::unprocessed_messages()` should
    /// filter out messages where `deliver_at > now()`.
    #[serde(default)]
    pub deliver_at: Option<DateTime<Utc>>,
}

/// Acknowledgement of a streamed chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckChunk {
    pub request_id: Snowflake,
    pub id: Snowflake,
    pub sequence: i32,
}

/// Request to interrupt processing of an entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Interrupt {
    pub request_id: Snowflake,
    pub address: EntityAddress,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EntityId, EntityType, ShardId};

    fn sample_request() -> EnvelopeRequest {
        EnvelopeRequest {
            request_id: Snowflake(1000),
            address: EntityAddress {
                shard_id: ShardId::new("default", 1),
                entity_type: EntityType::new("User"),
                entity_id: EntityId::new("u-1"),
            },
            tag: "getProfile".into(),
            payload: vec![1, 2, 3],
            headers: HashMap::from([("x-trace".into(), "abc".into())]),
            span_id: Some("span-1".into()),
            trace_id: Some("trace-1".into()),
            sampled: Some(true),
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        }
    }

    #[test]
    fn envelope_request_serde_roundtrip() {
        let req = sample_request();
        let bytes = rmp_serde::to_vec(&Envelope::Request(req.clone())).unwrap();
        let decoded: Envelope = rmp_serde::from_slice(&bytes).unwrap();
        match decoded {
            Envelope::Request(r) => {
                assert_eq!(r.request_id, req.request_id);
                assert_eq!(r.tag, req.tag);
                assert_eq!(r.payload, req.payload);
            }
            _ => panic!("expected Request variant"),
        }
    }

    #[test]
    fn envelope_request_preserves_uninterruptible() {
        let mut req = sample_request();
        req.persisted = true;
        req.uninterruptible = crate::schema::Uninterruptible::Server;

        let bytes = rmp_serde::to_vec(&Envelope::Request(req.clone())).unwrap();
        let decoded: Envelope = rmp_serde::from_slice(&bytes).unwrap();
        match decoded {
            Envelope::Request(r) => {
                assert!(r.persisted);
                assert_eq!(r.uninterruptible, crate::schema::Uninterruptible::Server);
            }
            _ => panic!("expected Request variant"),
        }
    }

    #[test]
    fn envelope_request_preserves_deliver_at() {
        let mut req = sample_request();
        let deliver_time = chrono::Utc::now() + chrono::Duration::hours(1);
        req.deliver_at = Some(deliver_time);

        let bytes = rmp_serde::to_vec(&Envelope::Request(req)).unwrap();
        let decoded: Envelope = rmp_serde::from_slice(&bytes).unwrap();
        match decoded {
            Envelope::Request(r) => {
                assert_eq!(r.deliver_at, Some(deliver_time));
            }
            _ => panic!("expected Request variant"),
        }
    }

    #[test]
    fn envelope_request_deliver_at_defaults_to_none() {
        // Verify that deserialization without deliver_at field defaults to None
        let req = sample_request();
        assert_eq!(req.deliver_at, None);
    }

    #[test]
    fn ack_chunk_serde_roundtrip() {
        let ack = AckChunk {
            request_id: Snowflake(100),
            id: Snowflake(200),
            sequence: 5,
        };
        let bytes = rmp_serde::to_vec(&Envelope::AckChunk(ack.clone())).unwrap();
        let decoded: Envelope = rmp_serde::from_slice(&bytes).unwrap();
        match decoded {
            Envelope::AckChunk(a) => {
                assert_eq!(a.request_id, ack.request_id);
                assert_eq!(a.sequence, ack.sequence);
            }
            _ => panic!("expected AckChunk variant"),
        }
    }

    #[test]
    fn interrupt_serde_roundtrip() {
        let int = Interrupt {
            request_id: Snowflake(300),
            address: EntityAddress {
                shard_id: ShardId::new("default", 2),
                entity_type: EntityType::new("Order"),
                entity_id: EntityId::new("o-1"),
            },
        };
        let bytes = rmp_serde::to_vec(&Envelope::Interrupt(int.clone())).unwrap();
        let decoded: Envelope = rmp_serde::from_slice(&bytes).unwrap();
        match decoded {
            Envelope::Interrupt(i) => {
                assert_eq!(i.request_id, int.request_id);
                assert_eq!(i.address, int.address);
            }
            _ => panic!("expected Interrupt variant"),
        }
    }
}
