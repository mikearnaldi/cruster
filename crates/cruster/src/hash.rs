/// DJB2 hash function for consistent shard assignment.
/// Produces a deterministic hash for any byte slice.
pub fn djb2_hash(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 5381;
    for &b in bytes {
        hash = hash.wrapping_mul(33).wrapping_add(b as u32);
    }
    hash
}

/// Produces a deterministic 64-bit hash for any byte slice.
pub fn djb2_hash64(bytes: &[u8]) -> u64 {
    djb2_hash64_with_seed(5381, bytes)
}

/// Produces a deterministic 64-bit hash seeded with the provided value.
pub fn djb2_hash64_with_seed(seed: u64, bytes: &[u8]) -> u64 {
    let mut hash = seed;
    for &b in bytes {
        hash = hash.wrapping_mul(33).wrapping_add(b as u64);
    }
    hash
}

/// High-quality 64-bit hash function optimized for uniform distribution.
///
/// Based on xxHash64 algorithm, provides excellent avalanche properties
/// and uniform distribution for rendezvous hashing and similar use cases.
pub fn hash64(bytes: &[u8]) -> u64 {
    const PRIME1: u64 = 0x9E3779B185EBCA87;
    const PRIME2: u64 = 0xC2B2AE3D27D4EB4F;
    const PRIME3: u64 = 0x165667B19E3779F9;
    const PRIME5: u64 = 0x27D4EB2F165667C5;

    let mut h: u64 = PRIME5.wrapping_add(bytes.len() as u64);

    // Process bytes
    for &b in bytes {
        h ^= (b as u64).wrapping_mul(PRIME5);
        h = h.rotate_left(11).wrapping_mul(PRIME1);
    }

    // Final avalanche
    h ^= h >> 33;
    h = h.wrapping_mul(PRIME2);
    h ^= h >> 29;
    h = h.wrapping_mul(PRIME3);
    h ^= h >> 32;
    h
}

/// Compute the shard index for an entity ID within a group.
///
/// Returns a 0-indexed shard index in `[0, shards_per_group)`.
///
/// **Note:** The TypeScript implementation uses 1-indexed shards (`hash % N + 1`).
/// This is intentional — wire compatibility with the TS implementation is a non-goal.
///
/// # Panics
///
/// Panics if `shards_per_group` is less than 1.
pub fn shard_for_entity(entity_id: &str, shards_per_group: i32) -> i32 {
    assert!(
        shards_per_group >= 1,
        "shards_per_group must be >= 1, got {shards_per_group}"
    );
    (djb2_hash(entity_id.as_bytes()) % shards_per_group as u32) as i32
}

/// Compute a deterministic hex-encoded hash of the given bytes.
///
/// Uses a combination of two 64-bit hashes with different seeds to produce
/// a 128-bit (32-character hex) string suitable for idempotency keys.
/// This is NOT cryptographic — it's designed for uniform distribution
/// in entity ID derivation.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let h1 = hash64(bytes);
    // Use djb2 with a different seed as a second independent hash
    let h2 = djb2_hash64_with_seed(0x517cc1b727220a95, bytes);
    format!("{h1:016x}{h2:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic() {
        let h1 = djb2_hash(b"hello");
        let h2 = djb2_hash(b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn different_inputs_differ() {
        let h1 = djb2_hash(b"hello");
        let h2 = djb2_hash(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn djb2_hash64_deterministic() {
        let h1 = djb2_hash64(b"hello");
        let h2 = djb2_hash64(b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn djb2_hash64_seeded_changes_output() {
        let h1 = djb2_hash64_with_seed(5381, b"hello");
        let h2 = djb2_hash64_with_seed(5382, b"hello");
        assert_ne!(h1, h2);
    }

    #[test]
    fn distribution() {
        let num_shards = 300;
        let num_keys = 10_000;
        let mut counts = vec![0u32; num_shards as usize];

        for i in 0..num_keys {
            let key = format!("entity-{i}");
            let shard = shard_for_entity(&key, num_shards);
            counts[shard as usize] += 1;
        }

        let expected = num_keys as f64 / num_shards as f64;
        let max_allowed = (expected * 2.0) as u32;
        for (i, &count) in counts.iter().enumerate() {
            assert!(
                count <= max_allowed,
                "shard {i} has {count} entities, expected at most {max_allowed}"
            );
        }
    }

    #[test]
    #[should_panic(expected = "shards_per_group must be >= 1")]
    fn shard_for_entity_zero_shards_panics() {
        shard_for_entity("test", 0);
    }

    #[test]
    #[should_panic(expected = "shards_per_group must be >= 1")]
    fn shard_for_entity_negative_shards_panics() {
        shard_for_entity("test", -1);
    }

    #[test]
    fn shard_for_entity_in_range() {
        for i in 0..1000 {
            let shard = shard_for_entity(&format!("id-{i}"), 300);
            assert!((0..300).contains(&shard));
        }
    }

    #[test]
    fn hash64_deterministic() {
        let h1 = hash64(b"hello");
        let h2 = hash64(b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash64_different_inputs_differ() {
        let h1 = hash64(b"hello");
        let h2 = hash64(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hash64_avalanche() {
        // Single bit change should produce very different output
        let h1 = hash64(b"test0");
        let h2 = hash64(b"test1");
        // Count differing bits - should be roughly half (good avalanche)
        let diff_bits = (h1 ^ h2).count_ones();
        assert!(
            diff_bits >= 20,
            "expected at least 20 differing bits, got {diff_bits}"
        );
    }
}
