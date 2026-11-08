use blake3::Hasher;

fn hash_components(components: &[&str]) -> blake3::Hash {
    let mut hasher = Hasher::new();

    for component in components {
        // Length-prefix each component so ("ab", "c") and ("a", "bc") do not collide.
        hasher.update(&(component.len() as u64).to_le_bytes());
        hasher.update(component.as_bytes());
    }

    hasher.finalize()
}

pub fn stable_entity_id(
    namespace: &str,
    host: &str,
    entity_type: &str,
    external_id: &str,
) -> String {
    hash_components(&[namespace, host, entity_type, external_id])
        .to_hex()
        .to_string()
}

pub fn stable_entity_id_u64(
    namespace: &str,
    host: &str,
    entity_type: &str,
    external_id: &str,
) -> u64 {
    let digest = hash_components(&[namespace, host, entity_type, external_id]);
    let bytes = digest.as_bytes();
    let mut prefix = [0u8; 8];
    prefix.copy_from_slice(&bytes[..8]);
    u64::from_be_bytes(prefix)
}

pub fn stable_host_id(namespace: &str, host: &str) -> u32 {
    let digest = hash_components(&[namespace, host]);
    let bytes = digest.as_bytes();
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

#[cfg(test)]
mod tests {
    use super::{stable_entity_id, stable_entity_id_u64, stable_host_id};

    #[test]
    fn stable_id_is_deterministic() {
        let first = stable_entity_id("toy", "host-a", "PROCESS", "p101");
        let second = stable_entity_id("toy", "host-a", "PROCESS", "p101");

        assert_eq!(first, second);
    }

    #[test]
    fn stable_id_changes_when_entity_changes() {
        let first = stable_entity_id("toy", "host-a", "PROCESS", "p101");
        let second = stable_entity_id("toy", "host-a", "PROCESS", "p102");

        assert_ne!(first, second);
    }

    #[test]
    fn component_boundaries_do_not_collide() {
        let first = stable_entity_id("toy", "ab", "c", "d");
        let second = stable_entity_id("toy", "a", "bc", "d");

        assert_ne!(first, second);
    }

    #[test]
    fn numeric_id_matches_hex_prefix() {
        let hex = stable_entity_id("toy", "host-a", "PROCESS", "p101");
        let numeric = stable_entity_id_u64("toy", "host-a", "PROCESS", "p101");

        assert_eq!(hex[..16], format!("{numeric:016x}"));
    }

    #[test]
    fn host_ids_are_stable() {
        assert_eq!(
            stable_host_id("toy", "host-a"),
            stable_host_id("toy", "host-a")
        );
        assert_ne!(
            stable_host_id("toy", "host-a"),
            stable_host_id("toy", "host-b")
        );
    }
}
