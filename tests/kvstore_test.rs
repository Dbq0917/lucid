use lucid::kvstore::KvStore;

const CIPHER: std::option::Option<[&str; 2]> = Some([
    "123456789012345678901234123456789012345678901234",
    "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff",
]);

const DATA: [u8; 512] = [42u8; 512];

const KEY: &str = "test_value";

fn init_kv() -> KvStore {
    let kv = KvStore::new(CIPHER);
    kv.set(KEY.to_string(), DATA.to_vec(), None);
    kv
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_a_value() {
        let kv = init_kv();
        let value = kv.get(KEY.to_string());

        match value {
            Some(v) => assert_eq!(v.data, DATA.to_vec()),
            None => panic!("No value found"),
        }
    }

    #[test]
    fn capacity_starts_at_zero() {
        let kv = KvStore::new(None);

        assert_eq!(kv.capacity(), 0);
    }

    #[test]
    fn capacity_uses_the_stored_value_size() {
        let kv = KvStore::new(CIPHER);
        kv.set(KEY.to_string(), vec![42u8; 100], None);

        assert_eq!(kv.capacity(), 112);
    }

    #[test]
    fn capacity_decreases_when_half_of_the_keys_are_deleted() {
        let kv = KvStore::new(None);

        for index in 0..100 {
            kv.set(format!("key_{}", index), vec![42u8; 128], None);
        }
        assert_eq!(kv.capacity(), 12_800);

        for index in 0..50 {
            kv.drop(format!("key_{}", index));
        }
        assert_eq!(kv.capacity(), 6_400);
    }

    #[test]
    fn capacity_only_tracks_the_current_value_after_overwrite() {
        let kv = KvStore::new(None);

        kv.set(KEY.to_string(), vec![42u8; 128], None);
        kv.set(KEY.to_string(), vec![42u8; 512], None);

        assert_eq!(kv.capacity(), 512);
    }

    #[test]
    fn capacity_does_not_change_when_a_locked_value_is_replaced() {
        let kv = KvStore::new(None);

        kv.set(KEY.to_string(), vec![42u8; 128], None);
        kv.switch_lock(KEY.to_string(), true);
        kv.set(KEY.to_string(), vec![42u8; 512], None);

        assert_eq!(kv.capacity(), 128);
    }

    #[test]
    fn dropping_a_missing_key_does_not_change_capacity() {
        let kv = init_kv();
        let capacity = kv.capacity();

        kv.drop("missing_key".to_string());

        assert_eq!(kv.capacity(), capacity);
    }

    #[test]
    fn capacity_returns_to_zero_after_repeated_session_churn() {
        let kv = KvStore::new(None);

        for index in 0..1_000 {
            let key = format!("session_{}", index);
            kv.set(key.clone(), vec![42u8; 256], None);
            kv.drop(key);
        }

        assert_eq!(kv.capacity(), 0);
    }

    #[test]
    fn capacity_tracks_value_size_changes_from_increment() {
        let kv = KvStore::new(None);
        kv.set("counter".to_string(), b"9".to_vec(), None);

        assert!(kv.increment_or_decrement("counter".to_string(), 1.0));
        assert_eq!(kv.capacity(), 2);
    }
}
