use lucid::kvstore::{KvStore, SetError};

const CIPHER: std::option::Option<[&str; 2]> = Some([
    "123456789012345678901234123456789012345678901234",
    "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff",
]);

const DATA: [u8; 512] = [42u8; 512];

const KEY: &str = "test_value";

fn init_kv() -> KvStore {
    let kv = KvStore::new(CIPHER);
    assert!(matches!(
        kv.set(KEY.to_string(), DATA.to_vec(), None),
        Ok(None)
    ));
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
    fn set_returns_none_when_creating_a_key() {
        let kv = KvStore::new(None);
        let result = kv.set(KEY.to_string(), DATA.to_vec(), None);

        assert!(matches!(result, Ok(None)));
    }

    #[test]
    fn set_rejects_a_locked_key_without_updating_it() {
        let kv = init_kv();
        let before = kv.get(KEY.to_string()).unwrap();

        assert!(kv.switch_lock(KEY.to_string(), true));

        let result = kv.set(KEY.to_string(), b"replacement".to_vec(), None);
        assert!(matches!(result, Err(SetError::Locked)));

        let after = kv.get(KEY.to_string()).unwrap();
        assert_eq!(after.data, before.data);
        assert_eq!(after.mime_type, before.mime_type);
        assert_eq!(after.created_at, before.created_at);
        assert_eq!(after.updated_at, before.updated_at);
        assert_eq!(after.expire_at, before.expire_at);
        assert_eq!(after.update_count, before.update_count);
        assert!(after.locked);
    }

    #[test]
    fn set_updates_an_unlocked_key() {
        let kv = init_kv();
        assert!(kv.switch_lock(KEY.to_string(), true));
        assert!(kv.switch_lock(KEY.to_string(), false));

        let result = kv.set(KEY.to_string(), b"replacement".to_vec(), None);
        assert!(matches!(result, Ok(Some(_))));
        assert_eq!(kv.get(KEY.to_string()).unwrap().data, b"replacement");
    }
}
