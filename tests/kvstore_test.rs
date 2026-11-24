use lucid::kvstore::KvStore;
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

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

fn persistence_dir(test_name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lucid-{}-{}-{}",
        test_name,
        std::process::id(),
        nonce
    ))
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
    fn values_survive_a_restart() {
        let directory = persistence_dir("restart");

        {
            let kv = KvStore::with_persistence(CIPHER, &directory).unwrap();
            for index in 0..100 {
                kv.set(
                    format!("config-{}", index),
                    format!("value-{}", index).into_bytes(),
                    Some("text/plain".to_string()),
                );
            }
            assert_eq!(kv.len(), 100);
        }

        {
            let kv = KvStore::with_persistence(CIPHER, &directory).unwrap();
            assert_eq!(kv.len(), 100);
            for index in 0..100 {
                let value = kv.get(format!("config-{}", index)).unwrap();
                assert_eq!(
                    value.data,
                    format!("value-{}", index).into_bytes(),
                    "config-{} was not restored",
                    index
                );
            }
        }

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn updates_deletes_and_locks_survive_a_restart() {
        let directory = persistence_dir("mutations");

        {
            let kv = KvStore::with_persistence(None, &directory).unwrap();
            kv.set("updated".to_string(), b"before".to_vec(), None);
            kv.set("deleted".to_string(), b"gone".to_vec(), None);
            kv.set("locked".to_string(), b"fixed".to_vec(), None);
            assert!(kv.switch_lock("locked".to_string(), true));
            kv.set("updated".to_string(), b"after".to_vec(), None);
            kv.drop("deleted".to_string());
            kv.set("expiring".to_string(), b"soon".to_vec(), None);
            kv.set_expiration("expiring".to_string(), 60);
        }

        {
            let kv = KvStore::with_persistence(None, &directory).unwrap();
            assert_eq!(kv.len(), 3);
            assert_eq!(kv.get("updated".to_string()).unwrap().data, b"after");
            assert!(kv.get("deleted".to_string()).is_none());
            assert!(kv.get("locked".to_string()).unwrap().locked);
            assert!(kv.get("expiring".to_string()).unwrap().expire_at > chrono::Utc::now());
        }

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn ignores_an_incomplete_trailing_record() {
        let directory = persistence_dir("partial");

        {
            let kv = KvStore::with_persistence(None, &directory).unwrap();
            kv.set("safe".to_string(), b"value".to_vec(), None);
        }

        let journal = directory.join("store.jsonl");
        let mut contents = fs::read(&journal).unwrap();
        contents.extend_from_slice(br#"{"op":"set","key":"incomplete""#);
        fs::write(&journal, contents).unwrap();

        {
            let kv = KvStore::with_persistence(None, &directory).unwrap();
            assert_eq!(kv.len(), 1);
            assert_eq!(kv.get("safe".to_string()).unwrap().data, b"value");
        }

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn journal_compaction_keeps_the_latest_values() {
        let directory = persistence_dir("compaction");

        {
            let kv = KvStore::with_persistence(None, &directory).unwrap();
            for revision in 0..4200 {
                kv.set(
                    format!("config-{}", revision % 100),
                    format!("revision-{}", revision).into_bytes(),
                    None,
                );
            }
            assert_eq!(kv.len(), 100);
        }

        {
            let kv = KvStore::with_persistence(None, &directory).unwrap();
            assert_eq!(kv.len(), 100);
            for index in 0..100 {
                let value = kv.get(format!("config-{}", index)).unwrap();
                assert_eq!(
                    value.data,
                    format!("revision-{}", 4100 + index).into_bytes()
                );
            }
        }

        fs::remove_dir_all(directory).unwrap();
    }
}
