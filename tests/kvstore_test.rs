use std::sync::Arc;
use std::thread;

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
        let value = kv.get(KEY);

        match value {
            Some(v) => assert_eq!(v.data, DATA.to_vec()),
            None => panic!("No value found"),
        }
    }

    #[test]
    fn eight_threads_can_read_the_same_key_concurrently() {
        let kv = Arc::new(init_kv());
        let mut handles = Vec::new();

        for _ in 0..8 {
            let kv = kv.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..1_000 {
                    let value = kv.get(KEY).expect("No value found");
                    assert_eq!(value.data, DATA.to_vec());
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn concurrent_writes_remain_visible() {
        let kv = Arc::new(KvStore::new(None));
        let mut handles = Vec::new();

        for shard in 0..8u8 {
            let kv = kv.clone();
            handles.push(thread::spawn(move || {
                for index in 0..100u8 {
                    let key = format!("key-{}-{}", shard, index);
                    let data = vec![shard.wrapping_add(index); 64];
                    kv.set(key.clone(), data.clone(), None);

                    let value = kv.get(&key).expect("No value found");
                    assert_eq!(value.data, data);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(kv.get("key-0-0").unwrap().data, vec![0; 64]);
        assert_eq!(kv.get("key-7-99").unwrap().data, vec![106; 64]);
    }

    #[test]
    fn increments_on_the_same_key_are_atomic() {
        let kv = Arc::new(KvStore::new(None));
        kv.set("counter".to_string(), b"0".to_vec(), None);
        let mut handles = Vec::new();

        for _ in 0..8 {
            let kv = kv.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..1_000 {
                    assert!(kv.increment_or_decrement("counter".to_string(), 1.0));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let value = kv.get("counter").unwrap();
        assert_eq!(String::from_utf8(value.data).unwrap(), "8000");
    }
}
