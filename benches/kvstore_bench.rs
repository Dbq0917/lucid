use std::sync::Arc;
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use lucid::kvstore::KvStore;

const CIPHER: std::option::Option<[&str; 2]> = Some([
    "123456789012345678901234123456789012345678901234",
    "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff",
]);

const DATA: [u8; 1000] = [42u8; 1000];
const CONCURRENT_THREADS: usize = 8;
const READS_PER_THREAD: u64 = 1_000;

fn set_1_kb_data(c: &mut Criterion) {
    let kv = KvStore::new(CIPHER);

    c.bench_function("Set 1KB", |b| {
        b.iter(|| kv.set("bench_one".to_string(), DATA.to_vec(), None))
    });
}
fn get_1_kb_data(c: &mut Criterion) {
    let kv = KvStore::new(CIPHER);

    let k = String::from("bench_one");
    kv.set(k.clone(), DATA.to_vec(), None);

    c.bench_function("Get 1KB", |b| b.iter(|| kv.get(&k)));
}

fn set_1_kb_data_without_encryption(c: &mut Criterion) {
    let kv = KvStore::new(None);

    c.bench_function("Set 1KB (w/o encrytion)", |b| {
        b.iter(|| kv.set("bench_one".to_string(), DATA.to_vec(), None))
    });
}
fn get_1_kb_data_without_encryption(c: &mut Criterion) {
    let kv = KvStore::new(None);

    let k = String::from("bench_one");
    kv.set(k.clone(), DATA.to_vec(), None);

    c.bench_function("Get 1KB (w/o encryption)", |b| b.iter(|| kv.get(&k)));
}

fn get_1_kb_data_with_eight_threads(c: &mut Criterion) {
    let kv = Arc::new(KvStore::new(None));
    let same_key = String::from("bench_same_key");
    kv.set(same_key.clone(), DATA.to_vec(), None);

    let distinct_keys: Vec<String> = (0..8)
        .map(|index| {
            let key = format!("bench_key_{}", index);
            kv.set(key.clone(), DATA.to_vec(), None);
            key
        })
        .collect();

    let mut group = c.benchmark_group("Concurrent get with 8 threads");
    group.throughput(Throughput::Elements(
        CONCURRENT_THREADS as u64 * READS_PER_THREAD,
    ));

    group.bench_function("same key", |b| {
        b.iter(|| {
            let mut handles = Vec::with_capacity(CONCURRENT_THREADS);
            for _ in 0..CONCURRENT_THREADS {
                let kv = kv.clone();
                let key = same_key.clone();
                handles.push(thread::spawn(move || {
                    for _ in 0..READS_PER_THREAD {
                        assert!(kv.get(&key).is_some());
                    }
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }
        })
    });

    group.bench_function("different keys", |b| {
        b.iter(|| {
            let mut handles = Vec::with_capacity(CONCURRENT_THREADS);
            for key in distinct_keys.iter() {
                let kv = kv.clone();
                let key = key.clone();
                handles.push(thread::spawn(move || {
                    for _ in 0..READS_PER_THREAD {
                        assert!(kv.get(&key).is_some());
                    }
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    set_1_kb_data,
    get_1_kb_data,
    set_1_kb_data_without_encryption,
    get_1_kb_data_without_encryption,
    get_1_kb_data_with_eight_threads
);
criterion_main!(benches);
