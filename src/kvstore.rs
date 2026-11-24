use block_modes::block_padding::ZeroPadding;
use block_modes::{BlockMode, Cbc};
use chashmap::CHashMap;
use chrono::{DateTime, Duration, TimeZone, Utc};
use log::{error, warn};
use serpent::Serpent;
use std::{
    fmt, fs,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration as StdDuration,
};

type SerpentCbc = Cbc<Serpent, ZeroPadding>;

const DEFAULT_JOURNAL_FILE: &str = "store.jsonl";
const COMPACT_AFTER_RECORDS: usize = 4096;
const SYNC_INTERVAL: StdDuration = StdDuration::from_secs(1);

#[derive(Debug, Clone)]
pub struct KvElement {
    pub data: Vec<u8>,
    pub mime_type: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub expire_at: DateTime<Utc>,
    pub update_count: i32,
    pub locked: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedKvElement {
    data: Vec<u8>,
    mime_type: String,
    created_at: i64,
    updated_at: i64,
    expire_at: i64,
    update_count: i32,
    locked: bool,
}

impl From<&KvElement> for PersistedKvElement {
    fn from(element: &KvElement) -> Self {
        PersistedKvElement {
            data: element.data.clone(),
            mime_type: element.mime_type.clone(),
            created_at: element.created_at.timestamp_millis(),
            updated_at: element.updated_at.timestamp_millis(),
            expire_at: element.expire_at.timestamp_millis(),
            update_count: element.update_count,
            locked: element.locked,
        }
    }
}

impl From<PersistedKvElement> for KvElement {
    fn from(element: PersistedKvElement) -> Self {
        KvElement {
            data: element.data,
            mime_type: element.mime_type,
            created_at: Utc.timestamp_millis(element.created_at),
            updated_at: Utc.timestamp_millis(element.updated_at),
            expire_at: Utc.timestamp_millis(element.expire_at),
            update_count: element.update_count,
            locked: element.locked,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum JournalRecord {
    Set {
        key: String,
        element: PersistedKvElement,
    },
    Delete {
        key: String,
    },
}

#[derive(Debug)]
pub enum PersistenceError {
    Io(io::Error),
    Serialize(serde_json::Error),
    CorruptJournal {
        line: usize,
        source: serde_json::Error,
    },
}

impl fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PersistenceError::Io(error) => write!(f, "persistence I/O error: {}", error),
            PersistenceError::Serialize(error) => {
                write!(f, "could not serialize persistence record: {}", error)
            }
            PersistenceError::CorruptJournal { line, source } => write!(
                f,
                "corrupt persistence journal at line {}: {}",
                line, source
            ),
        }
    }
}

impl std::error::Error for PersistenceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PersistenceError::Io(error) => Some(error),
            PersistenceError::Serialize(error) => Some(error),
            PersistenceError::CorruptJournal { source, .. } => Some(source),
        }
    }
}

impl From<io::Error> for PersistenceError {
    fn from(error: io::Error) -> Self {
        PersistenceError::Io(error)
    }
}

impl From<serde_json::Error> for PersistenceError {
    fn from(error: serde_json::Error) -> Self {
        PersistenceError::Serialize(error)
    }
}

struct PersistenceBackend {
    path: PathBuf,
    file: Mutex<File>,
    dirty: AtomicBool,
    records_since_snapshot: AtomicUsize,
}

impl PersistenceBackend {
    fn append(&self, record: &JournalRecord) -> Result<(), PersistenceError> {
        let mut encoded = serde_json::to_vec(record)?;
        encoded.push(b'\n');

        let mut file = lock_unpoisoned(&self.file);
        let previous_len = file.metadata()?.len();
        if let Err(error) = file.write_all(&encoded) {
            let _ = file.set_len(previous_len);
            let _ = file.seek(SeekFrom::End(0));
            return Err(error.into());
        }

        self.dirty.store(true, Ordering::Release);
        self.records_since_snapshot.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    fn should_compact(&self) -> bool {
        self.records_since_snapshot.load(Ordering::Acquire) >= COMPACT_AFTER_RECORDS
    }

    fn compact(&self, entries: Vec<(String, PersistedKvElement)>) -> Result<(), PersistenceError> {
        let temporary_path = snapshot_temporary_path(&self.path);
        let result = (|| {
            {
                let mut snapshot = File::create(&temporary_path)?;
                for (key, element) in entries {
                    serde_json::to_writer(&mut snapshot, &JournalRecord::Set { key, element })?;
                    snapshot.write_all(b"\n")?;
                }
                snapshot.sync_all()?;
            }

            replace_file(&temporary_path, &self.path)?;
            sync_parent_directory(&self.path);

            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&self.path)?;
            *lock_unpoisoned(&self.file) = file;
            self.records_since_snapshot.store(0, Ordering::Release);
            self.dirty.store(false, Ordering::Release);
            Ok(())
        })();

        if result.is_err() {
            let _ = fs::remove_file(&temporary_path);
        }
        result
    }

    fn flush(&self) -> Result<(), PersistenceError> {
        if !self.dirty.swap(false, Ordering::AcqRel) {
            return Ok(());
        }

        let file = lock_unpoisoned(&self.file);
        if let Err(error) = file.sync_data() {
            self.dirty.store(true, Ordering::Release);
            return Err(error.into());
        }
        Ok(())
    }
}

struct PersistenceRuntime {
    backend: Arc<PersistenceBackend>,
    stop: Arc<(Mutex<bool>, Condvar)>,
    worker: Option<JoinHandle<()>>,
}

impl PersistenceRuntime {
    fn start(backend: Arc<PersistenceBackend>) -> io::Result<Self> {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let worker_backend = backend.clone();
        let worker_stop = stop.clone();
        let worker = thread::Builder::new()
            .name("lucid-persistence".to_string())
            .spawn(move || persistence_worker(worker_backend, worker_stop))?;

        Ok(PersistenceRuntime {
            backend,
            stop,
            worker: Some(worker),
        })
    }

    fn shutdown(mut self) {
        if let Err(error) = self.backend.flush() {
            error!("Unable to flush the persistence journal: {}", error);
        }

        {
            let (lock, condition) = &*self.stop;
            *lock_unpoisoned(lock) = true;
            condition.notify_all();
        }

        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                error!("The persistence worker stopped unexpectedly");
            }
        }
    }
}

pub struct KvStore {
    container: CHashMap<String, KvElement>,
    cipher: Option<Cipher>,
    persistence: Option<PersistenceRuntime>,
    mutation_lock: Mutex<()>,
}

pub struct Cipher {
    priv_key: [u8; 24],
    iv: [u8; 16],
}

impl KvStore {
    pub fn new(cipher: Option<[&str; 2]>) -> KvStore {
        KvStore {
            container: CHashMap::new(),
            cipher: parse_cipher(cipher),
            persistence: None,
            mutation_lock: Mutex::new(()),
        }
    }

    pub fn with_persistence<P: AsRef<Path>>(
        cipher: Option<[&str; 2]>,
        location: P,
    ) -> Result<KvStore, PersistenceError> {
        let journal_path = resolve_journal_path(location.as_ref());
        if let Some(parent) = journal_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }

        let container = load_journal(&journal_path)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&journal_path)?;
        let backend = Arc::new(PersistenceBackend {
            path: journal_path,
            file: Mutex::new(file),
            dirty: AtomicBool::new(false),
            records_since_snapshot: AtomicUsize::new(0),
        });
        let persistence = PersistenceRuntime::start(backend)?;

        Ok(KvStore {
            container,
            cipher: parse_cipher(cipher),
            persistence: Some(persistence),
            mutation_lock: Mutex::new(()),
        })
    }

    pub fn open<P: AsRef<Path>>(
        cipher: Option<[&str; 2]>,
        location: P,
    ) -> Result<KvStore, PersistenceError> {
        KvStore::with_persistence(cipher, location)
    }

    pub fn new_with_persistence<P: AsRef<Path>>(
        cipher: Option<[&str; 2]>,
        location: P,
    ) -> Result<KvStore, PersistenceError> {
        KvStore::with_persistence(cipher, location)
    }

    pub fn set(&self, key: String, mut value: Vec<u8>, mime: Option<String>) -> Option<KvElement> {
        let _mutation = lock_unpoisoned(&self.mutation_lock);
        if let Some(c) = &self.cipher {
            let cipher = SerpentCbc::new_var(&c.priv_key, &c.iv).unwrap();
            value = cipher.encrypt_vec(&value);
        }
        let mime_type = match mime {
            Some(given_mimetype) => given_mimetype,
            None => tree_magic::from_u8(value.as_ref()).to_string(),
        };
        let key_for_record = key.clone();

        let (result, persisted) = match &mut self.container.get_mut(&key) {
            Some(kv_element) => {
                if !kv_element.locked {
                    kv_element.data = value;
                    kv_element.mime_type = mime_type;
                }
                kv_element.updated_at = Utc::now();
                kv_element.update_count = kv_element.update_count + 1;
                let updated = kv_element.to_owned();
                (Some(updated.clone()), Some(updated))
            }
            None => {
                let kv_element = KvElement {
                    data: value,
                    mime_type,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    expire_at: Utc::now(),
                    update_count: 1,
                    locked: false,
                };
                let persisted = kv_element.clone();
                self.container.insert(key, kv_element);
                (None, Some(persisted))
            }
        };

        if let Some(element) = &persisted {
            self.persist_set(key_for_record, element);
        }
        result
    }

    pub fn get(&self, key: String) -> Option<KvElement> {
        match self.container.get(&key) {
            Some(value) => {
                let mut cloned_value = value.clone();

                if let Some(c) = &self.cipher {
                    let cipher = SerpentCbc::new_var(&c.priv_key, &c.iv).unwrap();
                    cloned_value.data = cipher.decrypt_vec(&value.data).unwrap();
                }
                Some(cloned_value)
            }
            None => None,
        }
    }

    pub fn switch_lock(&self, key: String, to_lock: bool) -> bool {
        let _mutation = lock_unpoisoned(&self.mutation_lock);
        let updated = match &mut self.container.get_mut(&key) {
            Some(kv_element) => {
                if kv_element.locked == to_lock {
                    None
                } else {
                    kv_element.locked = to_lock;
                    Some(kv_element.to_owned())
                }
            }
            None => None,
        };

        if let Some(element) = updated {
            self.persist_set(key, &element);
            true
        } else {
            false
        }
    }

    pub fn increment_or_decrement(&self, key: String, value: f64) -> bool {
        let _mutation = lock_unpoisoned(&self.mutation_lock);
        let updated = match &mut self.container.get_mut(&key) {
            Some(kv_element) => {
                let byte_to_string = String::from_utf8(kv_element.clone().data).unwrap(); // TODO: handle convert to string error
                match byte_to_string.trim().parse::<f64>() {
                    Ok(initial_value) => {
                        kv_element.data = (initial_value + value).to_string().into_bytes();
                        kv_element.updated_at = Utc::now();
                        kv_element.update_count = kv_element.update_count + 1;
                        Some(kv_element.to_owned())
                    }
                    Err(_) => None,
                }
            }
            None => None,
        };

        if let Some(element) = updated {
            self.persist_set(key, &element);
            true
        } else {
            false
        }
    }

    pub fn set_expiration(&self, key: String, ttl: i64) -> Option<DateTime<Utc>> {
        let _mutation = lock_unpoisoned(&self.mutation_lock);
        let updated = match &mut self.container.get_mut(&key) {
            Some(kv_element) => {
                let expiration_date = Utc::now() + Duration::seconds(ttl);
                kv_element.expire_at = expiration_date;
                kv_element.updated_at = Utc::now();
                kv_element.update_count = kv_element.update_count + 1;
                Some((kv_element.to_owned(), expiration_date))
            }
            None => None,
        };

        match updated {
            Some((element, expiration_date)) => {
                self.persist_set(key, &element);
                Some(expiration_date)
            }
            None => None,
        }
    }

    pub fn drop(&self, key: String) {
        let _mutation = lock_unpoisoned(&self.mutation_lock);
        if self.container.remove(&key).is_some() {
            self.persist_record(JournalRecord::Delete { key });
        }
    }

    pub fn len(&self) -> usize {
        self.container.len()
    }

    pub fn count(&self) -> usize {
        self.len()
    }

    pub fn is_empty(&self) -> bool {
        self.container.is_empty()
    }

    pub fn flush(&self) -> Result<(), PersistenceError> {
        match &self.persistence {
            Some(persistence) => persistence.backend.flush(),
            None => Ok(()),
        }
    }

    fn persist_set(&self, key: String, element: &KvElement) {
        self.persist_record(JournalRecord::Set {
            key,
            element: element.into(),
        });
    }

    fn persist_record(&self, record: JournalRecord) {
        let persistence = match &self.persistence {
            Some(persistence) => persistence,
            None => return,
        };

        if let Err(error) = persistence.backend.append(&record) {
            error!("Unable to append to the persistence journal: {}", error);
            return;
        }

        if persistence.backend.should_compact() {
            let entries = self
                .container
                .clone()
                .into_iter()
                .map(|(key, element)| (key, PersistedKvElement::from(&element)))
                .collect();
            if let Err(error) = persistence.backend.compact(entries) {
                warn!("Unable to compact the persistence journal: {}", error);
            }
        }
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        if let Some(persistence) = self.persistence.take() {
            persistence.shutdown();
        }
    }
}

fn parse_cipher(cipher: Option<[&str; 2]>) -> Option<Cipher> {
    cipher.map(|c| {
        let (mut priv_key, mut iv) = ([0u8; 24], [0u8; 16]);
        priv_key[..24].copy_from_slice(&hex::decode(c[0]).unwrap());
        iv[..16].copy_from_slice(&hex::decode(c[1]).unwrap());
        Cipher { priv_key, iv }
    })
}

fn load_journal(path: &Path) -> Result<CHashMap<String, KvElement>, PersistenceError> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)?;

    let valid_len = contents
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map(|position| position + 1)
        .unwrap_or(0);
    if valid_len != contents.len() {
        file.set_len(valid_len as u64)?;
        file.sync_data()?;
        contents.truncate(valid_len);
    }

    let contents = String::from_utf8(contents)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let container = CHashMap::new();
    for (line_number, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let record = serde_json::from_str::<JournalRecord>(line).map_err(|source| {
            PersistenceError::CorruptJournal {
                line: line_number + 1,
                source,
            }
        })?;
        match record {
            JournalRecord::Set { key, element } => {
                container.insert(key, element.into());
            }
            JournalRecord::Delete { key } => {
                container.remove(&key);
            }
        }
    }

    Ok(container)
}

fn resolve_journal_path(location: &Path) -> PathBuf {
    if location.is_dir() {
        return location.join(DEFAULT_JOURNAL_FILE);
    }

    if location.exists() || location.extension().is_some() {
        location.to_path_buf()
    } else {
        location.join(DEFAULT_JOURNAL_FILE)
    }
}

fn snapshot_temporary_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(DEFAULT_JOURNAL_FILE);
    path.with_file_name(format!("{}.tmp.{}", file_name, std::process::id()))
}

fn replace_file(source: &Path, destination: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        if destination.exists() {
            fs::remove_file(destination)?;
        }
    }
    fs::rename(source, destination)
}

fn sync_parent_directory(path: &Path) {
    #[cfg(unix)]
    {
        if let Some(parent) = path.parent() {
            if let Ok(directory) = File::open(parent) {
                if let Err(error) = directory.sync_all() {
                    warn!("Unable to sync the persistence directory: {}", error);
                }
            }
        }
    }
}

fn persistence_worker(backend: Arc<PersistenceBackend>, stop: Arc<(Mutex<bool>, Condvar)>) {
    loop {
        let (lock, condition) = &*stop;
        let guard = lock_unpoisoned(lock);
        if *guard {
            return;
        }
        let (guard, _) = match condition.wait_timeout(guard, SYNC_INTERVAL) {
            Ok(result) => result,
            Err(error) => error.into_inner(),
        };
        if *guard {
            return;
        }
        drop(guard);

        if let Err(error) = backend.flush() {
            warn!("Unable to sync the persistence journal: {}", error);
        }
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(error) => error.into_inner(),
    }
}
