use std::collections::{HashMap};
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::ops::MulAssign;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use crate::parse::ConfigParser;
use std::result::Result;


pub type Status = Result<(), String>;
pub type CheckFunc = Arc<dyn Fn(&str) -> Status + Send + Sync>;

pub trait BaseValue: fmt::Debug {
    fn key(&self) -> &str;
    fn value(&self) -> String;
    fn set(&mut self, value: &str, init_stage: bool) -> Status;
}

pub struct StringValue {
    key: String,
    value: String,
    check_func: Option<CheckFunc>,
    rewritable: bool,
}

impl StringValue {
    pub fn new(
        key: String,
        check_func: Option<CheckFunc>,
        rewritable: bool,
        value: &mut String,
    ) -> Self {
        StringValue {
            key,
            value: value.clone(),
            check_func,
            rewritable,
        }
    }
}

impl Debug for StringValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("StringValue")
            .field("key", &self.key)
            .field("value", &self.value)
            .field("rewritable", &self.rewritable)
            .field("check_func", &format_args!("<function>"))
            .finish()
    }
}

impl BaseValue for StringValue {
    fn key(&self) -> &str {
        &self.key
    }

    fn value(&self) -> String {
        self.value.clone()
    }

    fn set(&mut self, value: &str, init_stage: bool) -> Status {
        if !init_stage && !self.rewritable {
            return Err("Dynamic modification not supported".to_string());
        }
        if let Some(f) = &self.check_func {
            f(value)?;
        }
        self.value = value.to_string();
        Ok(())
    }
}

#[derive(Debug)]
pub struct StringValueArray {
    key: String,
    values: Vec<String>,
    delimiter: char,
    rewritable: bool,
}

impl StringValueArray {
    pub fn new(key: String, rewritable: bool, values: &mut Vec<String>, delimiter: char) -> Self {
        StringValueArray {
            key,
            values: values.clone(),
            delimiter,
            rewritable,
        }
    }
}

impl BaseValue for StringValueArray {
    fn key(&self) -> &str {
        &self.key
    }

    fn value(&self) -> String {
        self.values.join(&self.delimiter.to_string())
    }

    fn set(&mut self, value: &str, init_stage: bool) -> Status {
        if !init_stage && !self.rewritable {
            return Err("Dynamic modification not supported".to_string());
        }

        let parts: Vec<&str> = value.split(self.delimiter).collect();

        if !self.values.is_empty() && parts.len() != self.values.len() {
            return Err("Number of parameters does not match".to_string());
        }

        self.values.clear();
        for part in parts {
            self.values.push(part.to_string());
        }

        Ok(())
    }
}

pub struct BoolValue {
    key: String,
    value: bool,
    check_func: Option<CheckFunc>,
    rewritable: bool,
}

impl BoolValue {
    pub fn new(
        key: String,
        check_func: Option<CheckFunc>,
        rewritable: bool,
        value: &mut bool,
    ) -> Self {
        BoolValue {
            key,
            value: *value,
            check_func,
            rewritable,
        }
    }
}

impl Debug for BoolValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("BoolValue")
            .field("key", &self.key)
            .field("value", &self.value)
            .field("rewritable", &self.rewritable)
            .field("check_func", &format_args!("<function>"))
            .finish()
    }
}

impl BaseValue for BoolValue {
    fn key(&self) -> &str {
        &self.key
    }

    fn value(&self) -> String {
        if self.value { "yes" } else { "no" }.to_string()
    }

    fn set(&mut self, value: &str, init_stage: bool) -> Status {
        if !init_stage && !self.rewritable {
            return Err("Dynamic modification not supported".to_string());
        }
        match value.to_lowercase().as_str() {
            "yes" | "true" => self.value = true,
            "no" | "false" => self.value = false,
            _ => return Err("Invalid boolean value".to_string()),
        }
        Ok(())
    }
}

pub struct NumberValue<T> {
    key: String,
    value: T,
    rewritable: bool,
    min: T,
    max: T,
}

impl<T: Copy + PartialOrd + FromStr + MulAssign> NumberValue<T> {
    pub fn new(key: String, rewritable: bool, value: &mut T, min: T, max: T) -> Self {
        NumberValue {
            key,
            value: *value,
            rewritable,
            min,
            max,
        }
    }
}

impl<T: Copy + PartialOrd + FromStr + MulAssign + Debug> Debug for NumberValue<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("NumberValue")
            .field("key", &self.key)
            .field("value", &self.value)
            .field("rewritable", &self.rewritable)
            .field("min", &self.min)
            .field("max", &self.max)
            .finish()
    }
}

impl<T: Copy + PartialOrd + FromStr + MulAssign + Display + Debug> BaseValue for NumberValue<T>
where
    T::Err: Display,
{
    fn key(&self) -> &str {
        &self.key
    }

    fn value(&self) -> String {
        format!("{}", self.value)
    }

    fn set(&mut self, value: &str, init_stage: bool) -> Status {
        if !init_stage && !self.rewritable {
            return Err("Dynamic modification not supported".to_string());
        }

        let v: T = value.parse::<T>().map_err(|e| e.to_string())?;

        if v < self.min {
            self.value = self.min;
        } else if v > self.max {
            self.value = self.max;
        } else {
            self.value = v;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct MemorySize {
    key: String,
    value: usize,
    rewritable: bool,
}

impl MemorySize {
    pub fn new(key: String, rewritable: bool, value: &mut usize) -> Self {
        MemorySize {
            key,
            value: *value,
            rewritable,
        }
    }
}

impl BaseValue for MemorySize {
    fn key(&self) -> &str {
        &self.key
    }

    fn value(&self) -> String {
        format!("{}", self.value)
    }

    fn set(&mut self, value: &str, init_stage: bool) -> Status {
        if !init_stage && !self.rewritable {
            return Err("Dynamic modification not supported".to_string());
        }

        let mut num = value[..value.len() - 1]
            .parse::<usize>()
            .map_err(|_| "Invalid memory size".to_string())?;

        match value.chars().last().unwrap_or(' ') {
            'k' | 'K' => num *= 1 << 10,
            'm' | 'M' => num *= 1 << 20,
            'g' | 'G' => num *= 1 << 30,
            _ => {}
        }

        self.value = num;
        Ok(())
    }
}

pub struct Config {
    parser: ConfigParser,
    config_map: HashMap<String, Box<dyn BaseValue>>,
    // 所有 public 字段都在这里定义
    pub timeout: u32,
    pub memory_size: usize,
    pub tcp_keepalive: u32,
    pub password: String,
    pub master_auth: String,
    pub master_ip: String,
    pub master_port: u32,
    pub aliases: HashMap<String, String>,
    pub max_clients: u32,
    pub slow_log_time: u32,
    pub slow_log_max_len: u32,
    pub include_file: String,
    pub modules: Vec<String>,
    pub daemonize: bool,
    pub pid_file: String,
    pub ips: Vec<String>,
    pub raft_ip: String,
    pub port: u16,
    pub raft_port_offset: u32,
    pub db_path: String,
    pub log_dir: String,
    pub log_level: String,
    pub run_id: String,
    pub databases: usize,
    pub redis_compatible_mode: bool,
    pub use_raft: bool,
    pub worker_threads_num: u32,
    pub slave_threads_num: u32,
    pub fast_cmd_threads_num: u32,
    pub slow_cmd_threads_num: u32,
    pub max_client_response_size: u64,
    pub small_compaction_threshold: u64,
    pub small_compaction_duration_threshold: u64,
    pub rocksdb_max_subcompactions: u32,
    pub rocksdb_max_background_jobs: u32,
    pub rocksdb_max_write_buffer_number: u32,
    pub rocksdb_min_write_buffer_number_to_merge: u32,
    pub rocksdb_write_buffer_size: usize,
    pub rocksdb_level0_file_num_compaction_trigger: u32,
    pub rocksdb_num_levels: u32,
    pub rocksdb_enable_pipelined_write: bool,
    pub rocksdb_level0_slowdown_writes_trigger: u32,
    pub rocksdb_level0_stop_writes_trigger: u32,
    pub rocksdb_ttl_second: u64,
    pub rocksdb_periodic_second: u64,
}

impl Config {
    pub fn new() -> Self {
        let mut config = Config {
            parser: ConfigParser::new(),
            config_map: Default::default(),
            timeout: 0,
            tcp_keepalive: 300,
            password: String::new(),
            master_auth: String::new(),
            master_ip: String::new(),
            master_port: 0,
            aliases: Default::default(),
            max_clients: 10000,
            slow_log_time: 1000,
            slow_log_max_len: 128,
            include_file: String::new(),
            modules: vec![],
            daemonize: false,
            pid_file: "./kiwi.pid".to_string(),
            ips: vec!["127.0.0.1".to_string(), "::1".to_string()],
            raft_ip: "127.0.0.1".to_string(),
            port: 9221,
            raft_port_offset: 10,
            db_path: "./db/".to_string(),
            log_dir: "stdout".to_string(),
            log_level: "warning".to_string(),
            run_id: String::new(),
            databases: 16,
            redis_compatible_mode: true,
            use_raft: false,
            worker_threads_num: 2,
            slave_threads_num: 2,
            fast_cmd_threads_num: 4,
            slow_cmd_threads_num: 4,
            max_client_response_size: 1073741824,
            small_compaction_threshold: 604800,
            small_compaction_duration_threshold: 259200,
            rocksdb_max_subcompactions: 0,
            rocksdb_max_background_jobs: 4,
            rocksdb_max_write_buffer_number: 2,
            rocksdb_min_write_buffer_number_to_merge: 2,
            rocksdb_write_buffer_size: 64 << 20,
            rocksdb_level0_file_num_compaction_trigger: 4,
            rocksdb_num_levels: 7,
            rocksdb_enable_pipelined_write: false,
            rocksdb_level0_slowdown_writes_trigger: 20,
            rocksdb_level0_stop_writes_trigger: 36,
            rocksdb_ttl_second: 604800,
            rocksdb_periodic_second: 259200,
            memory_size: 0,
        };

        // 注册配置项
        {
            let flag = &mut config.redis_compatible_mode.clone();
            config.add_bool(
                "redis-compatible-mode",
                Some(Arc::new(is_valid_bool)),
                true,
                flag,
            );
        }

        {
            let flag = &mut config.daemonize.clone();
            config.add_bool("daemonize", Some(Arc::new(is_valid_bool)), false, flag);
        }

        {
            let ips = &mut config.ips.clone();
            config.add_string_array("ips", false, ips, ' ');
        }

        {
            let ip = &mut config.raft_ip.clone();
            config.add_string("raft-ip", false, ip);
        }

        {
            let port = &mut config.port.clone();
            config.add_number_with_limit("port", false, port, 1_u16, 65535_u16);
        }

        {
            let memory_size = &mut config.memory_size.clone();
            config.add_memory_size("memory-size", true, memory_size);
        }

        {
            let offset = &mut config.raft_port_offset.clone();
            config.add_number("raft-port-offset", true, offset);
        }

        {
            let val = &mut config.timeout.clone();
            config.add_number("timeout", true, val);
        }

        {
            let val = &mut config.tcp_keepalive.clone();
            config.add_number("tcp-keepalive", true, val);
        }

        {
            let val = &mut config.db_path.clone();
            config.add_string("db-path", false, val);
        }

        {
            let val = &mut config.log_level.clone();
            config.add_string_with_func(
                "loglevel",
                Arc::new(|v| {
                    if ["info", "verbose", "notice", "warning"].contains(&v.to_lowercase().as_str())
                    {
                        Ok(())
                    } else {
                        Err("Log level must be debug / verbose / notice / warning".to_string())
                    }
                }),
                false,
                val,
            );
        }

        {
            let val = &mut config.log_dir.clone();
            config.add_string("logfile", false, val);
        }

        {
            let val = &mut config.databases.clone();
            config.add_number_with_limit("databases", false, val, 1_usize, 16_usize);
        }
        {
            let val = config.memory_size;
            config.add_memory_size("memory", true, &mut val.clone())
        }

        {
            let val = &mut config.password.clone();
            config.add_string("requirepass", true, val);
        }

        {
            let val = &mut config.max_clients.clone();
            config.add_number("maxclients", true, val);
        }

        {
            let val = &mut config.worker_threads_num.clone();
            config.add_number_with_limit("worker-threads", false, val, 1_u32, 129_u32);
        }

        {
            let val = &mut config.slave_threads_num.clone();
            config.add_number_with_limit("slave-threads", false, val, 1_u32, 129_u32);
        }

        {
            let val = &mut config.slow_log_time.clone();
            config.add_number("slowlog-log-slower-than", true, val);
        }

        {
            let val = &mut config.slow_log_max_len.clone();
            config.add_number("slowlog-max-len", true, val);
        }

        {
            let val = &mut config.fast_cmd_threads_num.clone();
            config.add_number_with_limit("fast-cmd-threads-num", false, val, 1_u32, 129_u32);
        }

        {
            let val = &mut config.slow_cmd_threads_num.clone();
            config.add_number_with_limit("slow-cmd-threads-num", false, val, 1_u32, 129_u32);
        }

        {
            let val = &mut config.max_client_response_size.clone();
            config.add_number("max-client-response-size", true, val);
        }

        {
            let val = &mut config.run_id.clone();
            config.add_string("runid", false, val);
        }

        {
            let val = &mut config.small_compaction_threshold.clone();
            config.add_number("small-compaction-threshold", true, val);
        }

        {
            let val = &mut config.small_compaction_duration_threshold.clone();
            config.add_number("small-compaction-duration-threshold", true, val);
        }

        {
            let val = &mut config.use_raft.clone();
            config.add_bool("use-raft", Some(Arc::new(is_valid_bool)), false, val);
        }

        // RocksDB 配置
        {
            let val = &mut config.rocksdb_max_subcompactions.clone();
            config.add_number("rocksdb-max-subcompactions", false, val);
        }

        {
            let val = &mut config.rocksdb_max_background_jobs.clone();
            config.add_number("rocksdb-max-background-jobs", false, val);
        }

        {
            let val = &mut config.rocksdb_max_write_buffer_number.clone();
            config.add_number("rocksdb-max-write-buffer-number", false, val);
        }

        {
            let val = &mut config.rocksdb_min_write_buffer_number_to_merge.clone();
            config.add_number("rocksdb-min-write-buffer-number-to-merge", false, val);
        }

        {
            let val = &mut config.rocksdb_write_buffer_size.clone();
            config.add_memory_size("rocksdb-write-buffer-size", false, val);
        }

        {
            let val = &mut config.rocksdb_level0_file_num_compaction_trigger.clone();
            config.add_number("rocksdb-level0-file-num-compaction-trigger", false, val);
        }

        {
            let val = &mut config.rocksdb_num_levels.clone();
            config.add_number("rocksdb-number-levels", true, val);
        }

        {
            let val = &mut config.rocksdb_enable_pipelined_write.clone();
            config.add_bool(
                "rocksdb-enable-pipelined-write",
                Some(Arc::new(is_valid_bool)),
                false,
                val,
            );
        }

        {
            let val = &mut config.rocksdb_level0_slowdown_writes_trigger.clone();
            config.add_number("rocksdb-level0-slowdown-writes-trigger", false, val);
        }

        {
            let val = &mut config.rocksdb_level0_stop_writes_trigger.clone();
            config.add_number("rocksdb-level0-stop-writes-trigger", false, val);
        }

        config
    }

    pub fn load_from_file<P: AsRef<Path>>(&mut self, path: P) -> Result<(), String> {
        self.parser.load(path)?;
        for (key, values) in self.parser.get_map() {
            if let Some(v) = self.config_map.get_mut(key) {
                v.set(&values[0], true)?;
            }
        }

        // Handle special cases
        if let Some(master) = self.parser.get_map().get("slaveof") {
            if master.len() == 2 {
                self.master_ip = master[0].clone();
                self.master_port = master[1].parse().unwrap_or(0);
            }
        }

        if let Some(alias) = self.parser.get_map().get("rename-command") {
            for chunk in alias.chunks(2) {
                if chunk.len() == 2 {
                    self.aliases.insert(chunk[0].clone(), chunk[1].clone());
                }
            }
        }

        Ok(())
    }

    pub fn set(&mut self, key: &str, value: &str, init_stage: bool) -> Status {
        if let Some(v) = self.config_map.get_mut(key) {
            v.set(value, init_stage)
        } else {
            Err("Non-existent configuration item".to_string())
        }
    }

    fn add_bool(
        &mut self,
        key: &str,
        check_func: Option<CheckFunc>,
        rewritable: bool,
        value: &mut bool,
    ) {
        let key = key.to_string();
        self.config_map.insert(
            key.clone(),
            Box::new(BoolValue::new(key, check_func, rewritable, value)),
        );
    }

    fn add_string(&mut self, key: &str, rewritable: bool, value: &mut String) {
        let key = key.to_string();
        self.config_map.insert(
            key.clone(),
            Box::new(StringValue::new(key, None, rewritable, value)),
        );
    }

    fn add_string_with_func(
        &mut self,
        key: &str,
        check_func: CheckFunc,
        rewritable: bool,
        value: &mut String,
    ) {
        let key = key.to_string();
        self.config_map.insert(
            key.clone(),
            Box::new(StringValue::new(key, Some(check_func), rewritable, value)),
        );
    }

    fn add_string_array(
        &mut self,
        key: &str,
        rewritable: bool,
        values: &mut Vec<String>,
        delimiter: char,
    ) {
        let key = key.to_string();
        self.config_map.insert(
            key.clone(),
            Box::new(StringValueArray::new(key, rewritable, values, delimiter)),
        );
    }

    fn add_number<T: Copy + PartialOrd + FromStr + Display + Debug + MulAssign + 'static>(
        &mut self,
        key: &str,
        rewritable: bool,
        value: &mut T,
    ) where
        T::Err: Display + Debug,
    {
        let key = key.to_string();
        let min = T::from_str("0").expect("0 should be valid");
        let max = T::from_str("4294967295").unwrap_or(min);

        self.config_map.insert(
            key.clone(),
            Box::new(NumberValue::new(key, rewritable, value, min, max)),
        );
    }

    fn add_number_with_limit<
        T: Copy + PartialOrd + FromStr + Display + MulAssign + Debug + 'static,
    >(
        &mut self,
        key: &str,
        rewritable: bool,
        value: &mut T,
        min: T,
        max: T,
    ) where
        T::Err: Display,
    {
        let key = key.to_string();
        self.config_map.insert(
            key.clone(),
            Box::new(NumberValue::new(key, rewritable, value, min, max)),
        );
    }

    fn add_memory_size(&mut self, key: &str, rewritable: bool, value: &mut usize) {
        let key = key.to_string();
        self.config_map.insert(
            key.clone(),
            Box::new(MemorySize::new(key, rewritable, value)),
        );
    }
}

pub fn is_valid_bool(value: &str) -> Result<(), String> {
    match value.to_lowercase().as_str() {
        "yes" | "no" | "true" | "false" => Ok(()),
        _ => Err("Invalid boolean value. Must be yes/no or true/false".to_string()),
    }
}
impl Config {
    pub fn get<T: FromStr>(&self, key: &str) -> Option<T>
    where
        T::Err: std::fmt::Debug,
    {
        if let Some(value) = self.config_map.get(key).map(|v| v.value()) {
            value.parse::<T>().ok()
        } else {
            None
        }
    }
}
