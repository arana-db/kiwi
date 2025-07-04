pub mod conf;
pub mod error;
pub mod parse;

use crate::conf::Config;
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_parser_load() {
        let mut config = Config::new();
        config.load_from_file("test.conf").unwrap();
        assert_eq!(config.load_from_file("test.conf").is_ok(), true);
        assert_eq!(config.get::<u16>("port").unwrap(), 9222);
        assert_eq!(config.get::<usize>("memory"), Some(134217728));
    }
}
