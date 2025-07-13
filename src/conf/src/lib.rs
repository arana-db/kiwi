pub mod config;
pub mod de_func;
pub mod error;

#[cfg(test)]
mod tests {
    use super::*;
    use config::Config;
    use validator::Validate;

    #[test]
    fn test_config_parsing() {
        let config = config::Config::load("./config.ini");
        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(50, config.timeout);
    }

    #[test]
    fn test_validate_port_range() {
        let mut invalid_config = Config {
            port: 999,
            timeout: 100,
            redis_compatible_mode: false,
            log_dir: "".to_string(),
            memory: 1024,
        };
        assert_eq!(false, invalid_config.validate().is_ok());

        invalid_config.port = 8080;
        assert_eq!(true, invalid_config.validate().is_ok());
    }
}
