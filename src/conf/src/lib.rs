pub mod conf;
pub mod error;
pub mod parse;

use crate::conf::{Config, Status};
use crate::parse::ConfigParser;
#[cfg(test)]
mod tests {
    use crate::conf::{BaseValue, BoolValue};
    use crate::parse::ConfigParser;
    use super::*;

    #[test]
    fn test_config_parser_load() {
        let mut config = Config::new();
        assert_eq!(config.load_from_file("test.conf").is_ok(), true);
        // println!("port {:?}",parser.get_data::<u16>("port"));
        // assert_eq!(parser.get_data::<u16>("port"), Some(9221));
        // assert_eq!(parser.get_data::<bool>("daemonize"), Some( true));
        // assert_eq!(parser.get_data::<String>("loglevel"), Some("info".to_string()));
        // assert_eq!(parser.get_data::<usize>("memory-limit"), Some(64 << 20));
    }

// #[test]
    // fn test_config_get_and_set() {
    //     let mut config = Config::new();
    //     // assert_eq!(config.get_value("port"), "9221");
    //     // assert_eq!(config.get_value("daemonize"), "no");
    //     //
    //     // assert_eq!(config.set("port".to_string(), "8080", true),Status::OK);
    //     // assert_eq!(config.get_value("port"), "8080");
    //     //
    //     // // 测试无效端口
    //     // assert_eq!(
    //     //     config.set("port".to_string(), "70000", true),
    //     //     Status::InvalidArgument("Failed to parse number.".to_string())
    //     // );
    // }

    // #[test]
    // fn test_bool_value() {
        // let mut config = Config::new();
        // assert_eq!(config.get_value("daemonize"), "no");
        //
        // assert_eq!(config.set("daemonize".to_string(), "\"yes\"", true), Status::OK);
        // assert_eq!(config.get_value("daemonize"), "yes");
        //
        // assert_eq!(
        //     config.set("daemonize".to_string(), "invalid", true),
        //     Status::InvalidArgument("Expected yes/no, true/false or 0/1".to_string())
        // );
    // }

//     #[test]
//     fn test_number_value() {
//         let mut config = Config::new();
//         assert_eq!(config.get_value("timeout"), "0");
//
//         assert_eq!(config.set("timeout".to_string(), "30", true), Status::OK);
//         assert_eq!(config.get_value("timeout"), "30");
//
//         assert_eq!(
//             config.set("timeout".to_string(), "abc", true),
//             Status::InvalidArgument("Failed to parse number.".to_string())
//         );
//     }
//
//     #[test]
//     fn test_memory_size_value() {
//         let mut config = Config::new();
//         assert_eq!(config.get_value("memory-limit"), "64MB");
//
//         assert_eq!(config.set("memory-limit".to_string(), "128MB", true), Status::OK);
//         assert_eq!(config.get_value("memory-limit"), "128MB");
//
//         assert_eq!(config.get_value("memory-limit"), "134217728");
//
//         assert_eq!(
//             config.set("memory-limit".to_string(), "invalid", true),
//             Status::InvalidArgument("Failed to parse memory size.".to_string())
//         );
//     }
//
//     #[test]
//     fn test_load_from_file() {
//         let mut config = Config::new();
//         assert!(config.load_from_file("test.conf"));
//
//         assert_eq!(config.get_value("port"), "9221");
//         assert_eq!(config.get_value("daemonize"), "yes");
//         assert_eq!(config.get_value("loglevel"), "info");
//         assert_eq!(config.get_value("db-path"), "/var/db/");
//         assert_eq!(config.get_value("memory-limit"), "128MB");
//     }
}
