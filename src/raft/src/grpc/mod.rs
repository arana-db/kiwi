// gRPC 服务模块 - 模块化实现
//
// 该模块包含所有 gRPC 服务的实现，按服务类型分为三个子模块：
// - core: RaftCoreService (Vote, AppendEntries, StreamAppend, InstallSnapshot)
// - admin: RaftAdminService (Initialize, AddLearner, ChangeMembership, RemoveNode)
// - client: RaftClientService + RaftMetricsService (Write, Read, Metrics, Leader, Members)

pub mod core;
pub mod admin;
pub mod client;

// 导出服务创建器，便于 main.rs 使用
pub use core::create_core_service;
pub use admin::create_admin_service;
pub use client::{create_client_service, create_metrics_service};
