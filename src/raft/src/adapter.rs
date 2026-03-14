//! Adapter to connect RaftApp with Redis storage layer.
//!
//! This module provides the glue between the Raft consensus layer
//! and the Redis storage layer, enabling transparent Raft integration.

use std::{future::Future, pin::Pin, sync::Arc};

use conf::raft_type::Binlog;
use storage::{error::Result, options::AppendLogFunction};

use crate::node::RaftApp;

/// Create an AppendLogFunction that routes to RaftApp::client_write.
///
/// # Arguments
/// * `raft_app` - The RaftApp instance
///
/// # Returns
/// An AppendLogFunction that can be set on Redis to enable Raft mode
pub fn create_append_log_fn(raft_app: Arc<RaftApp>) -> AppendLogFunction {
    Arc::new(move |binlog: Binlog| {
        let raft_app = raft_app.clone();
        Box::pin(async move {
            match raft_app.client_write(binlog).await {
                Ok(response) => {
                    if response.success {
                        Ok(())
                    } else {
                        Err(storage::error::Error::Batch {
                            message: response
                                .error_message
                                .unwrap_or_else(|| "Raft write failed".to_string()),
                            location: Default::default(),
                        })
                    }
                }
                Err(e) => Err(storage::error::Error::Batch {
                    message: format!("Raft error: {}", e),
                    location: Default::default(),
                }),
            }
        }) as Pin<Box<dyn Future<Output = Result<()>> + Send>>
    })
}
