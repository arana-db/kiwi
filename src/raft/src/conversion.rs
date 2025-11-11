// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Type conversion layer between Openraft types and Kiwi internal types
//!
//! This module provides conversion functions to bridge Openraft's sealed traits
//! with Kiwi's internal data structures. It handles:
//! - Entry → ClientRequest conversion
//! - ClientResponse → Response conversion  
//! - RaftError → StorageError conversion

use openraft::{Entry, EntryPayload, StorageError as OpenraftStorageError};

use crate::error::{RaftError, StorageError};
use crate::types::{ClientRequest, ClientResponse, NodeId, TypeConfig};

// ============================================================================
// Entry → ClientRequest Conversion
// ============================================================================

/// Convert Openraft Entry to Kiwi ClientRequest
///
/// This function extracts the ClientRequest from an Openraft Entry's payload.
/// It handles Normal payloads containing serialized ClientRequest data.
///
/// # Arguments
/// * `entry` - The Openraft Entry to convert
///
/// # Returns
/// * `Ok(ClientRequest)` - Successfully extracted and deserialized request
/// * `Err(RaftError)` - If the entry type is unsupported or deserialization fails
///
/// # Errors
/// - Returns `RaftError::InvalidRequest` if the entry payload is not Normal
/// - Returns `RaftError::Serialization` if deserialization fails
pub fn entry_to_client_request(entry: &Entry<TypeConfig>) -> Result<ClientRequest, RaftError> {
    match &entry.payload {
        EntryPayload::Normal(client_request) => {
            // The payload is already a ClientRequest due to TypeConfig::D = ClientRequest
            Ok(client_request.clone())
        }
        EntryPayload::Blank => Err(RaftError::invalid_request(
            "Cannot convert blank entry to ClientRequest",
        )),
        EntryPayload::Membership(_) => Err(RaftError::invalid_request(
            "Cannot convert membership entry to ClientRequest",
        )),
    }
}

/// Convert Openraft Entry payload bytes to ClientRequest
///
/// This is a lower-level function that deserializes raw bytes into a ClientRequest.
/// Used when working with serialized entry data.
///
/// # Arguments
/// * `payload_bytes` - The serialized ClientRequest data
///
/// # Returns
/// * `Ok(ClientRequest)` - Successfully deserialized request
/// * `Err(RaftError)` - If deserialization fails
pub fn deserialize_client_request(payload_bytes: &[u8]) -> Result<ClientRequest, RaftError> {
    bincode::deserialize(payload_bytes).map_err(|e| {
        RaftError::Storage(StorageError::DataInconsistency {
            message: format!("Failed to deserialize ClientRequest: {}", e),
            context: "deserialize_client_request".to_string(),
        })
    })
}

// ============================================================================
// ClientResponse → Response Conversion
// ============================================================================

/// Convert Kiwi ClientResponse to serialized bytes
///
/// This function serializes a ClientResponse into bytes that can be returned
/// to Openraft as a response payload.
///
/// # Arguments
/// * `response` - The ClientResponse to serialize
///
/// # Returns
/// * `Ok(Vec<u8>)` - Successfully serialized response
/// * `Err(RaftError)` - If serialization fails
pub fn serialize_client_response(response: &ClientResponse) -> Result<Vec<u8>, RaftError> {
    bincode::serialize(response).map_err(|e| {
        RaftError::Storage(StorageError::DataInconsistency {
            message: format!("Failed to serialize ClientResponse: {}", e),
            context: "serialize_client_response".to_string(),
        })
    })
}

/// Deserialize bytes to ClientResponse
///
/// This function deserializes response bytes back into a ClientResponse.
/// Used when reading responses from storage or network.
///
/// # Arguments
/// * `response_bytes` - The serialized ClientResponse data
///
/// # Returns
/// * `Ok(ClientResponse)` - Successfully deserialized response
/// * `Err(RaftError)` - If deserialization fails
pub fn deserialize_client_response(response_bytes: &[u8]) -> Result<ClientResponse, RaftError> {
    bincode::deserialize(response_bytes).map_err(|e| {
        RaftError::Storage(StorageError::DataInconsistency {
            message: format!("Failed to deserialize ClientResponse: {}", e),
            context: "deserialize_client_response".to_string(),
        })
    })
}

// ============================================================================
// RaftError → StorageError Conversion
// ============================================================================

/// Convert Kiwi RaftError to Openraft StorageError
///
/// This function maps Kiwi's internal error types to Openraft's StorageError,
/// preserving error context and information. It properly sets the ErrorSubject
/// and ErrorVerb based on the error type.
///
/// # Arguments
/// * `err` - The RaftError to convert
///
/// # Returns
/// * `OpenraftStorageError<NodeId>` - The converted storage error
///
/// # Error Context Preservation
/// All error context is preserved in the AnyError wrapper, ensuring detailed
/// debugging information is available throughout the error propagation chain.
pub fn to_storage_error(err: RaftError) -> OpenraftStorageError<NodeId> {
    use openraft::{ErrorSubject, ErrorVerb, StorageIOError};

    // Log the error conversion for debugging
    log::debug!(
        "Converting RaftError to StorageError: category={}, error={}",
        err.category(),
        err
    );
    log::trace!("to_storage_error: error_type={:?}, category={}", 
        std::any::type_name_of_val(&err), err.category());

    match &err {
        // Storage errors - map to IO errors with appropriate subject/verb
        RaftError::Storage(storage_err) => match storage_err {
            StorageError::RocksDb { context, .. } => {
                log::error!("RocksDB error during storage operation: {}, context: {}", err, context);
                OpenraftStorageError::IO {
                    source: StorageIOError::new(
                        ErrorSubject::Store,
                        ErrorVerb::Read,
                        openraft::AnyError::new(&err),
                    ),
                }
            }
            StorageError::LogCorruption { index, term, context } => {
                log::error!(
                    "Log corruption detected at index {} term {}: context: {}",
                    index,
                    term,
                    context
                );
                // Create a LogId for the corrupted log entry
                let log_id = openraft::LogId::new(
                    openraft::CommittedLeaderId::new(*term, 0),
                    *index,
                );
                OpenraftStorageError::IO {
                    source: StorageIOError::new(
                        ErrorSubject::Log(log_id),
                        ErrorVerb::Read,
                        openraft::AnyError::new(&err),
                    ),
                }
            }
            StorageError::SnapshotCreationFailed { snapshot_id, context, .. } => {
                log::error!(
                    "Snapshot creation failed for {}: context: {}",
                    snapshot_id,
                    context
                );
                OpenraftStorageError::IO {
                    source: StorageIOError::new(
                        ErrorSubject::Snapshot(None),
                        ErrorVerb::Write,
                        openraft::AnyError::new(&err),
                    ),
                }
            }
            StorageError::SnapshotRestorationFailed { snapshot_id, context, .. } => {
                log::error!(
                    "Snapshot restoration failed for {}: context: {}",
                    snapshot_id,
                    context
                );
                OpenraftStorageError::IO {
                    source: StorageIOError::new(
                        ErrorSubject::Snapshot(None),
                        ErrorVerb::Read,
                        openraft::AnyError::new(&err),
                    ),
                }
            }
            StorageError::InsufficientDiskSpace { required_bytes, available_bytes, path } => {
                log::error!(
                    "Insufficient disk space at {}: required {} bytes, available {} bytes",
                    path,
                    required_bytes,
                    available_bytes
                );
                OpenraftStorageError::IO {
                    source: StorageIOError::new(
                        ErrorSubject::Store,
                        ErrorVerb::Write,
                        openraft::AnyError::new(&err),
                    ),
                }
            }
            StorageError::DataInconsistency { message, context } => {
                log::error!("Data inconsistency: {}, context: {}", message, context);
                OpenraftStorageError::IO {
                    source: StorageIOError::new(
                        ErrorSubject::Store,
                        ErrorVerb::Read,
                        openraft::AnyError::new(&err),
                    ),
                }
            }
        },

        // State machine errors
        RaftError::StateMachine { message, context } => {
            log::error!("State machine error: {}, context: {}", message, context);
            OpenraftStorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Write,
                    openraft::AnyError::new(&err),
                ),
            }
        }

        // Serialization errors
        RaftError::Serialization(e) => {
            log::error!("Serialization error: {}", e);
            OpenraftStorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Store,
                    ErrorVerb::Read,
                    openraft::AnyError::new(&err),
                ),
            }
        }

        // IO errors
        RaftError::Io(e) => {
            log::error!("IO error: {}", e);
            OpenraftStorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Store,
                    ErrorVerb::Read,
                    openraft::AnyError::new(&err),
                ),
            }
        }

        // Invalid request/state errors
        RaftError::InvalidRequest { message, context } => {
            log::warn!("Invalid request: {}, context: {}", message, context);
            OpenraftStorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Store,
                    ErrorVerb::Read,
                    openraft::AnyError::new(&err),
                ),
            }
        }

        RaftError::InvalidState { message, current_state, context } => {
            log::warn!(
                "Invalid state: {}, current_state: {}, context: {}",
                message,
                current_state,
                context
            );
            OpenraftStorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Store,
                    ErrorVerb::Read,
                    openraft::AnyError::new(&err),
                ),
            }
        }

        // All other errors default to generic IO error with logging
        _ => {
            log::error!("Unexpected error type in storage conversion: {}", err);
            OpenraftStorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Store,
                    ErrorVerb::Read,
                    openraft::AnyError::new(&err),
                ),
            }
        }
    }
}

/// Convert Openraft StorageError to Kiwi RaftError
///
/// This function converts Openraft's StorageError back to Kiwi's RaftError.
/// Used when handling errors from Openraft operations.
///
/// # Arguments
/// * `err` - The Openraft StorageError to convert
///
/// # Returns
/// * `RaftError` - The converted Raft error
pub fn from_storage_error(err: OpenraftStorageError<NodeId>) -> RaftError {
    match err {
        OpenraftStorageError::IO { source } => {
            RaftError::Storage(StorageError::DataInconsistency {
                message: format!("Storage IO error: {}", source),
                context: "openraft_storage_error_conversion".to_string(),
            })
        }
        _ => RaftError::Storage(StorageError::DataInconsistency {
            message: format!("Storage error: {}", err),
            context: "openraft_storage_error_conversion".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests;
