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

//! Full bidirectional consistency validation for persisted VectorSet state.

use std::collections::{BTreeMap, BTreeSet};

use rocksdb::{DB, IteratorMode, ReadOptions};
use snafu::{ResultExt, ensure};

use crate::error::{InvalidFormatSnafu, Result, RocksSnafu};
use crate::format_base_key::ParsedBaseKey;
use crate::format_vector::{VectorDataValue, VectorMeta};
use crate::format_vector_member_key::ParsedVectorMemberDataKey;
use crate::{ColumnFamilyIndex, DataType, QuantizationType};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct VectorConsistencyReport {
    pub instances: usize,
    pub metas: usize,
    pub members: usize,
}

impl VectorConsistencyReport {
    pub(crate) fn merge(&mut self, other: Self) -> Result<()> {
        self.instances = self.instances.checked_add(other.instances).ok_or_else(|| {
            InvalidFormatSnafu {
                message: "vector consistency instance count overflow".to_string(),
            }
            .build()
        })?;
        self.metas = self.metas.checked_add(other.metas).ok_or_else(|| {
            InvalidFormatSnafu {
                message: "vector consistency meta count overflow".to_string(),
            }
            .build()
        })?;
        self.members = self.members.checked_add(other.members).ok_or_else(|| {
            InvalidFormatSnafu {
                message: "vector consistency member count overflow".to_string(),
            }
            .build()
        })?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemberGroup {
    count: u64,
    dimension: u32,
    quantization: QuantizationType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BaseMetaState {
    Vector { generation: u64 },
    NonVector,
}

pub(crate) fn validate_vector_consistency_db(
    db: &DB,
    storage_incarnation: u64,
    context: &str,
) -> Result<VectorConsistencyReport> {
    let vector_cf = db
        .cf_handle(ColumnFamilyIndex::VectorDataCF.name())
        .ok_or_else(|| {
            InvalidFormatSnafu {
                message: format!("VectorDataCF is missing during full validation of {context}"),
            }
            .build()
        })?;
    let meta_cf = db
        .cf_handle(ColumnFamilyIndex::MetaCF.name())
        .ok_or_else(|| {
            InvalidFormatSnafu {
                message: format!("MetaCF is missing during full validation of {context}"),
            }
            .build()
        })?;

    let mut report = VectorConsistencyReport {
        instances: 1,
        ..VectorConsistencyReport::default()
    };
    let mut member_groups = BTreeMap::<(Vec<u8>, u64), MemberGroup>::new();
    let mut member_user_keys = BTreeSet::<Vec<u8>>::new();
    let mut member_read_options = ReadOptions::default();
    member_read_options.set_verify_checksums(true);
    for entry in db.iterator_cf_opt(&vector_cf, member_read_options, IteratorMode::Start) {
        let (encoded_key, encoded_value) = entry.context(RocksSnafu)?;
        let member = ParsedVectorMemberDataKey::decode(&encoded_key)?;
        ensure!(
            member.storage_incarnation() == storage_incarnation,
            InvalidFormatSnafu {
                message: format!(
                    "vector member storage incarnation {} does not match manifest {} in {context}",
                    member.storage_incarnation(),
                    storage_incarnation
                )
            }
        );
        ensure!(
            member.generation_sequence() != 0,
            InvalidFormatSnafu {
                message: format!("vector member has zero generation in {context}")
            }
        );
        let value = VectorDataValue::decode(&encoded_value)?;
        let group_key = (member.key().to_vec(), member.generation_sequence());
        member_user_keys.insert(member.key().to_vec());
        let group = member_groups.entry(group_key).or_insert(MemberGroup {
            count: 0,
            dimension: value.dimension(),
            quantization: value.canonical().quantization(),
        });
        ensure!(
            group.dimension == value.dimension(),
            InvalidFormatSnafu {
                message: format!(
                    "vector member dimension mismatch within one key/generation in {context}"
                )
            }
        );
        ensure!(
            group.quantization == value.canonical().quantization(),
            InvalidFormatSnafu {
                message: format!(
                    "vector member quantization mismatch within one key/generation in {context}"
                )
            }
        );
        group.count = group.count.checked_add(1).ok_or_else(|| {
            InvalidFormatSnafu {
                message: format!("vector member count overflow in {context}"),
            }
            .build()
        })?;
        report.members = report.members.checked_add(1).ok_or_else(|| {
            InvalidFormatSnafu {
                message: format!("vector validation report member count overflow in {context}"),
            }
            .build()
        })?;
    }

    let mut base_meta_states = BTreeMap::<Vec<u8>, BaseMetaState>::new();
    let mut meta_read_options = ReadOptions::default();
    meta_read_options.set_verify_checksums(true);
    for entry in db.iterator_cf_opt(&meta_cf, meta_read_options, IteratorMode::Start) {
        let (encoded_key, encoded_value) = entry.context(RocksSnafu)?;
        let is_vector = encoded_value.first() == Some(&(DataType::VectorSet as u8));
        if !is_vector {
            if let Ok(parsed_key) = ParsedBaseKey::new(&encoded_key)
                && member_user_keys.contains(parsed_key.key())
            {
                base_meta_states.insert(parsed_key.key().to_vec(), BaseMetaState::NonVector);
            }
            continue;
        }

        let key = ParsedBaseKey::new(&encoded_key)?;
        let meta = VectorMeta::decode(&encoded_value)?;
        ensure!(
            meta.version() != 0,
            InvalidFormatSnafu {
                message: format!("vector meta has zero generation in {context}")
            }
        );
        ensure!(
            meta.data_revision() != 0,
            InvalidFormatSnafu {
                message: format!("vector meta has zero data revision in {context}")
            }
        );
        if meta.count() != 0 {
            let group = member_groups
                .remove(&(key.key().to_vec(), meta.version()))
                .ok_or_else(|| {
                    InvalidFormatSnafu {
                        message: format!(
                            "vector meta generation {} has no member range in {context}",
                            meta.version()
                        ),
                    }
                    .build()
                })?;
            ensure!(
                group.count == meta.count(),
                InvalidFormatSnafu {
                    message: format!(
                        "vector meta/member count mismatch for generation {} in {context}: meta={}, members={}",
                        meta.version(),
                        meta.count(),
                        group.count
                    )
                }
            );
            ensure!(
                group.dimension == meta.dimension(),
                InvalidFormatSnafu {
                    message: format!(
                        "vector meta/member dimension mismatch for generation {} in {context}",
                        meta.version()
                    )
                }
            );
            ensure!(
                group.quantization == meta.quantization(),
                InvalidFormatSnafu {
                    message: format!(
                        "vector meta/member quantization mismatch for generation {} in {context}",
                        meta.version()
                    )
                }
            );
        }
        base_meta_states.insert(
            key.key().to_vec(),
            BaseMetaState::Vector {
                generation: meta.version(),
            },
        );
        report.metas = report.metas.checked_add(1).ok_or_else(|| {
            InvalidFormatSnafu {
                message: format!("vector validation report meta count overflow in {context}"),
            }
            .build()
        })?;
    }

    for ((key, generation), _) in member_groups {
        match base_meta_states.get(&key) {
            None => {
                return InvalidFormatSnafu {
                    message: format!(
                        "vector member for generation {generation} has missing base meta in {context}"
                    ),
                }
                .fail();
            }
            Some(BaseMetaState::NonVector) => {
                return InvalidFormatSnafu {
                    message: format!(
                        "vector member for generation {generation} has non-vector base meta in {context}"
                    ),
                }
                .fail();
            }
            Some(BaseMetaState::Vector {
                generation: meta_generation,
            }) => {
                // DEL and same-name recreation deliberately leave older
                // generations for the VectorDataCF compaction filter. They
                // remain invisible because all reads are scoped to the meta's
                // current generation. Equal or future unmatched generations
                // cannot be produced by that lifecycle and fail closed.
                ensure!(
                    generation < *meta_generation,
                    InvalidFormatSnafu {
                        message: format!(
                            "vector member generation {generation} has no matching meta generation {meta_generation} in {context}"
                        )
                    }
                );
            }
        }
    }

    Ok(report)
}
