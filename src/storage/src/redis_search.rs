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

use std::collections::BTreeSet;
use std::sync::Arc;

use rocksdb::ReadOptions;
use snafu::{OptionExt, ResultExt};

use crate::error::{OptionNoneSnafu, RedisErrSnafu, RocksSnafu};
use crate::get_db_and_cfs;
use crate::search_distance::distance_for_schema;
use crate::search_encoding::{
    SearchKey, SearchKeyKind, decode_search_meta_value, encode_search_meta_value,
};
use crate::search_types::{
    DistanceMetric, SearchDataType, SearchFieldSchema, SearchIndexSchema, VectorAlgorithm,
    VectorFieldSchema, VectorValueType,
};
use crate::{ColumnFamilyIndex, Redis, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct VectorSearchHit {
    pub doc_key: Vec<u8>,
    pub score: f32,
}

pub struct SearchCfStore<'a> {
    redis: &'a Redis,
}

impl<'a> SearchCfStore<'a> {
    pub fn new(redis: &'a Redis) -> Self {
        Self { redis }
    }

    fn search_cf<'b>(
        &'b self,
    ) -> Result<(&'b dyn engine::Engine, Arc<rocksdb::BoundColumnFamily<'b>>)> {
        let (db, cfs) = get_db_and_cfs!(self.redis, ColumnFamilyIndex::SearchCF);
        debug_assert_eq!(cfs.len(), 1);
        let search_cf = cfs.first().cloned().context(OptionNoneSnafu {
            message: "Search CF is not initialized".to_string(),
        })?;
        Ok((db.as_ref(), search_cf))
    }

    pub fn put_index_schema(&self, schema: &SearchIndexSchema) -> Result<()> {
        let mut batch = self.redis.create_batch()?;
        let index_key = SearchKey::index_meta(schema.name.clone());
        batch.put(
            ColumnFamilyIndex::SearchCF,
            &index_key.encode()?,
            &encode_search_meta_value(schema)?,
        )?;

        for (field_name, field_schema) in &schema.fields {
            let SearchFieldSchema::Vector(vector_schema) = field_schema;
            let field_key = SearchKey::field_meta(schema.name.clone(), field_name.clone());
            batch.put(
                ColumnFamilyIndex::SearchCF,
                &field_key.encode()?,
                &encode_search_meta_value(vector_schema)?,
            )?;
        }

        batch.commit()
    }

    pub fn delete_index(&self, index: &[u8]) -> Result<()> {
        let (db, search_cf) = self.search_cf()?;
        let iter = db.iterator_cf_opt(
            &search_cf,
            ReadOptions::default(),
            rocksdb::IteratorMode::Start,
        );
        let mut batch = self.redis.create_batch()?;
        for item in iter {
            let (encoded_key, _) = item.context(RocksSnafu)?;
            let key = match SearchKey::decode(&encoded_key) {
                Ok(key) => key,
                Err(_) => continue,
            };
            if key.index.as_slice() == index {
                batch.delete(ColumnFamilyIndex::SearchCF, &encoded_key)?;
            }
        }
        batch.commit()
    }

    pub fn load_index_schema(&self, index: &[u8]) -> Result<Option<SearchIndexSchema>> {
        let (db, search_cf) = self.search_cf()?;
        let index_key = SearchKey::index_meta(index.to_vec()).encode()?;
        let Some(index_meta_bytes) = db
            .get_cf_opt(&search_cf, &index_key, &ReadOptions::default())
            .context(RocksSnafu)?
        else {
            return Ok(None);
        };

        let mut index_schema: SearchIndexSchema = decode_search_meta_value(&index_meta_bytes)?;
        let iter = db.iterator_cf_opt(
            &search_cf,
            ReadOptions::default(),
            rocksdb::IteratorMode::Start,
        );
        let mut fields = Vec::new();
        for item in iter {
            let (encoded_key, value_bytes) = item.context(RocksSnafu)?;
            let key = match SearchKey::decode(&encoded_key) {
                Ok(key) => key,
                Err(_) => continue,
            };
            if key.kind != SearchKeyKind::FieldMeta || key.index.as_slice() != index {
                continue;
            }
            let field_schema: VectorFieldSchema = decode_search_meta_value(&value_bytes)?;
            fields.push((key.field, SearchFieldSchema::Vector(field_schema)));
        }
        fields.sort_by(|left, right| left.0.cmp(&right.0));
        index_schema.fields = fields;
        Ok(Some(index_schema))
    }

    pub fn list_index_names(&self) -> Result<Vec<Vec<u8>>> {
        let (db, search_cf) = self.search_cf()?;
        let iter = db.iterator_cf_opt(
            &search_cf,
            ReadOptions::default(),
            rocksdb::IteratorMode::Start,
        );
        let mut names = BTreeSet::new();
        for item in iter {
            let (encoded_key, _) = item.context(RocksSnafu)?;
            let key = match SearchKey::decode(&encoded_key) {
                Ok(key) => key,
                Err(_) => continue,
            };
            if key.kind == SearchKeyKind::IndexMeta {
                names.insert(key.index);
            }
        }
        Ok(names.into_iter().collect())
    }

    pub fn put_vector_entry(
        &self,
        index: &[u8],
        field: &[u8],
        doc_key: &[u8],
        vector: &[u8],
    ) -> Result<()> {
        let mut batch = self.redis.create_batch()?;
        let key = SearchKey::flat_vector_entry(index.to_vec(), field.to_vec(), doc_key.to_vec());
        batch.put(ColumnFamilyIndex::SearchCF, &key.encode()?, vector)?;
        batch.commit()
    }

    pub fn delete_vector_entry(&self, index: &[u8], field: &[u8], doc_key: &[u8]) -> Result<()> {
        let mut batch = self.redis.create_batch()?;
        let key = SearchKey::flat_vector_entry(index.to_vec(), field.to_vec(), doc_key.to_vec());
        batch.delete(ColumnFamilyIndex::SearchCF, &key.encode()?)?;
        batch.commit()
    }

    pub fn iter_vector_entries(
        &self,
        index: &[u8],
        field: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let (db, search_cf) = self.search_cf()?;
        let prefix = SearchKey::flat_vector_entry(index.to_vec(), field.to_vec(), Vec::new())
            .encode_prefix()?;
        let iter = db.iterator_cf_opt(
            &search_cf,
            ReadOptions::default(),
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );
        let mut entries = Vec::new();
        for item in iter {
            let (encoded_key, vector_bytes) = item.context(RocksSnafu)?;
            if !encoded_key.starts_with(&prefix) {
                break;
            }
            let key = SearchKey::decode(&encoded_key)?;
            if key.kind != SearchKeyKind::FlatVectorEntry
                || key.index.as_slice() != index
                || key.field.as_slice() != field
            {
                continue;
            }
            entries.push((key.doc_key, vector_bytes.to_vec()));
        }
        Ok(entries)
    }
}

pub trait VectorIndex {
    fn upsert_vector(
        &self,
        index: &[u8],
        field: &[u8],
        doc_key: &[u8],
        vector: &[u8],
        schema: &VectorFieldSchema,
    ) -> Result<()>;

    fn delete_vector(&self, index: &[u8], field: &[u8], doc_key: &[u8]) -> Result<()>;

    fn knn(
        &self,
        index: &[u8],
        field: &[u8],
        query: &[u8],
        schema: &VectorFieldSchema,
        k: usize,
    ) -> Result<Vec<VectorSearchHit>>;
}

pub struct FlatVectorIndex<'a> {
    store: SearchCfStore<'a>,
}

impl<'a> FlatVectorIndex<'a> {
    pub fn new(redis: &'a Redis) -> Self {
        Self {
            store: SearchCfStore::new(redis),
        }
    }
}

impl<'a> VectorIndex for FlatVectorIndex<'a> {
    fn upsert_vector(
        &self,
        index: &[u8],
        field: &[u8],
        doc_key: &[u8],
        vector: &[u8],
        schema: &VectorFieldSchema,
    ) -> Result<()> {
        validate_flat_vector_schema(schema)?;
        crate::search_distance::decode_f32_vector(vector, schema.dim as usize)?;
        self.store.put_vector_entry(index, field, doc_key, vector)
    }

    fn delete_vector(&self, index: &[u8], field: &[u8], doc_key: &[u8]) -> Result<()> {
        self.store.delete_vector_entry(index, field, doc_key)
    }

    fn knn(
        &self,
        index: &[u8],
        field: &[u8],
        query: &[u8],
        schema: &VectorFieldSchema,
        k: usize,
    ) -> Result<Vec<VectorSearchHit>> {
        validate_flat_vector_schema(schema)?;
        crate::search_distance::decode_f32_vector(query, schema.dim as usize)?;

        let mut hits = Vec::new();
        for (doc_key, vector) in self.store.iter_vector_entries(index, field)? {
            let score = distance_for_schema(schema, query, &vector)?;
            hits.push(VectorSearchHit { doc_key, score });
        }

        hits.sort_by(|left, right| {
            left.score
                .total_cmp(&right.score)
                .then_with(|| left.doc_key.cmp(&right.doc_key))
        });
        hits.truncate(k);
        Ok(hits)
    }
}

pub struct SearchIndexManager<'a> {
    redis: &'a Redis,
}

impl<'a> SearchIndexManager<'a> {
    pub fn new(redis: &'a Redis) -> Self {
        Self { redis }
    }

    fn store(&self) -> SearchCfStore<'a> {
        SearchCfStore::new(self.redis)
    }

    pub fn create_index(&self, schema: SearchIndexSchema) -> Result<()> {
        validate_index_schema(&schema)?;
        if self.store().load_index_schema(&schema.name)?.is_some() {
            return RedisErrSnafu {
                message: format!(
                    "index already exists: {}",
                    String::from_utf8_lossy(&schema.name)
                ),
            }
            .fail();
        }

        self.store().put_index_schema(&schema)?;
        if let Err(error) = self.rebuild_index(&schema) {
            let _ = self.store().delete_index(&schema.name);
            return Err(error);
        }
        Ok(())
    }

    pub fn load_index(&self, index: &[u8]) -> Result<Option<SearchIndexSchema>> {
        self.store().load_index_schema(index)
    }

    pub fn rebuild_index(&self, schema: &SearchIndexSchema) -> Result<()> {
        let flat_index = FlatVectorIndex::new(self.redis);
        let mut unique_keys = BTreeSet::new();
        for prefix in &schema.prefixes {
            for key in self.redis.scan_hash_keys_by_prefix(prefix)? {
                unique_keys.insert(key);
            }
        }

        for doc_key in unique_keys {
            for (field_name, field_schema) in &schema.fields {
                let SearchFieldSchema::Vector(vector_schema) = field_schema;
                if let Some(raw_value) = self.redis.hget_raw(&doc_key, field_name)? {
                    flat_index.upsert_vector(
                        &schema.name,
                        field_name,
                        &doc_key,
                        &raw_value,
                        vector_schema,
                    )?;
                }
            }
        }

        Ok(())
    }

    pub fn refresh_hash_document(&self, doc_key: &[u8]) -> Result<()> {
        let indexes = self.store().list_index_names()?;
        let flat_index = FlatVectorIndex::new(self.redis);

        for index in indexes {
            let Some(schema) = self.load_index(&index)? else {
                continue;
            };
            if !schema
                .prefixes
                .iter()
                .any(|prefix| doc_key.starts_with(prefix))
            {
                continue;
            }

            for (field_name, field_schema) in &schema.fields {
                let SearchFieldSchema::Vector(vector_schema) = field_schema;
                match self.redis.hget_raw(doc_key, field_name)? {
                    Some(raw_value) => {
                        flat_index.upsert_vector(
                            &schema.name,
                            field_name,
                            doc_key,
                            &raw_value,
                            vector_schema,
                        )?;
                    }
                    None => {
                        flat_index.delete_vector(&schema.name, field_name, doc_key)?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn validate_hash_field_value(
        &self,
        doc_key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let indexes = self.store().list_index_names()?;

        for index in indexes {
            let Some(schema) = self.load_index(&index)? else {
                continue;
            };
            if !schema
                .prefixes
                .iter()
                .any(|prefix| doc_key.starts_with(prefix))
            {
                continue;
            }

            for (field_name, field_schema) in &schema.fields {
                if field_name.as_slice() != field {
                    continue;
                }

                let SearchFieldSchema::Vector(vector_schema) = field_schema;
                validate_flat_vector_schema(vector_schema)?;
                crate::search_distance::decode_f32_vector(value, vector_schema.dim as usize)?;
            }
        }

        Ok(())
    }

    pub fn delete_hash_document(&self, doc_key: &[u8]) -> Result<()> {
        let indexes = self.store().list_index_names()?;
        let flat_index = FlatVectorIndex::new(self.redis);

        for index in indexes {
            let Some(schema) = self.load_index(&index)? else {
                continue;
            };
            if !schema
                .prefixes
                .iter()
                .any(|prefix| doc_key.starts_with(prefix))
            {
                continue;
            }

            for (field_name, _) in &schema.fields {
                flat_index.delete_vector(&schema.name, field_name, doc_key)?;
            }
        }

        Ok(())
    }

    pub fn search_knn(
        &self,
        index: &[u8],
        field: &[u8],
        query: &[u8],
        k: usize,
    ) -> Result<Vec<VectorSearchHit>> {
        let schema = self
            .load_index(index)?
            .ok_or_else(|| crate::error::Error::KeyNotFound {
                key: String::from_utf8_lossy(index).to_string(),
                location: Default::default(),
            })?;
        let field_schema = schema
            .fields
            .iter()
            .find_map(|(field_name, field_schema)| {
                if field_name.as_slice() == field {
                    Some(field_schema)
                } else {
                    None
                }
            })
            .ok_or_else(|| crate::error::Error::InvalidArgument {
                message: format!("unknown vector field: {}", String::from_utf8_lossy(field)),
                location: Default::default(),
            })?;
        let SearchFieldSchema::Vector(vector_schema) = field_schema;
        let flat_index = FlatVectorIndex::new(self.redis);
        flat_index.knn(index, field, query, vector_schema, k)
    }
}

fn validate_index_schema(schema: &SearchIndexSchema) -> Result<()> {
    if schema.on != SearchDataType::Hash {
        return Err(crate::error::Error::InvalidArgument {
            message: "only ON HASH is supported".to_string(),
            location: Default::default(),
        });
    }
    if schema.fields.is_empty() {
        return Err(crate::error::Error::InvalidArgument {
            message: "search index must contain at least one vector field".to_string(),
            location: Default::default(),
        });
    }

    for (_, field_schema) in &schema.fields {
        let SearchFieldSchema::Vector(vector_schema) = field_schema;
        validate_flat_vector_schema(vector_schema)?;
    }

    Ok(())
}

fn validate_flat_vector_schema(schema: &VectorFieldSchema) -> Result<()> {
    if schema.algorithm != VectorAlgorithm::Flat {
        return Err(crate::error::Error::InvalidArgument {
            message: "only VECTOR FLAT is supported".to_string(),
            location: Default::default(),
        });
    }
    if schema.value_type != VectorValueType::Float32 {
        return Err(crate::error::Error::InvalidArgument {
            message: "only TYPE FLOAT32 is supported".to_string(),
            location: Default::default(),
        });
    }
    if schema.dim == 0 {
        return Err(crate::error::Error::InvalidArgument {
            message: "vector dimension must be greater than zero".to_string(),
            location: Default::default(),
        });
    }
    if !matches!(
        schema.distance_metric,
        DistanceMetric::L2 | DistanceMetric::IP | DistanceMetric::Cosine
    ) {
        return Err(crate::error::Error::InvalidArgument {
            message: "unsupported vector distance metric".to_string(),
            location: Default::default(),
        });
    }
    Ok(())
}
