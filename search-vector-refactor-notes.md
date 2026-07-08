# Search Vector Refactor Notes

Date: 2026-07-08

## Background

Current FT/vector implementation in Kiwi is usable, but the internal boundaries are still too flat:

- distance calculation is still modeled as a few helper functions
- vector encoding/decoding is not isolated as a stable abstraction
- search index building and query execution are coupled too tightly to the current `FLAT` implementation
- `SearchKey` encoding lacks a byte-layout comment similar to `BaseKey`/meta-key style documentation

This becomes a problem once we want to support:

- multiple vector index algorithms, such as `FLAT` and `HNSW`
- multiple vector element types
- multiple distance metrics
- different query execution strategies

## Reference Implementations Reviewed

### KV Rocks

Relevant code:

- `kvrocks/src/search/search_encoding.h`
- `kvrocks/src/search/indexer.h`
- `kvrocks/src/search/indexer.cc`
- `kvrocks/src/search/index_manager.h`
- `kvrocks/src/search/index_manager.cc`
- `kvrocks/src/search/hnsw_indexer.h`
- `kvrocks/src/search/hnsw_indexer.cc`
- `kvrocks/src/search/passes/index_selection.h`
- `kvrocks/src/search/executors/hnsw_vector_field_knn_scan_executor.h`

Useful ideas:

- field schema is modeled as typed metadata
- write-path indexing is isolated in `GlobalIndexer` / `IndexUpdater`
- query path is separated into IR -> plan -> executor
- search key construction is centralized

Limits of that design for our case:

- distance calculation is still embedded in concrete HNSW code via `switch`
- vector algorithm implementation and metric logic are not cleanly split

### RediSearch

Relevant code:

- `RediSearch/src/field_spec.h`
- `RediSearch/src/spec.c`
- `RediSearch/src/vector_index.h`
- `RediSearch/src/vector_index.c`
- `RediSearch/src/indexer.c`
- `RediSearch/src/query.c`

Useful ideas:

- vector field schema stores engine-facing params in one place
- index creation parses algorithm/type/dim/metric into a dedicated vector config
- vector writes and vector queries go through a dedicated vector engine boundary
- query execution does not hardcode metric math in upper layers

## Refactor Direction For Kiwi

The target is not to build a full query engine now. The goal is to establish stable internal boundaries so the current implementation can evolve without rewriting storage and command code again.

### 1. Introduce `VectorCodec`

Responsibility:

- validate raw vector bytes against schema
- decode raw bytes into a typed in-memory vector representation
- later support more element types cleanly

Expected effect:

- remove direct scattered calls to `decode_f32_vector`
- isolate binary format concerns from indexing/query logic

### 2. Introduce `VectorDistance`

Responsibility:

- define a common interface for distance computation
- provide implementations for `L2`, `IP`, `COSINE`

Expected effect:

- distance logic becomes replaceable and testable
- algorithm code no longer owns metric-specific math

### 3. Introduce `VectorIndexEngine`

Responsibility:

- validate schema for one algorithm
- upsert vector
- delete vector
- search vectors
- optionally rebuild from source documents

Initial implementation:

- `FlatVectorIndexEngine`

Future implementation:

- `HnswVectorIndexEngine`

Expected effect:

- `SearchIndexManager` no longer depends on concrete `FLAT` details

### 4. Introduce `VectorIndexFactory` or equivalent dispatch

Responsibility:

- select concrete engine from `VectorAlgorithm`

Expected effect:

- current code path remains simple
- future algorithm expansion has one routing point instead of many `match` blocks

### 5. Introduce a clearer write-path indexing boundary

Responsibility:

- determine which indexes are affected by a document key
- validate indexed field values before write
- refresh/delete index entries after write

Expected effect:

- `Storage::hset/hdel/del` keeps orchestration only
- search-specific maintenance logic moves out of general storage flow

### 6. Add `SearchKey` layout documentation

Must add a comment in `search_encoding.rs` describing:

- magic byte
- version byte
- key kind byte
- `index_len`, `field_len`, `doc_key_len`
- exact byte order
- which key kinds include which sections
- why `encode_prefix()` intentionally omits the trailing document section for prefix scan

The comment should follow the style used for base/meta key layout documentation.

## Scope Control

This refactor should stay scoped.

Do now:

- extract codec abstraction
- extract distance abstraction
- extract index engine abstraction
- add search key byte-layout comment
- keep current `FT.CREATE` / `FT.SEARCH` behavior unchanged

Do not do now:

- full query planner like KV Rocks
- real HNSW implementation
- support for JSON indexing
- support for multiple vector element types beyond what already exists

## Suggested Implementation Order

1. add `SearchKey` byte-layout comment first
2. extract `VectorCodec`
3. extract `VectorDistance`
4. convert current flat index into `VectorIndexEngine`
5. route `SearchIndexManager` through algorithm dispatch
6. keep current tests green, then add focused tests around the new abstractions

## Validation Expectations

After refactor, at minimum run:

- `cargo test -p storage --test redis_search_distance_test`
- `cargo test -p storage --test redis_search_encoding_test`
- `cargo test -p storage --test redis_search_index_test`
- `cargo test -p cmd --test ft_search_cmd_test`
- `cargo fmt --check`
- `cargo check --workspace`
