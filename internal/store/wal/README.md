# Write-ahead log

Each shard owns an append-only write-ahead log at
`data/wal_<shard-id>.log`. Keeping one file per shard preserves the project's
shared-nothing model: a shard's memory and persistence are independent of every
other shard.

## Encoding configuration

JSON is the default so existing callers and headerless JSON WALs continue to
work. Choose binary when starting the server with:

```sh
go run . --wal-encoding binary --wal-compression gzip
```

Library callers can set `shards.Config.WAL.Encoding` to `wal.JSONEncoding` or
`wal.BinaryEncoding`, plus the batching, compression, commit, and compaction
fields on `wal.Config`. A WAL file records its encoding, compression, and
checksum scheme in a versioned header. Opening an existing file with a different
codec or compression returns an error; changing its on-disk format requires an
explicit migration, not merely restarting the process.

## Record formats

In JSON mode, each decoded payload is an object describing either a put or a
delete:

```json
{"operation":"put","key":"session:123","value":"active","ttl":1799700000}
{"operation":"delete","key":"session:123"}
```

Version 2 WALs frame both encodings with a big-endian length and CRC32 checksum.
JSON payloads remain easy to inspect after extracting a frame. Binary payloads
store the operation, TTL, key length, value length, key, and value directly.
Optional gzip compression is applied to each payload before its checksum, so
corruption is detected before decompression.

Records are validated before append. Keys must be non-empty, operations must be
`put` or `delete`, and one encoded record may not exceed 16 MiB.

## Write path and durability

The store serializes mutations per shard. With the default batch size of one and
synchronous commit, it appends and syncs the WAL record before changing memory.
If persistence fails, the mutation is not applied. `Sync` is an explicit durable
boundary, and idempotent `Close` performs a final flush and sync.

Larger batches collect writes until `BatchSize` records arrive or
`BatchInterval` elapses. `AsyncCommit` returns without waiting for fsync; the
background committer makes dirty records durable at the interval. These modes
improve throughput in exchange for a bounded window of accepted writes that can
be lost during process or machine failure.

## Compaction

The WAL counts mutation records and identifies stale-heavy history once
`CompactionThreshold` is reached and the record count exceeds twice the live key
count. The store compacts by writing one put for each non-expired key to a
temporary WAL, syncing it, and atomically renaming it over the old file. New
writes are serialized around the replacement. A threshold of zero disables the
automatic trigger.

## Recovery

Opening a store replays its shard's existing WAL from the beginning:

- later puts replace earlier values for the same key;
- deletes remove values restored by earlier puts;
- puts that have already expired are omitted;
- malformed or unsupported records stop startup and report the record number
  (or line number for a legacy JSON-lines WAL).

The file is reopened at its end after replay so new mutations continue the same
log. Empty logs are valid.

Changing the configured shard count changes key routing. WALs cannot simply be
renamed or assigned to different shard IDs; the records must be read and
redistributed using the new hash modulo. Online resharding is intentionally out
of scope for the current implementation.

## Testing

The tests cover JSON and binary round trips, gzip, checksums and corruption,
batch-size and interval commits, async commit boundaries, atomic compaction,
creation, ordered replay, reopening, legacy headerless JSON, format mismatches,
large entries, validation, callback failures, idempotent close, and use after
close. Store tests verify recovery of overwrites, deletes, expired records, and
compacted state.

## Further reading

1. [PostgreSQL WAL introduction](https://www.postgresql.org/docs/current/wal-intro.html)
2. [SQLite WAL documentation](https://sqlite.org/wal.html)
3. [ARIES overview](https://blog.acolyer.org/2016/01/08/aries/)
4. [Per-key-value checksums in RocksDB](https://rocksdb.org/blog/2022/07/18/per-key-value-checksum.html)
