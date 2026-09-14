# Shards

KVShard partitions keys across independent shards. Each shard owns one in-memory
store and one write-ahead log (WAL), so no map or WAL file is shared between
shards.

## Routing

The manager hashes a key with `xxhash64` and chooses a shard with:

```text
shard index = hash(key) % shard count
```

Routing is deterministic while the shard count stays unchanged: every command
for a key reaches the same shard. Changing the shard count changes the modulo
and therefore requires an explicit data migration; online resharding is not yet
implemented.

The shard count must be greater than zero. Shard IDs are zero-based and cannot
be negative.

## Lifecycle

1. The manager creates every shard and its store.
2. Each store opens `data/wal_<shard-id>.log` using the configured encoding and
   replays its records in order.
3. `Run` starts one lifecycle loop per shard.
4. Every five seconds, a shard removes expired values from memory.
5. Cancelling the parent context stops every loop, syncs and closes every WAL,
   and waits for all shards before returning.

Expired values are also rejected lazily by `GET` and `EXISTS`, so callers never
observe stale data between periodic sweeps. A TTL is an absolute Unix timestamp;
zero or a negative value means the key does not expire.

## Store and WAL integration

With the durable defaults, `PUT` and `DEL` records are appended and synced to the
shard's WAL before the in-memory map is changed. In batching or async mode, an
accepted record may remain pending until the next configured commit boundary. A
failed append leaves memory unchanged. On restart, the store replays puts,
overwrites, and deletes, and skips values whose TTL has already elapsed.

WAL records can use JSON or a compact binary payload inside length-prefixed,
CRC32-checksummed frames. JSON is the backward-compatible default; pass
`--wal-encoding binary` to select binary and `--wal-compression gzip` to compress
each record. Each file stores its format, and opening it with a different
encoding or compression setting fails instead of misreading data. A single WAL
record is limited to 16 MiB so replay has a predictable memory bound.

Commits are configurable with `--wal-batch-size`, `--wal-batch-interval`, and
`--wal-async-commit`. The durable default syncs every mutation. A larger batch
holds records until the batch fills or its interval elapses. Async mode flushes
full batches to the operating system without waiting for disk sync; the
background committer performs the sync at the configured interval. `Flush` and
shutdown always force a disk sync.

The shard checks for stale-heavy logs after its periodic expiry sweep. Once the
configured record threshold is reached and history is more than twice the live
key count, it atomically compacts the WAL to one put per live key. Set
`--wal-compaction-threshold 0` to disable automatic compaction.

## Command path

The TCP server finds a command template, copies it for request-local arguments,
routes by the first key, and executes it against the selected shard. Copying the
template prevents concurrent clients from racing on the global command registry.
Supported commands are:

```text
PUT key value [absolute-unix-expiry]
GET key
DEL key
EXISTS key
```

`PUT` and `DEL` return `ok`, `GET` returns the stored value, and `EXISTS` returns
`true` or `false`.

## Guarantees and current limits

- Writes within one shard are serialized, keeping WAL order and the in-memory
  key count consistent under concurrency.
- Repeating `PUT` for a key overwrites its value without increasing shard size.
- Repeating `DEL` returns `key not found` and never makes the size negative.
- A malformed WAL entry fails shard startup with its record location rather
  than silently restoring partial or invalid state.
- Transactions across multiple keys or shards are not supported.
- Replication and online resharding are not yet implemented.

## Tests

Run the full suite, including the race detector, from the repository root:

```sh
go test ./...
go test -race ./...
```

Coverage includes deterministic routing, invalid shard counts, shutdown,
command validation, puts and overwrites, deletes, TTL boundaries and cleanup,
WAL recovery and corruption, batching, asynchronous commit, gzip compression,
checksums, compaction, closed-WAL behavior, and concurrent writes.
