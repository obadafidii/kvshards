# KVShard

This is a key-value store that is natively sharded, shards can be configurable.

## Motivation
The reason behind building this project is because I am studying database sharding, 
1. I want to understand it
2. I remember while contributing to dicedb, I saw them using shards [here](https://github.com/dicedb/dice-legacy/blob/master/internal/shard/main.go). This is the major motivation for me actually.
3. The fun of it.


## Note
In [dicedb](https://github.com/dicedb/dice-legacy), there are commands that are multi-shards and single-shards. I am going to implement both.

The more I am building this thing, I am getting to see that if you want to see the real effect of the sharded nature
I have to add an _eviction policy_ system into this cache, to evict keys from each shard based on certain strategies.


performance! performance!! performance!!! I need to track performance
## Goal
Measure performance for each number of shard and check the optimal number of shards.


## Current implementation

Keys are deterministically routed across independent shards. Each shard owns an
in-memory store and a durable, checksummed WAL, restores its state on startup,
removes expired keys, and shuts down cleanly when the server context is
cancelled. `PUT`, `GET`, `DEL`, and `EXISTS` are available over the TCP server.

See the [shard design and guarantees](internal/shards/README.md) and the
[WAL notes](internal/store/wal/README.md) for details.

Run the server with `go run . --shards 4`, then connect to port `9500` with a TCP
client such as `nc localhost 9500`. JSON WAL records are the default; select the
binary codec with `go run . --shards 4 --wal-encoding binary`.

The WAL also supports batching, asynchronous commits, gzip compression,
checksummed records, and automatic log compaction. For example:

```sh
go run . --shards 4 \
  --wal-encoding binary \
  --wal-compression gzip \
  --wal-batch-size 64 \
  --wal-batch-interval 25ms \
  --wal-async-commit \
  --wal-compaction-threshold 10000
```

The default (`batch-size=1`, synchronous commit, no compression) prioritizes
durability. Batching and asynchronous commit improve throughput but can lose the
most recent accepted writes if the process or machine fails before the next
background commit.

## Resources
1. [DiceDb - Legacy](https://github.com/dicedb/dice-legacy/blob/master/internal/shard/main.go)
2. [System Design Journal - Sharding](https://github.com/tdadadavid/sd-journal/blob/main/databases/sharding-partitioning.md)
3. [Sharding nothing architecture-Snowflake example](https://medium.com/@BuildandDebug/shared-disk-vs-shared-nothing-architecture-a-deep-dive-with-snowflake-as-a-case-study-80821098f934)
4. [The case of shared-nothing architecture - original paper](https://dsf.berkeley.edu/papers/hpts85-nothing.pdf)
5. [Parallelizing Query Optimization
   on Shared-Nothing Architectures](https://infoscience.epfl.ch/server/api/core/bitstreams/fcd857f9-f5eb-45e4-94af-9c3761a63231/content)
6. [Shared architecture spectrum](https://blog.purestorage.com/perspectives/the-storage-architecture-spectrum-why-shared-nothing-means-nothing/?print=pdf)
7. [Shared-nothing architecture - Aerospike](https://aerospike.com/blog/shared-nothing-architecture/)
8. [Concurrency guide](https://tallysolutions.com/technology/concurrent-computing-guide/)
9. [Concurrency deep-dive](https://nathanpeck.com/concurrency-deep-dive-strategies-for-high-traffic-applications/)
10. [Mastering concurrency](https://www.harrisonclarke.com/blog/mastering-concurrency-a-guide-for-software-engineers)
11. [Parallel Database Systems](https://www.cs.cmu.edu/~15721-f24/papers/Parallel_Database_Systems.pdf)
12. [Development of a Parallel DBMS on the Basis of PostgreSQL](https://ceur-ws.org/Vol-735/paper10.pdf#:~:text=One%20of%20the%20directions%20mentioned%20above%20is,represents%20Post%2D%20greSQL%20with%20embedded%20partitioned%20parallelism.)
