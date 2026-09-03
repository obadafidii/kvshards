# AOF
This is the persistence layer of cache for each shard.


The AOF is one key part of the `shared-nothing architecture` where each shard has its own disk meaning 
  1. No shared resource
  2. No contention because of 1
This improves performance drastically and isolates faults critically.


  The structure of the AOF is in this format, each shard will write to a `./data/wal_[shard-id].md`.
  
  Questions in my mind.
  1. If I started the shard with 10 shards, then they all create 10 wal files, then I either decrease or increases shard
    * if we decrease how do we get the data from the other wal files
    * if we increase how do we redistribute data very well.


The decision I am making is to first write to WAL (AOF) before making the change to store in memory. This is counter redis style of writing after the operation has been done, but I am not replicating `redis` I want to build "a sharded database with persistence following the shared-nothing architecture".






## Funny thoughts.
Lets say we have a leadership mechanism in-place where we have  master and followers and they must not have the same number of shards do we replay all logs from the master and replaying with each `follower` shard count eg.

```markdown
master = 10 shards

follower_a = 3 shards
follower_b = 8 shards
```

If we are replicating the operations of each shard (lol thinking about it now, we don't have a shared disk (WAL)) how does the replication now happen?

we can have something called a `combiner` combining all wal records while replicating and replaying them based on each `follower's shard-count`.

What is the use-case for this, I don't know 🤷‍♂️


## Resources
1. [WAL on S3](https://x.com/jhleath/status/2090847398625235198)
2. [WALTier](https://github.com/danthegoodman1/waltier)
3. [WAL3](https://www.trychroma.com/engineering/wal3)

---
4. [WAL - LSM](https://blog.lbenicio.dev/post/2025/02/04/write-ahead-logging-under-the-hood-designing-a-durable-wal-for-an-lsm-tree-storage-engine/)
5. [WAL: Durability, Checkpoint](https://shirokoff.ca/blog/write-ahead-log-durability)
6. [WAL: Checksum](https://rocksdb.org/blog/2022/07/18/per-key-value-checksum.html)
7. [WAL: - LSM Storage engine](https://khushal.net/blog/building-lsm-trees-storage-engine/)
8. [WAL: ARIES](https://blog.acolyer.org/2016/01/08/aries/)