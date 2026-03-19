# AOF
This is the persistence layer of cache for each shard.


The AOF is one key part of the `shared-nothing architecture` where each shard has its own disk meaning 
  1. No shared resource
  2. No contention because of 1
This improves performance drastically and isolates faults critically.


  The structure of the AOF is in this format, each shard will write to a `./data/wal_[shard-id].aof`.
  
  Questions in my mind.
  1. If I started the shard with 10 shards, then they all create 10 wal files, then I either decreases or increases shard
    * if we decrease how do we get the data from the other wal files
    * if we increase how do we redistribute data very well.
