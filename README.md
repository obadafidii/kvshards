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

## Resources
1. [DiceDb - Legacy](https://github.com/dicedb/dice-legacy/blob/master/internal/shard/main.go)
2. [System Design Journal - Sharding](https://github.com/tdadadavid/sd-journal/blob/main/databases/sharding-partitioning.md)
