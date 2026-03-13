# KVShard

This is a key-value store that is natively sharded, shards can be configurable.

## Motivation
The reason behind building this project is because I am studying database sharding, 
1. I want to understand it
2. I remember while contributing to dicedb, I saw them using shards [here](https://github.com/dicedb/dice-legacy/blob/master/internal/shard/main.go). This is the major motivation for me actually.
3. The fun of it.


## Note
In [dicedb](https://github.com/dicedb/dice-legacy), there are commands that are multi-shards and single-shards. I am going to implement both

## Goal
Measure performance for each number of shard and check the optimal number of shards.

## Resources
1. [DiceDb - Legacy](https://github.com/dicedb/dice-legacy/blob/master/internal/shard/main.go)
2. [System Design Journal - Sharding](https://github.com/tdadadavid/sd-journal/blob/main/databases/sharding-partitioning.md)
