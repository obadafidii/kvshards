# WAL (Write Ahead Log)
This is a protocol that allows the database to achieve _Atomicity_ and _Durability_

## Notes

Crash Recovery: This is the use of _algorithms_ that ensure database.
* consistency
* transaction atomicity
* durability


### UNDO vs REDO
Undo basically is removing the effects of an incomplete or aborted txn. While Redo is the _replaying_ of commited transaciton






















## Resources
* [WalGo](https://github.com/distroaryan/walgo/tree/master)
* [Write-Ahead-Log: Internals of Postgres](https://www.interdb.jp/pg/pgsql09/index.html)
* [Rebuilding Redis](https://www.joshuapare.com/posts/rebuilding-redis/2-wals-and-aofs/)
* [Write-Ahead-Log: Sqlite](https://sqlite.org/wal.html)
* [Postgres WAL Docs](https://www.postgresql.org/docs/current/wal-intro.html)
* [Building WAL that actually survives](https://blog.canoozie.net/disks-lie-building-a-wal-that-actually-survives/)
* [Write-Ahead-Log: Andy Pavlo CMU](https://www.youtube.com/watch?v=CedEy54pe3g)
* [Redo, Undo and WAL logs](https://www.youtube.com/watch?v=uHvR7nOu5m4)
* [wal: Arpit Bhayani](https://www.youtube.com/watch?v=wI4hKwl1Cn4)


**Good resources I found on Twitter [fanout(dot)sh](https://fanout.sh/)