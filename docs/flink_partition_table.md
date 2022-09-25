# Partition Table Support

Now, Flink 1.14 supports partition table writing when `tidb.sink.impl` is set as `tikv` , but there are some limitations here.

## Writing partition table into TiKV

Currently, Flink 1.14 only supports writing into the range and hash partition table under the following conditions:

- the partition expression is column expression
- the partition expression is `YEAR($argument)` where the argument is a column and its type is datetime or string literal that can be parsed as datetime.

Writing to partition table in Flink 1.14 is very easy: Just use `INSERT` statement with Flink SQL, and it supports replace and append semantics.

> [!NOTE]
> Currently the charset only supported is utf8mb4 and [`new_collations_enabled_on_first_bootstrap`](https://docs.pingcap.com/tidb/dev/tidb-configuration-file#new_collations_enabled_on_first_bootstrap)
> need to be set to `false` in TiDB.