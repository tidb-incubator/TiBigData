# 分区表支持

现在，在`tidb.sink.impl`设置为`tikv`的情况下，Flink 1.14 也支持分区表写，但是这里有一些限制。

## 将分区表写入 TiKV

目前，Flink 1.14 仅支持在以下情况下写入范围和哈希分区表：

- 分区表达式是列表达式
- 分区表达式是 `YEAR($argument)`，其中参数是列，其类型是日期时间或可以解析为日期时间的字符串文字。

在Flink 1.14中写入分区表非常简单:只需在 Flink SQL 中使用`INSERT`语句，并且它支持`replace`和`append`语义。

> [注意]
> 目前只支持 utf8mb4 和 [`new_collations_enabled_on_first_bootstrap`](https://docs.pingcap.com/tidb/dev/tidb-configuration-file#new_collations_enabled_on_first_bootstrap)
> 需要在 TiDB 中设置为 `false`。