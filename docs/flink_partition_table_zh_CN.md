# 分区表支持

现在，在`tidb.sink.impl`设置为`tikv`的情况下，Flink 1.14 也支持分区表读写，但是这里有一些限制。

## 从 TiKV 读取分区表

Flink 1.14 不支持 MySQL/TiDB 分区表语法 `select col_name from table_name partition(partition_name)`，但您仍然可以使用 `where` 条件过滤分区。

Flink 1.14 根据分区类型和与表关联的分区表达式来决定是否应用分区剪枝。

目前，Flink 1.14 对范围分区部分应用了分区修剪。

当范围分区的分区表达式为以下之一时，将应用分区修剪：

- 列表达式

- `YEAR($argument)` 其中参数是一个列，它的类型是日期时间或可以被解析为日期时间的字符串文字。

如果不应用分区修剪，Flink 1.14 的读取相当于对所有分区进行表扫描。

## 将分区表写入 TiKV

目前，Flink 1.14 仅支持在以下情况下写入范围和哈希分区表：

- 分区表达式是列表达式
- 分区表达式是 `YEAR($argument)`，其中参数是列，其类型是日期时间或可以解析为日期时间的字符串文字。

在Flink 1.14中写入分区表非常简单:只需在 Flink SQL 中使用`INSERT`语句，并且它支持`replace`和`append`语义。

> [注意]
> 目前只支持 utf8mb4 和 [`new_collations_enabled_on_first_bootstrap`](https://docs.pingcap.com/tidb/dev/tidb-configuration-file#new_collations_enabled_on_first_bootstrap)
> 需要在 TiDB 中设置为 `false`。