# Real-time wide table 

- Author(s): [yangxin](http://github.com/xuanyu66)
- Tracking Issue: https://github.com/tidb-incubator/TiBigData/issues/197

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Compatibility](#compatibility)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [References](#references)

## Introduction

This design doc is about how to sink Real-time wide table in TiDBDynamicTable.

## Motivation or Background

It is a common requirement that partially updates some fields based on the primary key in the `Real-time wide table` scenario.

![image alt text](imgs/real-time-wide-table/Materialized-View.png)

Inevitably, it occurs to us that it's a suitable way to use `INSERT ... ON DUPLICATE KEY UPDATE`  
However, Flink SQL doesn't support the statement `INSERT ... ON DUPLICATE KEY UPDATE`. So we need to find an alternative way to achieve `INSERT ... ON DUPLICATE KEY UPDATE` semantics in `TiDBDynamicTable`. 

## Detailed Design

The design will be divided into two parts, one is how it is implemented at the SQL syntax level, in other words, how the user passes all the required parameters. 
The other part is the implementation of `DynamicTableSink` interface to interact with the TiDB-server.

### Use `SQL Hints` to pass update columns

With option `tidb.sink.update-columns` in SQL hints, update-columns would be passed to `TiDBDynamicTable`. Combined with `INSERT INTO` clause, we have gotten all information needed.

For example, `id`, `item_id`, `item_name`, `user_id`, `ts` are the columns needed to be updated, SQL is as follows. The `mock_${value}` will never be used，just a placeholder.

```sql
INSERT INTO `tidb`.`dstDatabase`.`order_wide_table` /*+ OPTIONS('tidb.sink.update-columns'='id, item_id, item_name, user_id, ts') */
VALUES(100, 001, '手机','张三', 2021-12-06 12:01:01, mock_pay_id, mock_pay_amount, mock_pay_status, mock_ps_ts, mock_exp_id, mock_address, mock_recipent, mock_exp_ts)
```

> [!NOTE]
> Currently we don't support ```INSERT INTO `tidb`.`dstDatabase`.`dstDatabase` /*+ OPTIONS('tidb.sink.update-columns'='id, item_id, item_name, user_id, ts') */ (id, item_id, item_name, user_id, ts)
VALUES(100, 001, '手机'，'张三'，2021-12-06 12:01:01)```, since there is a [bug](https://issues.apache.org/jira/browse/FLINK-27683) in Flink SQL.

### Check option 'tidb.sink.update-columns' in catalog properties

'DynamicTable' will inherit properties from catalog. If user set `tidb.sink.update-columns` in catalog as follows, it will make all tables have 'tidb.sink.update-columns' option. 
```sql
CREATE CATALOG `tidb`
WITH (
    'type' = 'tidb',
    'tidb.database.url' = 'jdbc:mysql://localhost:4000/test',
    'tidb.username' = 'root',
    'tidb.password' = '',
    'tidb.sink.update-columns' = 'c1, c2'
);
```

Therefore, it's necessary to check `tidb.sink.update-columns` in catalog properties and throw `IllegalArgumentException` if exists.

### Argument constraints

Due to reasons mentioned in [MySQL Reference Manual](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html), we should try to avoid using an ON DUPLICATE KEY UPDATE clause on tables with multiple unique indexes.
So we will check the following argument constraints before execution in case of an unexpected result.
- the destination table should contain only one not-null unique key(including primary key).
  - Multiple-Column Indexes should be all not-null.
- the update columns should contain the unique key column(including primary key).

It should be noted that this validation is optional(default not skip). Users can explicitly skip the validation(set `tidb.sink.skip-check-update-columns` to true), but be aware of the risks.

### Implement `InsertOrUpdateOnDuplicateSink`

`InsertOrUpdateOnDuplicateSink` implement the interface `DynamicTableSink`, provide `GenericJdbcSinkFunction` to sink data.
`GenericJdbcSinkFunction` require `JdbcOutputFormat` to generate FieldNamedPreparedStatement, extract data from rowData.

![image alt text](imgs/real-time-wide-table/classes.png)

#### Generate FieldNamedPreparedStatement `INSERT ... ON DUPLICATE KEY UPDATE`

With `tidb.sink.update-columns`, we can easily generate `INSERT ... ON DUPLICATE KEY UPDATE` clause as follows. 

```sql
INSERT INTO `tidb`.`dstDatabase`.`order_wide_table` (`id`, `item_id`, `item_name`, `user_id`, `ts`) VALUES (:id, :item_id, :item_name, :user_id, :ts) ON DUPLICATE KEY UPDATE `id`=VALUES(:id), `item_id`=VALUES(:item_id), `item_name`=VALUES(:item_name), `user_id`=VALUES(:user_id), `ts`=VALUES(:ts)
```

### Column Pruning

Use a wrapper class to prune column, which contains an array to store the indexes needed

```java
public class DuplicateKeyUpdateOutputRowData implements RowData {

  private final RowData rowData;
  private final int[] index;

  public DuplicateKeyUpdateOutputRowData(RowData rowData, int[] index) {
    this.rowData = rowData;
    this.index = index;
  }

  @Override
  public int getArity() {
    return index.length;
  }

  @Override
  public boolean isNullAt(int pos) {
    return rowData.isNullAt(index[pos]);
  }

}
```

## Compatibility

- This feature works in both batch and streaming mode.
- This feature only works when `tidb.sink.impl` is `JDBC` and `tidb.write_mode` is `upsert`.

## Test Design

The tests will run on both batch and streaming mode
- insert on duplicate update without unique key.
- insert on duplicate update with only one unique key.
- insert on duplicate update with multiple-column unique key(skip the constraint).
- insert on duplicate update with multiple-column unique key(keep the constraint).


