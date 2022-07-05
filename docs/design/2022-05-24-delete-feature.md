# Flink connector Delete Feature

- Author(s): [Shi Yuhang](http://github.com/shiyuhang0)
- Tracking Issue: https://github.com/tidb-incubator/TiBigData/issues/200

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
  * [Goals](#goals)
  * [New Configuration](#new-configuration)
  * [Main Steps](#main-steps)
  * [Delete Logical](#delete-logical)
  * [Codec](#codec)
  * [Row Order](#row-order)
* [Compatibility](#compatibility)
* [Test Design](#test-design)

## Introduction

Support delete feature for TiBigData/Flink.

## Motivation or Background

Currently, TiBigData/Flink doesn't support DELETE RowKind in the TiKV sink. In other words, we can't consume delete changelog to execute delete.
As a real batch&streaming engine, it's necessary to support delete in Flink.

## Detailed Design

### Goals
Flink does not support DELETE statement now, in other word, we can't ingest data to delete with SQL in batch mode. Therefore, we can only ingest data from the TiCDC changelog in streaming mode, which is DELETE RowKind in Flink.

Streaming mode introduces another problem, if the DELETE RowKind does not have any constraints, then we do not know which row needs to be deleted. So we need a constraint: at least one pk or valid uk.

Pk is easy to understand, a valid uk means:
- The uk's value should not be null.
- Every column should not be null if uk has multiple-column.

In summary, Here are the goals:
- Delete will bypass TiDB.
- Delete is only supported in streaming mode, which means delete can only work in `minibatch` because `global` transaction is for batch mode. If you work in `GLOBAL` transaction, an exception will be thrown.
- Delete is only supported in upsert mode, for append mode does not have delete semantics. If you work in append mode, an exception will be thrown.
- Delete is only supported in tables with at least one pk or valid uk, or an exception will be thrown.

### New Configuration

We introduce a new config `sink.tikv.delete-enable` to control delete.
- The config is a boolean type with the default value `false`, which will disable the delete feature.
- It is a config for streaming. When you work in batch mode and set this config, an exception will be thrown.
- The config works with `tidb.sink.impl=tikv`, it will not work when `tidb.sink.impl=jdbc`.

### Main Steps

Here are the main steps to support the delete feature:
- Add the configuration to open delete.
- Check if delete is enabled. If you are not in minibatch transaction or upsert mode, delete will be disabled even you configure `sink.tikv.delete-enable` to `true`.
- Use a new class TiRow to distinguish between delete RowKind and insert/update RowKind in MiniBatch.
- Exclude delete RowKind to upsert when flush rows buffer.
- Use delete RowKind to delete when flush rows buffer.
  - check pk/uk
  - extract handle
  - encode key/value of records and index
- 2PC to commit both the upsert and delete key/value.

![image alt text](imgs/delete_feature/delete.png)

### Delete Logical

> TiBigData/Flink only supports delete from table with pk/uk, or exception will be thrown.

At first, check pk/uk and get snapshot.

Then, get handle and value of delete row ,we will ignore the rows which do not exist in the table.

After that, generate record key/value with handle and generate index key/value with index.

At last, mix the upsert and delete keyValue to do two phase commit in a transaction.

![image alt text](imgs/delete_feature/delete_logical.png)

### Codec
TiBigData supports three TiCDC encoding types, namely json(called default in the lower version of tidb), craft, and canal-json.

Delete supports all three encoding types. when you use canal-json, pay attention to adding `enable-tidb-extension=true` config when we create changefeed with TiCDC.

The TiDB source can't ingest any data without `enable-tidb-extension=true`, thus we can not perform delete in the sink too.

### Row Order

It is important to keep order in streaming mode, or we may get the error results.
- TiCDC will ingest the changelogs and sink to kafka. So, make sure kafka will partition the messages by key.
- It's better to optimize deduplication and leave the latest operation for the same row.
- When Flink runs distributed, make sure the operations on the same row will be sent to the same task.

## Compatibility

- Delete is only supported in Flink 1.14.
- Delete can work with TiDB >= 4.0.

## Test Design

> All the test will work in streaming mode

| scenes                        | expected results   |
|-------------------------------|--------------------|
| enable delete & append        | throw exception    |
| disable delete & upsert       | throw exception    |
| enable delete & upsert        | delete correctly   |
| table with pk                 | delete correctly   |
| table with uk                 | delete correctly   |
| table with multiple-column uk | delete correctly   |
| codec: json                   | delete correctly   |
| codec: craft                  | delete correctly   |
| codec: canal_json             | delete correctly   |
