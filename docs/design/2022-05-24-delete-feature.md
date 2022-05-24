# TiBigdata Design Documents

- Author(s): [Shi Yuhang](http://github.com/shiyuhang0)
- Tracking Issue: https://github.com/tidb-incubator/TiBigData/issues/200

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
  * [New Configuration](#new-configuration)
  * [Main Steps](#mian-steps)
  * [Delete Logical](#delete-logical)
  * [Row Order](#row-order)
* [Compatibility](#compatibility)
* [Test Design](#test-design)

## Introduction

Support delete feature for TiBigData/Flink

## Motivation or Background

Currently, TiBigData/Flink doesn't support DELETE RowKind in the TiKV sink. In other words, we can't consume delete changelog to execute delete.
As a real batch&streaming engine, it's necessary to support delete in Flink

## Detailed Design

### New Configuration
We introduce a new configuration `sink.tikv.delete-enable` to control delete.
- The configuration is a boolean type with the default value `false`, which will disable the delete feature
- The configuration can only work in MINIBATCH transaction and upsert mode, or delete RowKind will be filtered
- Only support delete from table with cluster index so far

### Main Steps
Here are the main steps to support the delete feature
- Add the configuration to open delete
- Check if delete is enabled. If you are in MINIBATCH transaction or upsert mode, delete will be disabled even you config `sink.tikv.delete-enable` to `true`
- Use a new class TiRow to distinguish between delete RowKind and insert/update RowKind in MiniBatch
- Optimize deduplication logic in MINIBATCH transaction
- Exclude delete RowKind to upsert when flush rows buffer
- Use delete RowKind to delete when flush rows buffer
  - check cluster index
  - extract handle from pk
  - encode key/value of records and index
- 2PC to commit both the upsert and delete key/value

![image alt text](imgs/delete_feature/delete.png)

### Delete Logical
Now, TiBigData/Flink only supports delete with cluster index because it's hard to extract handle from non-cluster index or table without pk

It will also check if the delete row exists in the table, if not, the row will be ignored

At last, TiBigData/Flink will mix the upsert and delete keyValue to do two phase commit in a transaction

![image alt text](imgs/delete_feature/delete_logical.png)

### Row Order
It is important to keep order in streaming mode, or we may get the error results.
- TiCDC will Ingest the changelogs and sink to kafka. So, make sure kafka will partition the messages by key
- It's better to optimize deduplication and leave the lasted operation for the same row
- When Flink executes sink distributedly, make sure the operations on the same row will be sent to the same task

## Compatibility
- Delete feature can't work with batch mode, and it doesn't support the DELETE statement
- Delete feature can't work in global transaction or append mode
- Delete feature can only work with cluster index so far. For TiDB < 5.0, delete only work with int pk table and config `alter-primary-key = false`

## Test Design
| scenes                                | expected results       |
| ------------------------------------- | ---------------------- |
| global & enable delete                | delete rows be ignored |
| minibatch & enable delete & append    | delete rows be ignored |
| minibatch & disable delete & upsert   | delete rows be ignored |
| minibatch & enable delete & upsert    | delete correctly        |
| table with no pk                      | throw exception        |
| table with no-cluster index           | throw exception        |
| table with cluster index & int pk     | delete correctly        |
| table with cluster index & varchar pk | delete correctly        |
