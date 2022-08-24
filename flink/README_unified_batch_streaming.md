# TiBigData Unified Batch & Streaming

TiBigData supports reading snapshot data from TiDB, and then merging the TiCDC data after this snapshot to build a real-time TiDB table snapshot.

## Table of Contents

* [1 Environment](#1-Environment)
* [2 Configure Flink cluster](#2-Configure-Flink-cluster)
* [3 Configure Kafka cluster](#3-Configure-Kafka-cluster)
* [4 Configure TiCDC](#4-Configure-TiCDC)
* [5 Reading TiDB in Streaming Mode](#5-Reading-TiDB-in-Streaming-Mode )
* [6 Delete in Streaming Mode](#6-Delete-in-Streaming-Mode)
* [7 Configuration](#7-Configuration)
* [8 Codec](#8-Codec)
* [9 TiDB Metadata](#9-TiDB-Metadata)
* [10 Note](#10-Note)
* [11 Questions and Answers](#11-Questions-and-Answers)

## 1 Environment

| Component | Version                         |
|-----------|---------------------------------|
| JDK       | 8                               |
| Maven     | 3.6+                            |
| Flink     | 1.13.x / 1.14.x                 |
| TiCDC     | 4.x / 5.x                       |
| Kafka     | All versions supported by Flink |

## 2 Configure Flink cluster

See [Flink-TiDB-Connector](./README.md).

## 3 Configure Kafka cluster

See [Apache Kafka QuickStart](https://kafka.apache.org/quickstart).

## 4 Configure TiCDC

This section describes how to use [TiUP](https://tiup.io/) to launch a simple TiCDC component for testing. You need to replace the following address with your real address.

Start cdc server：
```bash
tiup cdc server --pd=http://localhost:2379 --log-file=/tmp/ticdc/ticdc.log --addr=0.0.0.0:8301 --advertise-addr=127.0.0.1:8301 --data-dir=/tmp/log/ticdc
```

Send change log to Kafka：
```bash
tiup cdc cli changefeed create --pd=http://127.0.0.1:2379 --sink-uri="kafka://localhost:9092/test_cdc?kafka-version=2.4.0&partition-num=1&max-message-bytes=67108864&replication-factor=1&protocol=default"
```

## 5 Reading TiDB in Streaming Mode 

Create a table with a unique index in tidb:

```bash
# Connect to TiDB
mysql --host 127.0.0.1 --port 4000 -uroot --database test
```

```sql
CREATE TABLE `test`.`test_cdc`(
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(16) 
);
```
Start Flink SQL client in streaming mode, create TiDB Catalog and query:

```sql
SET 'sql-client.execution.result-mode' = 'table';

CREATE CATALOG `tidb`
WITH (
  'type' = 'tidb',
  'tidb.database.url' = 'jdbc:mysql://localhost:4000/',
  'tidb.username' = 'root',
  'tidb.password' = '',
  'tidb.streaming.source' = 'kafka',
  'tidb.streaming.codec' = 'json',
  'tidb.streaming.kafka.bootstrap.servers' = 'localhost:9092',
  'tidb.streaming.kafka.topic' = 'test_cdc',
  'tidb.streaming.kafka.group.id' = 'test_cdc_group',
  'tidb.streaming.ignore-parse-errors' = 'true'
);

SELECT * FROM `tidb`.`test`.`test_cdc`;
```

Modify the data in tidb and observe the results in Flink SQL client:

```sql
INSERT INTO `test`.`test_cdc` VALUES(1,'zs');
INSERT INTO `test`.`test_cdc` VALUES(2,'ls');
DELETE FROM `test`.`test_cdc` WHERE id = 1;
UPDATE `test`.`test_cdc` SET id = 1 WHERE id = 2;
```

You will find that the data in Flink is the same as the real data of tidb and is updated in real time.

## 6 Delete in Streaming Mode

Create source table and target table with same schema in TiDB:

```sql
CREATE TABLE `test`.`source_table`(
    id BIGINT(20) PRIMARY KEY ,
    name VARCHAR(16) ,
    PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
);

CREATE TABLE `test`.`target_table`(
    id BIGINT(20) PRIMARY KEY ,
    name VARCHAR(16) ,
    PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
);
```

Start Flink SQL client in streaming mode, create TiDB Catalog and insert:

```sql
SET 'sql-client.execution.result-mode' = 'table';

CREATE CATALOG `tidb`
WITH (
  'type' = 'tidb',
  'tidb.database.url' = 'jdbc:mysql://localhost:4000/',
  'tidb.username' = 'root',
  'tidb.password' = '',
  'tidb.streaming.source' = 'kafka',
  'tidb.streaming.codec' = 'json',
  'tidb.streaming.kafka.bootstrap.servers' = 'localhost:9092',
  'tidb.streaming.kafka.topic' = 'test_cdc',
  'tidb.streaming.kafka.group.id' = 'test_cdc_group',
  'tidb.streaming.ignore-parse-errors' = 'true',
  'tidb.sink.impl' = 'tikv',
  'tidb.write_mode' = 'upsert',
  'tikv.sink.transaction' = 'minibatch',
  'tikv.sink.delete_enable' = 'true'
);

INSERT INTO `tidb`.`test`.`target_table` SELECT id,name FROM `tidb`.`test`.`source_table`;
```

Insert into source table, and you will find data also be inserted into target table

```sql
INSERT INTO `test`.`source_table` VALUES(1,'zs');
INSERT INTO `test`.`source_table` VALUES(2,'ls');
INSERT INTO `test`.`source_table` VALUES(3,'is');
```

Delete from source table, and you will find data also be deleted from target table

```sql
DELETE FROM `test`.`source_table` WHERE id = 1 or id = 2;
```

Keypoints
- Delete is only supported in streaming mode, which means delete can only work in minibatch because global transaction is for batch mode. If you work in GLOBAL transaction, an exception will be thrown.
- Delete is only supported in upsert mode, If you work in append mode, an exception will be thrown.
- Delete is only supported in tables with at least one pk or valid uk, or an exception will be thrown. valid uk means:
  - The uk's value should not be null.
  - Every column should not be null if uk has multiple-column.
- Delete is compatible with all the codecs that is supported by the source, currently including json, craft and canal_json.

## 7 Configuration

In addition to supporting the configuration in [TiDB Batch Mode](./README.md), the streaming mode adds the following configuration:

| Configuration                          | Default Value | Description                                                                                                                                                                                    |
|:---------------------------------------|:--------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| tidb.source.semantic                   | at-least-once | TiDB batch stage consumption semantics, which takes effect when the read data fails, optionally at-least-once and exactly-once. exactly-once is only supported for tables with unique indexes. |
| tidb.streaming.source                  | -             | The data source(messaging system) where TiDB's change logs are stored, currently only supports Kafka and Pulsar will be supported later.                                                       |
| tidb.streaming.codec                   | craft         | TiDB's change log encoding method, currently supports json(called default in the lower version of tidb), craft, canal-json. See [Codec](#8-Codec)                                              |
| tidb.streaming.kafka.bootstrap.servers | -             | Kafka server address                                                                                                                                                                           |
| tidb.streaming.kafka.topic             | -             | Kafka topic                                                                                                                                                                                    |
| tidb.streaming.kafka.group.id          | -             | Kafka group id                                                                                                                                                                                 |
| tidb.streaming.ignore-parse-errors     | false         | Whether to ignore exceptions in case of decoding failure                                                                                                                                       |
| tidb.metadata.included                 | -             | TiDB Metadata, see [TiDB Metadata](#9-TiDB-Metadata)                                                                                                                                           |
| tikv.sink.delete_enable                | false         | Whether enable delete in streaming, this config only works in `tidb.sink.impl=TIKV`                                                                                                            |

## 8 Codec

TiBigData supports several TiCDC encoding types, namely json(called default in the lower version of tidb), craft, and canal-json.

1. json is the default implementation of TiCDC and is highly readable;
2. craft sacrifices readability, is fully binary encoded, has higher compression, and requires a high version of TiDB(5.x). Currently, craft is still incubating, but it is working properly;
3. canal-json is compatible with canal and must be used with the TiDB extension field enabled to read `commitTs`. Lower versions of TiDB do not have `commitTs`, so it cannot be used.

## 9 TiDB Metadata

TiBigData supports adding some additional columns as metadata, which will be appended to the end of the original data.

Currently, the following metadata columns can be selected:

| Metadata         | Description                    |
|:-----------------|:-------------------------------|
| source_event     | Data source is stream or batch |
| commit_version   | Version                        |
| commit_timestamp | time                           |

Enable all metadata：`'tidb.metadata.included' = '*'`；

Enable partial metadata and rename metadata column names：`'tidb.metadata.included' = 'commit_timestamp=ts,commit_version=version,source_event=source'`.

## 10 Note

1. The first time you run a job, TiBigData will read from TiDB by **snapshot time**(configured by `tidb.snapshot_timestamp` or `tidb.snapshot_version` )，then read the CDC data from Kafka after this **snapshot time**, the consumption of Kafka data starts from the earliest offset. After that, when the job is restarted and resumed from checkpoint/savepoint, the data will not be read from TiDB again, but will be consumed from the last recorded Kafka offset;
2. If you do not configure **snapshot time**, we will choose the current time as the snapshot time. The configuration must meet this condition `(${now} - ${snapshot_timestamp}) + ${batch stage execution time}) < ${GC lifetime}`. Configuring it yourself may result in incomplete data due to the selection of the wrong version, so we recommend not configuring it;
3. When metadata columns are enabled, writing will be disabled in Flink, because metadata columns are not real data in TiDB;
4. Job parallelism must be less than or equal to the number of kafka topic partitions.

## 11 Questions and Answers

### What is the difference between TiBigData Unified Batch & Streaming mode and Flink TiDB CDC

TiBigData leaves the complexity of CDC entirely to native TiCDC, it only needs to consume the data sent to Kafka by TiCDC instead of starting TiCDC inside Flink itself, which may be safer. When you need to reuse CDC data (a TiKV cluster with multiple databases and multiple tables), TiBigData is a good choice. If you don't want to introduce additional components like TiCDC or Kafka, Flink TiDB CDC is a good choice.

