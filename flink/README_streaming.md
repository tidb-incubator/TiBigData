# TiBigData Streaming Mode

TiBigData supports reading snapshot data from TiDB, and then merging the TiCDC data after this snapshot to build a real-time TiDB table snapshot.

## Table of Contents

* [1 Environment](#1-Environment)
* [2 Configure Flink cluster](#2-Configure-Flink-cluster)
* [3 Configure Kafka cluster](#3-Configure-Kafka-cluster)
* [4 Configure TiCDC](#4-Configure-TiCDC)
* [5 Reading TiDB in Streaming Mode](#5-Reading-TiDB-in-Streaming-Mode )
* [6 Configuration](#6-Configuration)
* [7 TiDB Metadata](#7-TiDB-Metadata)
* [8 Note](#8-Note)

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

See [Apache Kafka QuickStart](https://kafka.apache.org/quickstart)。

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

## 6 Configuration

In addition to supporting the configuration in [TiDB Batch Mode](./README.md), the streaming mode adds the following configuration:

| Configuration                          | Default Value | Description                                                                                                                                                                                                                                         |
|:---------------------------------------|:--------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| tidb.streaming.source                  | -             | The data source(messaging system) where TiDB's change logs are stored, currently only supports Kafka and Pulsar will be supported later.                                                                                                            |
| tidb.streaming.codec                   | -             | TiDB's change log encoding method, currently supports default(json), craft, canal-json, among which craft and canal-json formats require higher TiDB version (5.x), and canal-json must be used with TiDB extended fields enabled to read commitTs. |
| tidb.streaming.kafka.bootstrap.servers | -             | Kafka server address                                                                                                                                                                                                                                |
| tidb.streaming.kafka.topic             | -             | Kafka topic                                                                                                                                                                                                                                         |
| tidb.streaming.kafka.group.id          | -             | Kafka group id                                                                                                                                                                                                                                      |
| tidb.streaming.ignore-parse-errors     | -             | Whether to ignore exceptions in case of decoding failure                                                                                                                                                                                            |
| tidb.metadata.included                 | -             | TiDB Metadata, see [TiDB Metadata](#7-TiDB-Metadata)                                                                                                                                                                                                |

## 7 TiDB Metadata

TiBigData supports adding some additional columns as metadata, which will be appended to the end of the original data.

Currently, the following metadata columns can be selected:

| Metadata         | Description                    |
|:-----------------|:-------------------------------|
| source_event     | Data source is stream or batch |
| commit_version   | Version                        |
| commit_timestamp | time                           |

Enable all metadata：`'tidb.metadata.included' = '*'`；

Enable partial metadata and rename metadata column names：`'tidb.metadata.included' = 'commit_timestamp=ts,commit_version=version,source_event=source'`。

## 8 Note

1. The first time you run a job, TiBigData will read from TiDB by **snapshot time**(configured by `tidb.snapshot_timestamp` or `tidb.snapshot_version` )，then read the CDC data from Kafka after this **snapshot time**, the consumption of Kafka data starts from the earliest offset. After that, when the job is restarted and resumed from checkpoint/savepoint, the data will not be read from TiDB again, but will be consumed from the last recorded Kafka offset;
2. If you do not configure **snapshot time**, we will choose the current time as the snapshot time. We recommend not configuring snapshot time;
3. When metadata columns are enabled, writing will be disabled in Flink;
4. Job parallelism must be less than or equal to the number of partitions in Kafka.


