# TiBigData 流批一体

TiBigData 支持以某一快照读取 TiDB 内存量数据，再合并此快照以后的 TiCDC 数据，构建实时 TiDB 表快照。

## Table of Contents

* [1 环境准备](#1-环境准备)
* [2 配置 Flink 集群](#2-配置-Flink-集群)
* [3 安装并启动 Kafka 集群](#3-安装并启动-Kafka-集群)
* [4 配置并启动 TiCDC](#4-配置并启动-TiCDC)
* [5 利用 Flink 读写 TiDB](#5-利用-Flink-读写-TiDB)
* [6 利用 Flink 删除 TiDB](#6-利用-Flink-删除-TiDB)
* [7 高级配置](#7-高级配置)
* [8 Codec](#8-Codec)
* [9 TiDB Metadata](#9-TiDB-Metadata)
* [10 注意事项](#10-注意事项)
* [11 常见问题](#11-常见问题)

## 1 环境准备

| 组件    | 版本              |
|-------|-----------------|
| JDK   | 8               |
| Maven | 3.6+            |
| Flink | 1.13.x / 1.14.x |
| TiCDC | 4.x / 5.x       |
| Kafka | Flink 支持的所有版本   |

## 2 配置 Flink 集群

参考 [TiDB 与 Flink 集成](./README_zh_CN.md).

## 3 安装并启动 Kafka 集群

参考 [Apache Kafka QuickStart](https://kafka.apache.org/quickstart)。

## 4 配置并启动 TiCDC

本节介绍如何利用 [TiUP](https://tiup.io/) 启动一个简单的 TiCDC 组件用作测试。你需要将下面的地址替换为自己真实的地址。

启动 cdc server：
```bash
tiup cdc server --pd=http://localhost:2379 --log-file=/tmp/ticdc/ticdc.log --addr=0.0.0.0:8301 --advertise-addr=127.0.0.1:8301 --data-dir=/tmp/log/ticdc
```

将 change log 发送至 Kafka：
```bash
tiup cdc cli changefeed create --pd=http://127.0.0.1:2379 --sink-uri="kafka://localhost:9092/test_cdc?kafka-version=2.4.0&partition-num=1&max-message-bytes=67108864&replication-factor=1&protocol=default"
```

## 5 利用 Flink 读写 TiDB

在 TiDB 内创建带有唯一索引的表：


```bash
# 连接至 TiDB
mysql --host 127.0.0.1 --port 4000 -uroot --database test
```

```sql
CREATE TABLE `test`.`test_cdc`(
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(16) 
);
```

以流模式启动 Flink SQL client，创建 TiDB Catalog 并查询：

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

在 TiDB 内对数据进行修改，并且在 Flink SQL client 内观察结果：
```sql
INSERT INTO `test`.`test_cdc` VALUES(1,'zs');
INSERT INTO `test`.`test_cdc` VALUES(2,'ls');
DELETE FROM `test`.`test_cdc` WHERE id = 1;
UPDATE `test`.`test_cdc` SET id = 1 WHERE id = 2;
```

你会发现 Flink 查到的数据与 TiDB 的真实数据一样，并且实时更新。

## 6 利用 Flink 删除 TiDB

在 TiDB 中创建表结构相同的源表和目标表：

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

以流模式启动 Flink SQL client，创建 TiDB Catalog 并执行 insert 语句：

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

在 TiDB 中向源表插入数据，你会发现数据被同步插入到目标表。

```sql
INSERT INTO `test`.`source_table` VALUES(1,'zs');
INSERT INTO `test`.`source_table` VALUES(2,'ls');
INSERT INTO `test`.`source_table` VALUES(3,'is');
```

在 TiDB 中从源表删除数据，你会发现目标表中的数据被同步删除。

```sql
DELETE FROM `test`.`source_table` WHERE id = 1 or id = 2;
```

关键点
- 删除只支持在流模式中运行，因此它只能在 minibatch 下工作，如果在 global 下工作，则会抛出异常。
- 删除只能在 upsert 模式下运行，如果你运行在 append 模式下，则会抛出异常。
- 删除只能在含有至少一个主键或合法唯一索引的表下运行，否则将会抛出异常，合法的唯一索引是指:
  - 唯一索引的值不为 null
  - 如果是联合索引，那么每一列的值都不能为 null
- 删除兼容 source 支持的所有编码方式，目前包括: json,craft 以及 canal-json


## 7 高级配置

除了支持 [TiDB 批模式](./README_zh_CN.md) 中的配置外，流模式新增了以下配置：

| Configuration                          | Default Value | Description                                                                                    |
|:---------------------------------------|:--------------|:-----------------------------------------------------------------------------------------------|
| tidb.source.semantic                   | at-least-once | TiDB 批阶段的消费语义，读取数据失败时才会生效，可选 at-least-once 与 exactly-once。exactly-once 仅支持有唯一索引的表。             |
| tidb.streaming.source                  | -             | TiDB 的变更日志存放的数据源（消息系统），当前只支持配置 Kafka，后续会支持 Pulsar.                                             |
| tidb.streaming.codec                   | craft         | TiDB 的变更日志选取的编码方式，当前支持 json(低版本 TiDB 叫 default)，craft，canal-json 三种格式，详细信息参考 [Codec](#8-Codec) |
| tidb.streaming.kafka.bootstrap.servers | -             | Kafka server 地址                                                                                |
| tidb.streaming.kafka.topic             | -             | Kafka topic                                                                                    |
| tidb.streaming.kafka.group.id          | -             | Kafka group id                                                                                 |
| tidb.streaming.ignore-parse-errors     | false         | 在解码失败时，是否忽略异常                                                                                  |
| tidb.metadata.included                 | -             | TiDB 元数据列，详细信息参考 [TiDB Metadata](#9-TiDB-Metadata)                                             |
| tidb.sink.delete_enable                | false         | 是否在流模式中开启删除，这个配置只有当 `tidb.sink.impl=TIKV` 时才会生效                                                |

## 8 Codec

TiBigData 支持多种 TiCDC 的编码类型，分别是 json(低版本 TiDB 叫 default)，craft，canal-json.

1. json 是 TiCDC 的默认实现，具有很强的可读性；
2. craft 牺牲了可读性，是完全二进制的编码方式，具有更高的压缩率，需要高版本 TiDB(5.x)，当前还在孵化中，但是已经能够正常使用；
3. canal-json 是对 canal 的兼容，使用时必须开启 TiDB 扩展字段以读取 commitTs，低版本的 TiDB 没有这个字段，所以不能使用。

## 9 TiDB Metadata

TiBigData 支持添加一些额外的列作为元数据，元数据列会追加到原始数据的最后。

当前可选择的元数据列有以下几种：

| Metadata         | Description       |
|:-----------------|:------------------|
| source_event     | 数据来源，标志着数据来源是流还是批 |
| commit_version   | 版本                |
| commit_timestamp | 时间                |

启用全部元数据：`'tidb.metadata.included' = '*'`；

启用部分元数据并重命名元数据列名：`'tidb.metadata.included' = 'commit_timestamp=ts'`。

## 10 注意事项

1. 在第一次运行任务时，TiBigData 将从 TiDB 以指定的**快照时间**（可以使用 `tidb.snapshot_timestamp` 或者 `tidb.snapshot_version` 配置）读取存量数据，再从 Kafka 读取此**快照时间**以后的 CDC 数据，对 Kafka 的数据的消费是从 earliest offset 开始；此后任务重启，从 checkpoint/savepoint 恢复的时候，将不会从 TiDB 再次读取数据，而是从上次记录的 Kafka offset 开始消费；
2. **快照时间** 如果不配置，将会以当前任务运行时的快照为准。配置必须满足条件 `(${now} - ${snapshot_timestamp}) + ${batch stage execution time}) < ${GC lifetime}`，自己配置可能会选择错误的版本导致数据不完整，所以我们建议不配置；
3. 启用元数据列后，写入将会被禁用，因为元数据列并不是 TiDB 里真实的数据；
4. 任务并行度必须小于或等于 Kafka topic 的分区数，否则会有状态恢复相关的异常。

## 11 常见问题

### TiBigData 流批一体模式与 Flink TiDB CDC 的区别是什么

TiBigData 将 CDC 的复杂度完全交给原生的 TiCDC，它只需要消费 TiCDC 发送到 Kafka 里的数据，而不用自己在 Flink 内部启动 TiCDC，这可能会更安全。当你需要复用 CDC 数据的时候（一个 TiKV 集群有多个库多个表），TiBigData 是一个很好的选择。如果你不想引入额外的组件，比如 TiCDC 或者 Kafka，Flink TiDB CDC 是一个很好的选择。




