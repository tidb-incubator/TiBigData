# TiDB 与 Flink 集成

## Table of Contents

* [1 环境准备](#1-环境准备)
* [2 编译 Flink Connector](#2-编译-flink-connector)
* [3 部署 Flink](#3-部署-flink)
  * [3.1 下载安装包](#31-下载安装包)
  * [3.2 安装 TiBigData 并启动 Flink 集群](#32-安装-tibigdata-并启动-flink-集群)
* [利用 Flink 读写 TiDB](#利用-flink-读写-tidb)
* [实时宽表场景](#实时宽表场景)
* [支持和限制](#支持和限制)
* [Flink 与 TiDB 的类型映射](#flink-与-tidb-的类型映射)
* [高级配置](#高级配置)
* [TableFactory（废弃）](#tablefactory废弃)
* [常见问题](#常见问题)
  * [TiBigData 会占用 TiDB 的资源吗](#tibigdata-会占用-tidb-的资源吗)
  * [Flink 的配置应该如何设置](#flink-的配置应该如何设置)
  * [我该如何设置并发度来控制任务运行的时长](#我该如何设置并发度来控制任务运行的时长)

## 1 环境准备

| 组件    | 版本                                |
|-------|-----------------------------------|
| JDK   | 8                                 |
| Maven | 3.6+                              |
| Flink | 1.11.x / 1.12.x / 1.13.x / 1.14.x |

## 2 编译 Flink Connector
请参考以下步骤。TiBigData 的 API 基于 1.11.0/1.12.0/1.13.0/1.14.0 小版本构建，如果你的 Flink 版本是 1.14.x，需要将 Flink 的版本手动从 1.14.0 替换为 1.14.x 来避免一些奇怪的问题，这是因为 Flink 在小版本改动的时候也有可能改动 API 的接口。

```bash
# 克隆项目
git clone git@github.com:tidb-incubator/TiBigData.git
cd TiBigData

# 编译 flink connector, 我们以 1.14.3 为例，你需要先设置 TiBigData 编译的模块为 flink-1.14 并且设置 Flink 的版本为 1.14.3
mvn clean package -DskipTests -am -pl flink/flink-1.14 -Ddep.flink.version=1.14.3 -Dmysql.driver.scope=compile -Dflink.jdbc.connector.scope=compile -Dflink.kafka.connector.scope=compile
```
因为 Flink 的依赖较多，根据网络状况与电脑配置，整个过程可能需要花费 10 到 30 分钟，国内用户推荐使用国内 maven 仓库来加速。

以下是可选的编译参数：

| 参数                            | 默认值    | 描述                                                        |
|-------------------------------|--------|-----------------------------------------------------------|
| -Ddep.flink.version           | 1.14.0 | flink 的版本，注意大版本需要与 TiBigData 模块对齐                         |
| -Dmysql.driver.scope          | test   | 是否包含 mysql jdbc driver 依赖编译，可设置为 compile 以包含此依赖，默认不包含     |
| -Dflink.jdbc.connector.scope  | test   | 是否包含 flink jdbc connector 依赖编译，可设置为 compile 以包含此依赖，默认不包含  |
| -Dflink.kafka.connector.scope | test   | 是否包含 flink kafka connector 依赖编译，可设置为 compile 以包含此依赖，默认不包含 |

## 3 部署 Flink

Flink 提供多种部署方式，本文仅提供单机版的 Flink 部署用于测试，如果你想在生产环境使用 Flink, 请参考 [Flink 官方文档](https://flink.apache.org/)。

我们推荐使用 Flink 1.14 版本，下面的步骤以 Flink 1.14 为例，其他版本的 Flink 安装步骤大致相同。

### 3.1 下载安装包

请到 [Flink 下载页面](https://flink.apache.org/downloads.html) 下载对应版本的安装包，下载 scala 2.11 或者 2.12 编译的 Flink 均可。页面仅保留最新的 Flink 版本，历史版本可在这里下载：[Flink 历史版本](http://archive.apache.org/dist/flink)。

国内用户可以使用国内镜像进行下载来追求更快的下载速度，比如 [腾讯镜像](https://mirrors.cloud.tencent.com/apache/flink/)。

### 3.2 安装 TiBigData 并启动 Flink 集群

```bash
# 解压 flink 的二进制安装包，我们以 flink-1.14.3 为例
tar -zxf flink-1.14.3-bin-scala_2.11.tgz
# 进入到 flink 的 home 目录
cd flink-1.14.3
# 拷贝编译出来的 tibigdata 组件到 flink 的 lib 目录
cp ${TIBIGDATA_HOME}/flink/flink-1.14/target/flink-tidb-connector-1.14-${TIBIGDATA_VERSION}.jar lib
# 启动 flink 集群
bin/start-cluster.sh
```

此时你可以访问 http://localhost:8081 来查看 Flink 的 web 页面。

## 利用 Flink 读写 TiDB

TiBigData 支持以**批模式**和**流批一体模式**消费数据。本篇后续内容仅介绍以**批模式**读取 TiDB 数据，**流批一体模式**请参考 [TiBigData 流批一体模式](./README_unified_batch_streaming_zh_CN.md).

在 Flink 集群部署完成后，你可以尝试使用 Flink 的 sql-client 来读写 TiDB 内表的数据。

```bash
 # 启动 flink sql 客户端
 bin/sql-client.sh
```

进入 sql 客户端以后，就可以创建 TiDB 对应的 catalog 了，下面的连接串、用户名以及密码需要替换成自己真实 TiDB 集群的。

```sql
CREATE CATALOG `tidb`
WITH (
    'type' = 'tidb',
    'tidb.database.url' = 'jdbc:mysql://localhost:4000/test',
    'tidb.username' = 'root',
    'tidb.password' = ''
);
```

我们尝试在 TiDB 内创建一张表。

```bash
# 连接至 TiDB
mysql --host 127.0.0.1 --port 4000 -uroot --database test
```

在 TiDB 建表：

```sql
-- 这段 sql 跑在 TiDB 内
CREATE TABLE `people`(
  `id` int,
  `name` varchar(16)
);
```

建完 TiDB 的表以后，我们可以在 Flink 内查看刚刚建出来的 TiDB 的表结构：

```sql
DESC `tidb`.`test`.`people`;
```

你会得到以下信息：
```sql
Flink SQL> DESC `tidb`.`test`.`people`;
+------+--------+------+-----+--------+-----------+
| name |   type | null | key | extras | watermark |
+------+--------+------+-----+--------+-----------+
|   id |    INT | true |     |        |           |
| name | STRING | true |     |        |           |
+------+--------+------+-----+--------+-----------+
2 rows in set
```



尝试在 Flink 内向 TiDB 插入一条数据并查询：

```sql
SET sql-client.execution.result-mode=tableau;
INSERT INTO `tidb`.`test`.`people`(`id`,`name`) VALUES(1,'zs');
SELECT * FROM `tidb`.`test`.`people`;
```
你会得到以下信息：
```sql
Flink SQL> SET sql-client.execution.result-mode=tableau;
[INFO] Session property has been set.

Flink SQL> INSERT INTO `tidb`.`test`.`people`(`id`,`name`) VALUES(1,'zs');
[INFO] Submitting SQL update statement to the cluster...
[INFO] SQL update statement has been successfully submitted to the cluster:
Job ID: a3944d4656785e36cf03fa419533b12c

Flink SQL> SELECT * FROM `tidb`.`test`.`people`;
+----+-------------+--------------------------------+
| op |          id |                           name |
+----+-------------+--------------------------------+
| +I |           1 |                             zs |
+----+-------------+--------------------------------+
Received a total of 1 row
```

至此，你已经知道如何在 Flink 内使用 TiBigData 了。更多高级的功能以及配置调优可参考下面的章节。


## 实时宽表场景

请看[文档](../docs/real-time_wide_table_zh_CN.md)

## 支持和限制

TiBigDate/Flink 支持向聚簇索引表写入数据、读取数据，这是TiDB-5.0.0的一个新特性。

TiBigDate/Flink 暂不支持以下功能：
- bypass TiDB 写带有 AUTO_RANDOM 列的表
- 分区表

## Flink 与 TiDB 的类型映射

TiDB 与 Flink 的类型映射关系可参考下表：

|     TiDB     |    Flink     |
|:------------:|:------------:|
|   TINYINT    |   TINYINT    |
|   SMALLINT   |   SMALLINT   |
|  MEDIUMINT   |     INT      |
|     INT      |     INT      |
|    BIGINT    |    BIGINT    |
|     CHAR     |    STRING    |
|   VARCHAR    |    STRING    |
|   TINYTEXT   |    STRING    |
|  MEDIUMTEXT  |    STRING    |
|     TEXT     |    STRING    |
|   LONGTEXT   |    STRING    |
|    BINARY    |    BYTES     |
|  VARBINARY   |    BYTES     |
|   TINYBLOB   |    BYTES     |
|  MEDIUMBLOB  |    BYTES     |
|     BLOB     |    BYTES     |
|   LONGBLOB   |    BYTES     |
|    FLOAT     |    FLOAT     |
|    DOUBLE    |    DOUBLE    |
| DECIMAL(p,s) | DECIMAL(p,s) |
|     DATE     |     DATE     |
|     TIME     |     TIME     |
|   DATETIME   |  TIMESTAMP   |
|  TIMESTAMP   |  TIMESTAMP   |
|     YEAR     |   SMALLINT   |
|     BOOL     |   BOOLEAN    |
|     JSON     |    STRING    |
|     ENUM     |    STRING    |
|     SET      |    STRING    |

## 高级配置

| Configuration                               | Default Value                                                                  | Description                                                                                                                                                                                                                                                                                          |
|:--------------------------------------------|:-------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| tidb.database.url                           | -                                                                              | 你需要用 jdbc url 的格式来填写你的 TiDB server 的地址：`jdbc:mysql://host:port/database` 或者 `jdbc:tidb://host:port/database`. 如果你有多个 TiDB server，我们推荐填写后一种格式以使用 TiDB jdbc driver, TiDB jdbc driver 是 MySQL jdbc driver 的一个轻量级的包装，它会自动发现所有 TiDB server 的地址，并做负载均衡，负载均衡策略默认为随机。                                        |
| tidb.username                               | -                                                                              | 用户名。                                                                                                                                                                                                                                                                                                 |
| tidb.password                               | null                                                                           | 密码。                                                                                                                                                                                                                                                                                                  |
| tidb.jdbc.connection-provider-impl          | io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.BasicJdbcConnectionProvider | JDBC 连接提供方式: 设置 'io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.HikariDataSourceJdbcConnectionProvider', TiBigData 将会使用连接池 [HikariCP](https://github.com/brettwooldridge/HikariCP) 提供连接; 设置 'io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.BasicJdbcConnectionProvider', 将会直接新建连接，而不会用到连接池。 |
| tidb.maximum.pool.size                      | 10                                                                             | 连接池大小。                                                                                                                                                                                                                                                                                               |
| tidb.minimum.idle.size                      | 10                                                                             | 最小存活连接数。                                                                                                                                                                                                                                                                                             |
| tidb.write_mode                             | append                                                                         | 在向 TiDB 写入数据时指定，可指定 `upsert` 或者 `append`. 如果指定为 `append`，在写入 TiDB 时遇到唯一键约束会报错；如果指定为 `upsert` ，在写入 TiDB 时遇到唯一键约束会替换原来的数据。                                                                                                                                                                             |
| tidb.replica-read                           | leader                                                                         | TiBigData 支持从指定的角色读取数据，你配置多个角色，比如 `tidb.replica-read=leader,follower`，这代表从 leader 和 follower 读取。                                                                                                                                                                                                     |
| tidb.replica-read.label                     | null                                                                           | TiBigData 支持从指定了 label 的 TiKV store 读取数据你可以这样配置：`label_x=value_x,label_y=value_y`                                                                                                                                                                                                                    |
| tidb.replica-read.whitelist                 | null                                                                           | TiKV store 的 ip 白名单列表，如果配置了，TiBigData 将会只从这些节点读取数据。                                                                                                                                                                                                                                                  |
| tidb.replica-read.blacklist                 | null                                                                           | TiKV store 的 ip 黑名单列表，如果配置了，TiBigData 将不会从这些节点读取数据。                                                                                                                                                                                                                                                  |
| sink.buffer-flush.max-rows                  | 100                                                                            | 写入数据的缓冲区大小，你可以设置为 0 以禁用缓冲区。                                                                                                                                                                                                                                                                          |
| sink.buffer-flush.interval                  | 1s                                                                             | The flush interval mills, over this time, asynchronous threads will flush data. Can be set to `'0'` to disable it. Note, `'sink.buffer-flush.max-rows'` can be set to `'0'` with the flush interval set allowing for complete async processing of buffered actions.                                  |
| tidb.sink.skip-check-update-columns         | false                                                                          | 请看 [Real-time wide table](../docs/real-time_wide_table_zh_CN.md)                                                                                                                                                                                                                                     |
| sink.max-retries                            | 3                                                                              | 写入数据失败的最大重试次数。                                                                                                                                                                                                                                                                                       |
| tidb.filter-push-down                       | false                                                                          | TiBigData 支持下推 Flink 的算子到 TiKV，设置为 true 以开启，仅对 Flink 1.13+ 支持。详细文档见[Flink 下推说明](../docs/flink_push_down_zh_CN.md)                                                                                                                                                                                    |
| tidb.snapshot_timestamp                     | null                                                                           | TiBigData 支持读取 TiDB 的快照数据，我们采用的格式为 `java.time.format.DateTimeFormatter#ISO_ZONED_DATE_TIME`.  比如 `2021-01-01T14:00:00+08:00`                                                                                                                                                                         |
| tidb.dns.search                             | null                                                                           | TiBigData 支持在节点的域名上添加后缀来支持复杂的网络情况，比如跨数据中心的 k8s 集群。                                                                                                                                                                                                                                                   |
| tidb.catalog.load-mode                      | eager                                                                          | TiDB Catalog 在调用 open 方法时，是否立即与 TiDB 建立连接。设置为 `eager` 将会立即建立连接， 设置为 `lazy` 将会在真正需要的时候再建立连接。                                                                                                                                                                                                          |
| tidb.sink.impl                              | jdbc                                                                           | 可指定 `jdbc` 或者 `tikv`. 如果设置为 `tikv`， flink connector 写数据时会 bypass TiDB server，直接写入 TiKV。                                                                                                                                                                                                              |
| tikv.sink.transaction                       | minibatch                                                                      | 只有在 sink 选项设置为 `tikv` 时才生效。可指定为 `minibatch` 或者 `global`. `global` 只能使用于有界数据流，所有数据会在一次事务内提交。 如果写冲突发生十分频繁，可以设置为 `minibatch`，数据会被拆分为多个事务提交。                                                                                                                                                             |
| tikv.sink.buffer-size                       | 1000                                                                           | 只有在 sink 选项设置为 `tikv` 时才生效。刷新缓存数据的阈值行数。注意，当 transaction 设置为 `minibatch` 时，每一次 flush 都是一次事务提交。                                                                                                                                                                                                        |
| tikv.sink.row-id-allocator.step             | 30000                                                                          | 只有在 sink 选项设置为 `tikv` 时才生效。每次 allocator 申请的 row-id 范围长度。                                                                                                                                                                                                                                             |
| tikv.sink.ignore-autoincrement-column-value | false                                                                          | 只有在 sink 选项设置为 `tikv` 时才生效。如果设置为 `true`，对于 autoincrement 列，会自动生成以替换原有的值。如果设置为 `false`，autoincrement 列的值不能为 null。                                                                                                                                                                                     |
| tikv.sink.deduplicate                       | false                                                                          | 只有在 sink 选项设置为 `tikv` 时才生效。如果设置为 `true`，重复的行数据将会进行去重。如果设置为 `false`，需要确保没有重复的行数据，不然程序会抛出异常。                                                                                                                                                                                                           |
| tidb.cluster-tls-enable                     | false                                                                          | 在 TiBigData 和 TiKV 之间是否开启 TLS，默认为 `false`。                                                                                                                                                                                                                                                           |
| tidb.cluster-tls-ca                         | _                                                                              | 信任证书路径，用于验证远端服务器。该文件为 PEM 格式的 X.509 证书。示例：`/home/TiBigData/.ci/config/cert/pem/root.pem`。                                                                                                                                                                                                            |
| tidb.cluster-tls-key                        | _                                                                              | 本节点证书路径，用于远端服务验证本节点。该证书为 PEM 格式的 X.509 证书。示例：`/home/TiBigData/.ci/config/cert/pem/client.pem`。                                                                                                                                                                                                       |
| tidb.cluster-tls-cert                       | _                                                                              | 本节点证书密钥路径，该密钥为 PKCS#8 加密的 PEM 格式密钥。示例：`/home/TiBigData/.ci/config/cert/pem/client-pkcs8.key`。                                                                                                                                                                                                        |
| tidb.cluster-jks-enable                     | false                                                                          | 开启 TLS 后，是否使用 JKS 密钥库。默认为 `false`。                                                                                                                                                                                                                                                                   |
| tidb.cluster-jks-key-path                   | _                                                                              | JKS 密钥库路径，用于远端服务验证本节点。该密钥加密方式 PKCS#12。示例：`/home/TiBigData/.ci/config/cert/jks/client-keystore.p12`。                                                                                                                                                                                                  |
| tidb.cluster-jks-key-password               | _                                                                              | JKS 密钥库的密码。                                                                                                                                                                                                                                                                                          |
| tidb.cluster-jks-trust-path                 | _                                                                              | JKS 信任证书库路径，用于验证远端服务器。示例：`/home/TiBigData/.ci/config/cert/jks/server-cert-store`。                                                                                                                                                                                                                    |
| tidb.cluster-jks-trust-password             | _                                                                              | JKS 信任证书库密码。                                                                                                                                                                                                                                                                                         |
| tidb.telemetry.enable                       | true                                                                           | TiBigData 默认开启遥测数据收集，可以主动设置为`false`来关闭遥测。                                                                                                                                                                                                                                                            |

### TLS 配置项说明

TiBigData 支持与 TiDB 集群链接时开启 TLS。如果想要完全启用 TLS，需要分别开启 JDBC 和 TiKV-client 的 TLS。

#### JDBC TLS

在 TiBigData 中开启 JDBC TLS，只需要在 `tidb.database.url` 配置中加上 TLS 配置。

```
# 此配置项表示 JDBC 链接启用 TLS 加密传输，但不做客户端和服务端的验证
&useSSL=true&requireSSL=true&verifyServerCertificate=false
```

更多 JDBC TLS 配置参数可参考 [MySQL Connector Security](https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-connp-props-security.html)。

JDBC 连接器开启 TLS 可参考 [Connecting Securely Using SSL](https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-using-ssl.html)。

TiDB 集群开启 TLS 可参考[为 TiDB 客户端服务端间通信开启加密传输](https://docs.pingcap.com/zh/tidb/stable/enable-tls-between-clients-and-servers)。

#### TiKV-Client TLS

TiKV-Client 是 TiBigData 用于链接 TiKV 集群的客户端，开启 TiKV-Client TLS，需要在配置中指定 `tidb.cluster-tls-enable=true`。

目前支持 TiKV-Client 两种指定证书形式：

1. 配置 PEM 格式证书。需配置以下三项。
   - tidb.cluster-tls-ca
   - tidb.cluster-tls-key
   - tidb.cluster-tls-cert
2. 配置 JKS 密钥库证书。需配置以下五项。
   - tidb.cluster-jks-enable 
   - tidb.cluster-jks-key-path
   - tidb.cluster-jks-key-password
   - tidb.cluster-jks-trust-path
   - tidb.cluster-jks-trust-password

启用 TiKV-Client TLS，需要预先开启 TiDB 集群内部组件 TLS，具体可参考[为 TiDB 组件间通信开启加密传输](https://docs.pingcap.com/zh/tidb/stable/enable-tls-between-components)。

## TableFactory（废弃）

注意：TableFactory 已被废弃，只在 Flink-1.13（包括）版本之前可用，请使用 catalog。 

TiBigData 也实现了 Flink TableFactory 相关的 API，不过我们并不推荐你使用它，会引入数据类型转换和列对齐的相关难题，会增加使用成本。我们将会在 Flink 1.14 **不再支持**，所以本小节只做简单介绍。

你可以使用如下 SQL 在 Flink 中创建 TiDB 的映射表并查询。

```sql
CREATE TABLE `people`(
  `id` INT,
  `name` STRING
) WITH (
  'connector' = 'tidb',
  'tidb.database.url' = 'jdbc:mysql://localhost:4000/',
  'tidb.username' = 'root',
  'tidb.password' = '',
  'tidb.database.name' = 'test',
  'tidb.table.name' = 'people'
);
SELECT * FROM people;
```

## 遥测说明

目前，TiBigData 中的 flink-tidb-connector（仅包括 flink-tidb-connector-1.14 和 flink-tidb-connector-1.13 版本） 会默认收集使用情况信息，并将这些信息分享给 PingCAP。
用户可以主动通过配置 `tidb.telemetry.enable = false` 来关闭遥测。

当 TiBigData 遥测功能开启时，TiBigData 会在初始化 `catalog` 时向 PingCAP 发送使用情况信息，包括但不限于：
- 随机生成的标示符
- 操作系统和硬件信息
- 部分 TiBigData 配置信息。

你可以通过日志查看遥测收集的具体数据。

```
2022-05-13 18:20:55,021 [INFO] [ForkJoinPool.commonPool-worker-1] io.tidb.bigdata.telemetry.Telemetry: Telemetry report: {"track_id":"4cec54a944bce9663f19115557c86884","time":"2022-05-13 18:20:54","subName":"flink-1.14","hardware":{"memory":"Available: 931.7 MiB/15.4 GiB","os":"Ubuntu","disks":[{"size":"512110190592","name":"/dev/nvme0n1"}],"cpu":{"logicalCores":"8","model":"11th Gen Intel(R) Core(TM) i7-1160G7 @ 1.20GHz","physicalCores":"4"},"version":"20.04.4 LTS (Focal Fossa) build 5.13.0-41-generic"},"instance":{"TiDBVersion":"v6.0.0","TiBigDataFlinkVersion":"0.0.5-SNAPSHOT","FlinkVersion":"1.14.0"},"content":{"tidb.write_mode":"append","tidb.catalog.load-mode":"eager","tikv.sink.deduplicate":"false","tidb.replica-read":"leader","tikv.sink.buffer-size":"1000","tidb.filter-push-down":"false","sink.buffer-flush.interval":"1s","tidb.sink.impl":"JDBC","tikv.sink.row-id-allocator.step":"30000","sink.buffer-flush.max-rows":"100","tikv.sink.ignore-autoincrement-column-value":"false","tikv.sink.transaction":"minibatch"}}
```

遥测数据字段说明如下表格所示。

| Field name                                            | Description             |
|-------------------------------------------------------|-------------------------|
| trackId                                               | 随机生成的标示符                |
| time                                                  | 遥测时间戳                   |
| subName                                               | 应用名称                    |
| hardware.os                                           | 操作系统                    |
| hardware.version                                      | 操作系统版本                  |
| hardware.cpu.model                                    | CPU 型号                  |
| hardware.cpu.logicalCores                             | CPU 逻辑核心数               |
| hardware.cpu.physicalCores                            | CPU 物理核心数               |
| hardware.disks.name                                   | 磁盘名称                    |
| hardware.disks.size                                   | 磁盘容量                    |
| hardware.memory                                       | 内存容量                    |
| instance.TiDBVersion                                  | TiDB 版本                 |
| instance.TiBigDataFlinkVersion                        | flink-tidb-connector 版本 |
| instance.FlinkVersion                                 | Flink 版本                |
| content.{tidb.write_mode}                             | flink-tidb-connector 配置 |
| content.{tidb.catalog.load-mode}                      | flink-tidb-connector 配置 |
| content.{tikv.sink.deduplicate}                       | flink-tidb-connector 配置 |
| content.{tidb.replica-read}                           | flink-tidb-connector 配置 |
| content.{tikv.sink.buffer-size}                       | flink-tidb-connector 配置 |
| content.{tidb.filter-push-down}                       | flink-tidb-connector 配置 |
| content.{sink.buffer-flush.interval}                  | flink-tidb-connector 配置 |
| content.{tidb.sink.impl}                              | flink-tidb-connector 配置 |
| content.{tikv.sink.row-id-allocator.step}             | flink-tidb-connector 配置 |
| content.{sink.buffer-flush.max-rows}                  | flink-tidb-connector 配置 |
| content.{tikv.sink.ignore-autoincrement-column-value} | flink-tidb-connector 配置 |
| content.{tikv.sink.transaction}                       | flink-tidb-connector 配置 |


## 常见问题

### TiBigData 会占用 TiDB 的资源吗

TiBigData 只会占用 Flink 资源，不会占用 TiDB 的资源，但是在读写 TiDB 数据的时候，会给 TiDB 带来一定的压力，推荐读取使用 Follower Read 的方式，这样不会影响到 Leader 节点。

### Flink 的配置应该如何设置

生产环境我们推荐一个 Flink 的 Slot 占用 4G 1Core 的资源。

### 我该如何设置并发度来控制任务运行的时长

TiBigData 读取一个 Region 的时间大约在 6 到 15 秒，我们用变量 `time_per_region` 表示，表的 Region 总数我们用 `region_count` 表示，Flink 任务的并行度我们用 `parallelism` 表示，则任务运行时间的计算公式如下：

```
job_time = max(time_per_region, (region_count x time_per_region) / parallelism)
```

请注意，**并行度不要超过表的 Region 数，否则会造成资源浪费**。一般来说，1T 大小的 TiDB 表，在 20 并发的情况下，读取全量数据需要花费 1 小时左右（时间根据服务器配置的不同可能会上下波动）。以上公式仅限读取数据的任务计算，写入任务跟 TiDB 的负载以及表的索引相关，这里不做预估。