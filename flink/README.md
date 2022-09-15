# Flink-TiDB-Connector

## Table of Contents

* [1 Environment](#1-environment)
* [2 Compile Flink Connector](#2-compile-flink-connector)
* [3 Deploy Flink](#3-deploy-flink)
  * [Download Flink](#download-flink)
  * [Install TiBigData and start Flink cluster](#install-tibigdata-and-start-flink-cluster)
* [Read &amp; Write](#read--write)
* [Real-time wide table](#real-time-wide-table)
* [Supports and Limitations](#supports-and-limitations)
* [DataTypes supported](#datatypes-supported)
* [Configuration](#configuration)
* [TableFactory(deprecated)](#tablefactorydeprecated)

## 1 Environment

| Component | Version                           |
|-----------|-----------------------------------|
| JDK       | 8                                 |
| Maven     | 3.6+                              |
| Flink     | 1.11.x / 1.12.x / 1.13.x / 1.14.x |

## 2 Compile Flink Connector

```bash
# clone
git clone git@github.com:tidb-incubator/TiBigData.git
cd TiBigData

# compile flink connector, using Flink-1.14.3 as an example
mvn clean package -DskipTests -am -pl flink/flink-1.14 -Ddep.flink.version=1.14.3 -Dmysql.driver.scope=compile -Dflink.jdbc.connector.scope=compile -Dflink.kafka.connector.scope=compile
```

The following parameters are available for compiling:

| parameter                     | default | description                                                |
|-------------------------------|---------|------------------------------------------------------------|
| -Ddep.flink.version           | 1.14.0  | The version of Flink                                       |
| -Dmysql.driver.scope          | test    | Whether the dependency `mysql-jdbc-driver` is included     |
| -Dflink.jdbc.connector.scope  | test    | Whether the dependency `flink-jdbc-connector` is included  |
| -Dflink.kafka.connector.scope | test    | Whether the dependency `flink-kafka-connector` is included |


## 3 Deploy Flink

We only present the standalone cluster for testing. If you want to use Flink in production environment, please refer to the [Flink official documentation](https://flink.apache.org/).

We recommend using Flink 1.14, the following steps are based on Flink 1.14 for example, other versions of Flink installation steps are more or less the same.

### Download Flink

Please go to [Flink Download Page](https://flink.apache.org/downloads.html) to download the corresponding version of the installation package. Only the latest version of Flink is kept on this page, the historical version can be downloaded here: [Flink Historical Versions](http://archive.apache.org/dist/flink).

### Install TiBigData and start Flink cluster

```bash
tar -zxf flink-1.14.3-bin-scala_2.11.tgz
cd flink-1.14.3
cp ${TIBIGDATA_HOME}/flink/flink-1.14/target/flink-tidb-connector-1.14-${TIBIGDATA_VERSION}.jar lib
bin/start-cluster.sh
```

You should be able to navigate to the web UI at http://localhost:8081 to view the Flink dashboard and see that the cluster is up and running.

## Read & Write

TiBigData supports **Batch Mode** and **Unified Batch & Streaming Mode**. The subsequent content of this article only introduces reading TiDB in **Batch Mode**，For **Unified Batch & Streaming Mode**, please refer to [TiBigData Unified Batch & Streaming](./README_unified_batch_streaming.md).

After the Flink cluster is deployed, you could use Flink sql-client to read and write data from TiDB.

```bash
 # start flink sql client
 bin/sql-client.sh
```

Create a catalog in flink sql client:

```sql
CREATE CATALOG `tidb`
WITH (
    'type' = 'tidb',
    'tidb.database.url' = 'jdbc:mysql://localhost:4000/test',
    'tidb.username' = 'root',
    'tidb.password' = ''
);
```
Using mysql client to create a table in TiDB:

```bash
# connect to TiDB
mysql --host 127.0.0.1 --port 4000 -uroot --database test
```

```sql
CREATE TABLE `people`(
  `id` int,
  `name` varchar(16)
);
```

Using flink sql client to query TiDB schema:

```sql
DESC `tidb`.`test`.`people`;
```

The output can be found in console, like:
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

Using flink sql client to insert and select data from TiDB：

```sql
SET sql-client.execution.result-mode=tableau;
INSERT INTO `tidb`.`test`.`people`(`id`,`name`) VALUES(1,'zs');
SELECT * FROM `tidb`.`test`.`people`;
```

output：

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

## Real-time wide table

See [Real-time wide table](../docs/real-time_wide_table.md)

## Partition table supports

See [flink_partition_table](../docs/flink_partition_table.md)

## Supports and Limitations

TiBigDate/Flink supports writing data to and reading from clustered index tables, which is a new feature in TiDB-5.0.0.

TiBigDate/Flink does not support the following features: 
- When [new collation](https://docs.pingcap.com/tidb/stable/tidb-configuration-file#new_collations_enabled_on_first_bootstrap) is enabled in TiDB, the TiKV sink cannot be used to read or write data。

## DataTypes supported

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

## Configuration

| Configuration                               | Default Value                                                                  | Description                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|:--------------------------------------------|:-------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| tidb.database.url                           | -                                                                              | You should provide your own TiDB server address with a jdbc url format:  `jdbc:mysql://host:port/database` or `jdbc:tidb://host:port/database`. If you have multiple TiDB server addresses and the amount of data to be inserted is huge, it would be better to use TiDB jdbc driver rather then MySQL jdbc driver. TiDB driver is a load-balancing driver, it will query all TiDB server addresses and pick one  randomly when establishing connections. |
| tidb.username                               | -                                                                              | JDBC username.                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| tidb.password                               | null                                                                           | JDBC password.                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| tidb.jdbc.connection-provider-impl          | io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.BasicJdbcConnectionProvider | JDBC connection provider implements: set 'io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.HikariDataSourceJdbcConnectionProvider', TiBigData will use JDBC connection pool implemented by [HikariCP](https://github.com/brettwooldridge/HikariCP) to provider connection; set 'io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.BasicJdbcConnectionProvider', connection pool will not be used.                                                      |
| tidb.maximum.pool.size                      | 10                                                                             | Connection pool size.                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| tidb.minimum.idle.size                      | 10                                                                             | The minimum number of idle connections that HikariCP tries to maintain in the pool.                                                                                                                                                                                                                                                                                                                                                                       |
| tidb.write_mode                             | append                                                                         | TiDB sink write mode: `upsert` or `append`.                                                                                                                                                                                                                                                                                                                                                                                                               |
| tidb.replica-read                           | leader                                                                         | Read data from specified role. The optional roles are leader, follower and learner. You can also specify multiple roles, and we will pick the roles you specify in order.                                                                                                                                                                                                                                                                                 |
| tidb.replica-read.label                     | null                                                                           | Only select TiKV store match specified labels. Format: label_x=value_x,label_y=value_y                                                                                                                                                                                                                                                                                                                                                                    |
| tidb.replica-read.whitelist                 | null                                                                           | Only select TiKV store with given ip addresses.                                                                                                                                                                                                                                                                                                                                                                                                           |
| tidb.replica-read.blacklist                 | null                                                                           | Do not select TiKV store with given ip addresses.                                                                                                                                                                                                                                                                                                                                                                                                         |
| tidb.database.name                          | null                                                                           | Database name. It is required for table factory, no need for catalog.                                                                                                                                                                                                                                                                                                                                                                                     |
| tidb.table.name                             | null                                                                           | Table name. It is required for table factory, no need for catalog.                                                                                                                                                                                                                                                                                                                                                                                        |
| tidb.timestamp-format.${columnName}         | null                                                                           | For each column, you could specify timestamp format in two cases: 1. TiDB `timestamp` is mapped to Flink `string`; 2. TiDB `varchar` is mapped to Flink `timestamp`. Format of timestamp may refer to `java.time.format.DateTimeFormatter`, like `yyyy-MM-dd HH:mm:ss.SSS`. It is optional for table factory, no need for catalog.                                                                                                                        |
| sink.buffer-flush.max-rows                  | 100                                                                            | The max size of buffered records before flush. Can be set to zero to disable it.                                                                                                                                                                                                                                                                                                                                                                          |
| sink.buffer-flush.interval                  | 1s                                                                             | The flush interval mills, over this time, asynchronous threads will flush data. Can be set to `'0'` to disable it. Note, `'sink.buffer-flush.max-rows'` can be set to `'0'` with the flush interval set allowing for complete async processing of buffered actions.                                                                                                                                                                                       |
| tidb.catalog.load-mode                      | eager                                                                          | TiDB catalog load mode: `eager` or `lazy`. If you set this configuration to lazy, catalog would establish a connection to tidb when the data is actually queried rather than when catalog is opened.                                                                                                                                                                                                                                                      |
| tidb.sink.skip-check-update-columns         | false                                                                          | See [Real-time wide table](../docs/real-time_wide_table.md)                                                                                                                                                                                                                                                                                                                                                                                               |
| sink.max-retries                            | 3                                                                              | The max retry times if writing records to database failed.                                                                                                                                                                                                                                                                                                                                                                                                |
| tidb.filter-push-down                       | false                                                                          | Support filter push down. It is only available for version 1.13+. More details see [Flink Filter Push Down Description](../docs/flink_push_down.md)                                                                                                                                                                                                                                                                                                       |
| tidb.snapshot_timestamp                     | null                                                                           | It is available for TiDB connector to read snapshot. You could configure it in table properties. The format of timestamp may refer to `java.time.format.DateTimeFormatter#ISO_ZONED_DATE_TIME`.                                                                                                                                                                                                                                                           |
| tidb.dns.search                             | null                                                                           | Append dns search suffix to host names. It's especially necessary to map K8S cluster local name to FQDN.                                                                                                                                                                                                                                                                                                                                                  |
| tidb.catalog.load-mode                      | eager                                                                          | TiDB catalog load mode: `eager` or `lazy`. If you set this configuration to lazy, catalog would establish a connection to tidb when the data is actually queried rather than when catalog is opened.                                                                                                                                                                                                                                                      |
| tidb.sink.impl                              | jdbc                                                                           | The value can be `jdbc` or `tikv`. If you set this configuration to `tikv`, flink will write data bypass TiDB. It is only available for version 1.14+.                                                                                                                                                                                                                                                                                                    |
| tikv.sink.transaction                       | minibatch                                                                      | Only work when sink option is `tikv`. The value can be `minibatch` or `global`.`global` only works with bounded stream, all data will be submit in one transaction. When writing conflicts happen frequently, you can `minibatch`, it will split data to many transactions.                                                                                                                                                                               |
| tikv.sink.buffer-size                       | 1000                                                                           | Only work when sink option is `tikv`. The max size of buffered records before flush. Notice: On mode `minibatch`, each flush will be executed in one transaction.                                                                                                                                                                                                                                                                                         |
| tikv.sink.row-id-allocator.step             | 30000                                                                          | Only work when sink option is `tikv`. The size of row-ids each time allocator query for.                                                                                                                                                                                                                                                                                                                                                                  |
| tikv.sink.ignore-autoincrement-column-value | false                                                                          | Only work when sink option is `tikv`. If value is `true`, for autoincrement column, we will generate value instead of the the actual value. And if `false`, the value of autoincrement column can not be null.                                                                                                                                                                                                                                            |
| tikv.sink.ignore-autorandom-column-value    | false                                                                          | Only work when sink option is `tikv`. If value is `true`, for autorandom column, we will generate value instead of the the actual value. And if `false`, the value of autorandom column can not be null.                                                                                                                                                                                                                                                  |
| tikv.sink.deduplicate                       | false                                                                          | Only work when sink option is `tikv`. If value is `true`, duplicate row will be de-duplicated. If `false`, you should make sure each row is unique otherwise exception will be thrown.                                                                                                                                                                                                                                                                    |
| tidb.cluster-tls-enable                     | false                                                                          | Whether to enable TLS between TiBigData and TiKV.                                                                                                                                                                                                                                                                                                                                                                                                         |
| tidb.cluster-tls-ca                         | _                                                                              | Trusted certificates for verifying the remote endpoint's certificate, e.g. `/home/TiBigData/.ci/config/cert/pem/root.pem`. The file should contain an X.509 certificate collection in PEM format.                                                                                                                                                                                                                                                         |
| tidb.cluster-tls-key                        | _                                                                              | A PKCS#8 private key file in PEM format. e.g. `/home/TiBigData/.ci/config/cert/pem/client.pem`.                                                                                                                                                                                                                                                                                                                                                           |
| tidb.cluster-tls-cert                       | _                                                                              | An X.509 certificate chain file in PEM format, e.g. `/home/TiBigData/.ci/config/cert/pem/client-pkcs8.key`.                                                                                                                                                                                                                                                                                                                                               |
| tidb.cluster-jks-enable                     | false                                                                          | Whether to use JKS keystore.                                                                                                                                                                                                                                                                                                                                                                                                                              |
| tidb.cluster-jks-key-path                   | _                                                                              | The path of the JKS key store, which is used by the remote service to authenticate this node. The key encryption is PKCS#12. e.g. `/home/TiBigData/.ci/config/cert/jks/client-keystore.p12`.                                                                                                                                                                                                                                                              |
| tidb.cluster-jks-key-password               | _                                                                              | The key of JKS key store.                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| tidb.cluster-jks-trust-path                 | _                                                                              | The path of the JKS trust certificates store, which is used to authenticate remote service. e.g. `/home/TiBigData/.ci/config/cert/jks/server-cert-store`.                                                                                                                                                                                                                                                                                                 |
| tidb.cluster-jks-trust-password             | _                                                                              | The key of JKS trust certificates store.                                                                                                                                                                                                                                                                                                                                                                                                                  |
| tidb.telemetry.enable                       | true                                                                           | Whether to enable telemetry collection in TiBigData. Telemetry can be off by setting `false`.                                                                                                                                                                                                                                                                                                                                                             |

### TLS NOTE

TiBigData supports enabling TLS when connecting with a TiDB cluster. If you want to fully enable TLS, you need to enable TLS for JDBC and TiKV-client respectively.

#### JDBC TLS

To enable JDBC TLS in TiBigData, just add the TLS configuration to the `tidb.database.url` configuration.

```
&useSSL=true&requireSSL=true&verifyServerCertificate=false
```

More JDBC TLS configurations can be found [here](https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-connp-props-security.html).

For how to open JDBC TLS, see [here](https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-using-ssl.html).

For how to open TiDB TLS, see [here](https://docs.pingcap.com/zh/tidb/stable/enable-tls-between-clients-and-servers).

#### TiKV-Client TLS

TiKV-Client is the client used by TiBigData to link TiKV clusters. To enable TiKV-Client TLS, you need to specify `tidb.cluster-tls-enable=true` in the configuration.

Currently, TiKV-Client supports two specified certificate forms:

1. An X.509 certificate collection in PEM format needs three configurations.
  - tidb.cluster-tls-ca
  - tidb.cluster-tls-key
  - tidb.cluster-tls-cert
2. A JKS store with five configurations.
  - tidb.cluster-jks-enable
  - tidb.cluster-jks-key-path
  - tidb.cluster-jks-key-password
  - tidb.cluster-jks-trust-path
  - tidb.cluster-jks-trust-password

To enable TiKV-Client TLS, you need to enable TLS for the internal components of the TiDB cluster in advance. For details, please refer to [Enable TLS Between TiDB Components](https://docs.pingcap.com/zh/tidb/stable/enable-tls-between -components).

## TableFactory(deprecated)

Attention: TableFactory is deprecated, only support before Flink 1.13(included).

TiBigData also implements the Flink TableFactory API, but we don't recommend you to use it, it will introduce difficulties related to data type conversion and column alignment, which will increase the cost of using it. We stop supporting it in Flink-1.14, so this section is only a brief introduction.

You can use the following SQL to create a TiDB mapping table in Flink and query it.

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

## Telemetry

Currently, flink-tidb-connector in TiBigData (only flink-tidb-connector-1.14 and flink-tidb-connector-1.13 versions) will collect usage information by default and share this information with PingCAP.
Users can actively turn off telemetry by configuring `tidb.telemetry.enable = false`.

When TiBigData telemetry is enabled, TiBigData will send usage information to PingCAP when initializing `catalog`, including but not limited to:

- Randomly generated identifiers
- Operating system and hardware information
- Part of TiBigData configuration information.

Here is an example of telemetry information.

```
2022-05-13 18:20:55,021 [INFO] [ForkJoinPool.commonPool-worker-1] io.tidb.bigdata.telemetry.Telemetry: Telemetry report: {"track_id":"4cec54a944bce9663f19115557c86884","time":"2022-05-13 18:20:54","subName":"flink-1.14","hardware":{"memory":"Available: 931.7 MiB/15.4 GiB","os":"Ubuntu","disks":[{"size":"512110190592","name":"/dev/nvme0n1"}],"cpu":{"logicalCores":"8","model":"11th Gen Intel(R) Core(TM) i7-1160G7 @ 1.20GHz","physicalCores":"4"},"version":"20.04.4 LTS (Focal Fossa) build 5.13.0-41-generic"},"instance":{"TiDBVersion":"v6.0.0","TiBigDataFlinkVersion":"0.0.5-SNAPSHOT","FlinkVersion":"1.14.0"},"content":{"tidb.write_mode":"append","tidb.catalog.load-mode":"eager","tikv.sink.deduplicate":"false","tidb.replica-read":"leader","tikv.sink.buffer-size":"1000","tidb.filter-push-down":"false","sink.buffer-flush.interval":"1s","tidb.sink.impl":"JDBC","tikv.sink.row-id-allocator.step":"30000","sink.buffer-flush.max-rows":"100","tikv.sink.ignore-autoincrement-column-value":"false","tikv.sink.transaction":"minibatch"}}
```

An entry table of telemetry is shown here.

| Field name                                            | Description                         |
|-------------------------------------------------------|-------------------------------------|
| trackId                                               | ID of the telemetry                 |
| time                                                  | The time point of reporting         |
| subName                                               | application name                    |
| hardware.os                                           | Operating system name               |
| hardware.version                                      | Operating system version            |
| hardware.cpu.model                                    | CPU model                           |
| hardware.cpu.logicalCores                             | Number of CPU logical cores         |
| hardware.cpu.physicalCores                            | Number of CPU physical cores        |
| hardware.disks.name                                   | Disks name                          |
| hardware.disks.size                                   | Disks capacity                      |
| hardware.memory                                       | Memory capacity                     |
| instance.TiDBVersion                                  | TiDB Version                        |
| instance.TiBigDataFlinkVersion                        | flink-tidb-connector Version        |
| instance.FlinkVersion                                 | Flink Version                       |
| content.{tidb.write_mode}                             | flink-tidb-connector configuration  |
| content.{tidb.catalog.load-mode}                      | flink-tidb-connector configuration  |
| content.{tikv.sink.deduplicate}                       | flink-tidb-connector configuration  |
| content.{tidb.replica-read}                           | flink-tidb-connector configuration  |
| content.{tikv.sink.buffer-size}                       | flink-tidb-connector configuration  |
| content.{tidb.filter-push-down}                       | flink-tidb-connector configuration  |
| content.{sink.buffer-flush.interval}                  | flink-tidb-connector configuration  |
| content.{tidb.sink.impl}                              | flink-tidb-connector configuration  |
| content.{tikv.sink.row-id-allocator.step}             | flink-tidb-connector configuration  |
| content.{sink.buffer-flush.max-rows}                  | flink-tidb-connector configuration  |
| content.{tikv.sink.ignore-autoincrement-column-value} | flink-tidb-connector configuration  |
| content.{tikv.sink.transaction}                       | flink-tidb-connector configuration  |