# Hive-TiDB-Storage-Handler

## 1 Environment

| Component | Version         |
|-----------|-----------------|
| JDK       | 8               |
| Maven     | 3.6+            |
| Hive      | 1.x / 2.x / 3.x |

Hive Version to TiBigData Module：

| Hive Version  | TiBigData Module |
|---------------|------------------|
| 1.0.0 - 1.2.2 | hive-2.2.0       |
| 2.0.0 - 2.2.0 | hive-2.2.0       |
| 2.3.0 - 2.3.6 | hive-2.3.6       |
| 3.0.0 - 3.1.2 | hive-3.1.2       |

## 2 Compile Hive Storage Handler

Please refer to the following steps, as the comments say, you need to compile the TiKV java client before you compile TiBigData, because TiBigData preempts some new features that are not released in the TiKV java client.

```bash
# clone project
git clone git@github.com:tidb-incubator/TiBigData.git
cd TiBigData
# compile TiKV java client
./.ci/build-client-java.sh
# compile Hive Storage Handler，using hive-2.2.0 as an example
mvn clean package -DskipTests -am -pl hive/hive-2.2.0
```

The following parameters are available for compiling:

| parameter                     | default | description                                                |
|-------------------------------|---------|------------------------------------------------------------|
| -Dmysql.driver.scope          | test    | Whether the dependency `mysql-jdbc-driver` is included     |

## 3 Install Hive Storage Handler

Both of the following are available.

### 3.1 Using ADD JAR syntax

```bash
# upload TiBigData jar to HDFS
hadoop fs -put $TIBIGDATA_HOME/hive/hive-2.2.0/target/hive-tidb-storage-handler-2.2.0-0.0.5-SNAPSHOT.jar /tmp
# run add jar sql in hive shell or beeline
ADD JAR hdfs:///tmp/hive-tidb-storage-handler-2.2.0-0.0.5-SNAPSHOT.jar;
```

### 3.2 Copy lib to hive

```bash
cp $TIBIGDATA_HOME/hive/hive-2.2.0/target/hive-tidb-storage-handler-2.2.0-0.0.5-SNAPSHOT.jar $HIVE_HOME/lib/
```

## 4 Reading TiDB by Hive

Using mysql client to create a table in TiDB:

```bash
# connect to TiDB
mysql --host 127.0.0.1 --port 4000 -uroot --database test
```

Insert data to TiDB:

```sql
CREATE TABLE `people`(
  `id` int,
  `name` varchar(16)
);

INSERT INTO `people`(`id`,`name`) VALUES (1,'zs');
```

Create the TiDB mapping table in Hive, where the columns are arbitrary and TiBigData will automatically correct the columns.

```sql
CREATE TABLE `people`(
 `fake_column` int
)
STORED BY
  'io.tidb.bigdata.hive.TiDBStorageHandler'
TBLPROPERTIES (
  'tidb.database.url'='jdbc:mysql://localhost:4000/',
  'tidb.database.name'='test',
  'tidb.table.name'='people',
  'tidb.username'='root',
  'tidb.password'=''
 );
```

Using hive sql to query TiDB schema:

```sql
SHOW CREATE TABLE `people`;
```

We can see that the columns of the table have been automatically corrected:

```sql
hive (default)> SHOW CREATE TABLE `people`;
OK
createtab_stmt
CREATE TABLE `people`(
  `id` int COMMENT 'from deserializer',
  `name` string COMMENT 'from deserializer')
ROW FORMAT SERDE
  'io.tidb.bigdata.hive.TiDBSerde'
STORED BY
  'io.tidb.bigdata.hive.TiDBStorageHandler'
WITH SERDEPROPERTIES (
  'serialization.format'='1')
LOCATION
  'hdfs://XXX:8020/user/hive/warehouse/people'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='{\"BASIC_STATS\":\"true\"}',
  'numFiles'='0',
  'numRows'='0',
  'rawDataSize'='0',
  'tidb.database.name'='test',
  'tidb.database.url'='jdbc:mysql://localhost:4000/',
  'tidb.password'='',
  'tidb.table.name'='people',
  'tidb.username'='root',
  'totalSize'='0',
  'transient_lastDdlTime'='1642779466')
Time taken: 2.276 seconds, Fetched: 23 row(s)
hive (default)>
```

Query TiDB in Hive:

```sql
SELECT * FROM `people`;
```

output:

```sql
hive (default)> SELECT * FROM `people`;
OK
people.id	people.name
1	zs
Time taken: 4.979 seconds, Fetched: 1 row(s)
hive (default)>
```

## 5 DataTypes

|     TiDB     |     Hive     |
|:------------:|:------------:|
|   TINYINT    |     INT      |
|   SMALLINT   |     INT      |
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
|     TIME     |    STRING    |
|   DATETIME   |  TIMESTAMP   |
|  TIMESTAMP   |  TIMESTAMP   |
|     YEAR     |     INT      |
|     BOOL     |   BOOLEAN    |
|     JSON     |    STRING    |
|     ENUM     |    STRING    |
|     SET      |    STRING    |

## 6 Configuration

| Configuration                      | Default Value                                                                  | Description                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|:-----------------------------------|:-------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| tidb.database.url                  | -                                                                              | You should provide your own TiDB server address with a jdbc url format:  `jdbc:mysql://host:port/database` or `jdbc:tidb://host:port/database`. If you have multiple TiDB server addresses and the amount of data to be inserted is huge, it would be better to use TiDB jdbc driver rather then MySQL jdbc driver. TiDB driver is a load-balancing driver, it will query all TiDB server addresses and pick one  randomly when establishing connections. |
| tidb.username                      | -                                                                              | JDBC username.                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| tidb.password                      | null                                                                           | JDBC password.                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| tidb.jdbc.connection-provider-impl | io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.BasicJdbcConnectionProvider | JDBC connection provider implements: set 'io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.HikariDataSourceJdbcConnectionProvider', TiBigData will use JDBC connection pool implemented by [HikariCP](https://github.com/brettwooldridge/HikariCP) to provider connection; set 'io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.BasicJdbcConnectionProvider', connection pool will not be used.                                                      |
| tidb.maximum.pool.size             | 10                                                                             | Connection pool size.                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| tidb.minimum.idle.size             | 10                                                                             | The minimum number of idle connections that HikariCP tries to maintain in the pool.                                                                                                                                                                                                                                                                                                                                                                       |
| tidb.replica-read                  | leader                                                                         | Read data from specified role. The optional roles are leader, follower and learner. You can also specify multiple roles, and we will pick the roles you specify in order.                                                                                                                                                                                                                                                                                 |
| tidb.replica-read.label            | null                                                                           | Only select TiKV store match specified labels. Format: label_x=value_x,label_y=value_y                                                                                                                                                                                                                                                                                                                                                                    |
| tidb.replica-read.whitelist        | null                                                                           | Only select TiKV store with given ip addresses.                                                                                                                                                                                                                                                                                                                                                                                                           |
| tidb.replica-read.blacklist        | null                                                                           | Do not select TiKV store with given ip addresses.                                                                                                                                                                                                                                                                                                                                                                                                         |
| tidb.snapshot_timestamp            | null                                                                           | It is available for TiDB connector to read snapshot. You could set it by `SET SESSION tidb.snapshot_timestamp='2021-01-01T14:00:00+08:00'` and unset it by `SET SESSION tidb.snapshot_timestamp=''` within a session. The format of timestamp may refer to `java.time.format.DateTimeFormatter#ISO_ZONED_DATE_TIME`.                                                                                                                                      |
| tidb.dns.search                    | null                                                                           | Append dns search suffix to host names. It's especially necessary to map K8S cluster local name to FQDN.                                                                                                                                                                                                                                                                                                                                                  |


