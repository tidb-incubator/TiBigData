# PrestoSQL-TiDB-Connector

## 1 Environment

| Component | Version |
|-----------|---------|
| JDK       | 8       |
| Maven     | 3.6+    |
| PrestoSQL | 0.234.2 |

## 2 Compile PrestoSQL Connector

Please refer to the following steps, as the comments say, you need to compile the TiKV java client before you compile TiBigData, because TiBigData preempts some new features that are not released in the TiKV java client.

```bash
# clone project
git clone git@github.com:tidb-incubator/TiBigData.git
cd TiBigData
# compile TiKV java client
./.ci/build-client-java.sh
# compile presto connector
mvn clean package -DskipTests -am -pl prestosql -Dmysql.driver.scope=compile
# 解压 plugin
tar -zxf prestosql/target/prestosql-connector-0.0.5-SNAPSHOT-plugin.tar.gz -C prestosql/target
```

The following parameters are available for compiling:

| parameter            | default | description                                            |
|----------------------|---------|--------------------------------------------------------|
| -Dmysql.driver.scope | test    | Whether the dependency `mysql-jdbc-driver` is included |

## 3 Deploy PrestoSQL

We only present the standalone cluster for testing. If you want to use Presto in production environment, please refer to the [PrestoSQL official documentation](https://trino.io).

### 3.1 Download

Please go to [PrestoSQL Download Page](https://repo1.maven.org/maven2/io/prestosql/presto-server) to download the corresponding version of the installation package.

### 3.2 Install TiBigData

```bash
wget https://repo1.maven.org/maven2/io/prestosql/presto-server/350/presto-server-350.tar.gz
tar -zxf presto-server-0.234.2.tar.gz
cd presto-server-0.234.2
cp -r ${TIBIGDATA_HOME}/prestosql/target/prestosql-connector-0.0.5-SNAPSHOT/tidb plugin
```

### 3.3 Config PrestoSQL standalone cluster

Here we give a simple configuration to start a standalone PrestoSQL cluster.

```bash
cd $PRESTO_HOME
mkdir -p etc/catalog
```

The next step is to configure the PrestoSQL configuration files.

#### 3.3.1 Config config.properties

```bash
vim etc/config.properties
```

The content of `config.properties`：

```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=12345
query.max-memory=2GB
query.max-memory-per-node=2GB
query.max-total-memory-per-node=2GB
discovery-server.enabled=true
discovery.uri=http://localhost:12345
```

#### 3.3.2 Config jvm.properties
```bash
vim etc/jvm.config
```

The content of `jvm.config`：

```properties
-server
-Xmx4G
-XX:+UseConcMarkSweepGC
-XX:+ExplicitGCInvokesConcurrent
-XX:+CMSClassUnloadingEnabled
-XX:+AggressiveOpts
-XX:+HeapDumpOnOutOfMemoryError
-XX:OnOutOfMemoryError=kill -9 %p
-XX:ReservedCodeCacheSize=150M
```
#### 3.3.3 Config node.properties
```bash
vim etc/node.properties
```

The content of `node.properties`：

```properties
node.environment=test
node.id=1
node.data-dir=/tmp/prestosql/logs
```
#### 3.3.4 Config log.properties
```bash
vim etc/log.properties
```

The content of `log.properties`：

```properties
io.prestosql=INFO
```

#### 3.3.4 Config tidb connector
```bash
vim etc/catalog/tidb.properties
```

The content of `tidb.properties`：

```properties
# must be tidb
connector.name=tidb
tidb.database.url=jdbc:mysql://localhost:4000/test
tidb.username=root
tidb.password=
```

If you have multiple TiDB clusters, you can create multiple properties files, such as `tidb01.properties` and `tidb02.properties`, and just write a different connection string and password for each configuration file.

### 3.4 Start PrestoSQL cluster

```bash
# foreground
bin/launcher run
# background
bin/launcher start
```

### 3.5 Reading or Writing TiDB by PrestoSQL

```bash
# download prestosql client
curl -L https://repo1.maven.org/maven2/io/prestosql/presto-cli/350/presto-cli-350-executable.jar -o prestosql
chmod 777 prestosql
# connect to prestosql
./prestosql --server localhost:12345 --catalog tidb --schema test
```

Using mysql client to create a table in TiDB:

```bash
# connect to tidb
mysql --host 127.0.0.1 --port 4000 -uroot --database test
```

```sql
CREATE TABLE `people`(
  `id` int,
  `name` varchar(16)
);
```

Using prestosql client to query TiDB schema:

```sql
show create table people;
```

output:

```sql
presto:test> show create table people;
          Create Table
---------------------------------
 CREATE TABLE tidb.test.people (
    id integer,
    name varchar(16)
 )
 WITH (
    primary_key = '',
    unique_key = ''
 )
(1 row)

Query 20220105_143658_00002_a26k7, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```
Using prestosql client to insert and select data from TiDB：

```sql
INSERT INTO "test"."people"("id","name") VALUES(1,'zs');
SELECT * FROM "test"."people";
```

output:

```sql
presto:test> INSERT INTO "test"."people"("id","name") VALUES(1,'zs');
INSERT: 1 row

Query 20220105_143723_00003_a26k7, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]

presto:test> INSERT INTO "test"."people"("id","name") VALUES(1,'zs');
INSERT: 1 row

Query 20220105_143741_00004_a26k7, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]

presto:test> SELECT * FROM "test"."people";
 id | name
----+------
  1 | zs
(1 row)

Query 20220105_143748_00005_a26k7, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0:00 [1 rows, 0B] [2 rows/s, 0B/s]
```

## 5 DataTypes

|     TiDB     |  PrestoSQL   |
|:------------:|:------------:|
|   TINYINT    |   TINYINT    |
|   SMALLINT   |   SMALLINT   |
|  MEDIUMINT   |     INT      |
|     INT      |     INT      |
|    BIGINT    |    BIGINT    |
|     CHAR     |   VARCHAR    |
|   VARCHAR    |   VARCHAR    |
|   TINYTEXT   |   VARCHAR    |
|  MEDIUMTEXT  |   VARCHAR    |
|     TEXT     |   VARCHAR    |
|   LONGTEXT   |   VARCHAR    |
|    BINARY    |  VARBINARY   |
|  VARBINARY   |  VARBINARY   |
|   TINYBLOB   |  VARBINARY   |
|  MEDIUMBLOB  |  VARBINARY   |
|     BLOB     |  VARBINARY   |
|   LONGBLOB   |  VARBINARY   |
|    FLOAT     |     REAL     |
|    DOUBLE    |    DOUBLE    |
|   DECIMAL    |   DECIMAL    |
|     DATE     |     DATE     |
|   TIME(p)    |   TIME(p)    |
| DATETIME(p)  | TIMESTAMP(p) |
| TIMESTAMP(p) | TIMESTAMP(p) |
|     YEAR     |   SMALLINT   |
|     BOOL     |   TINYINT    |
|     JSON     |   VARCHAR    |
|     ENUM     |   VARCHAR    |
|     SET      |   VARCHAR    |

## 6 Configuration

| Configuration                      | Default Value                                                                  | Description                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|:-----------------------------------|:-------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| tidb.database.url                  | -                                                                              | You should provide your own TiDB server address with a jdbc url format:  `jdbc:mysql://host:port/database` or `jdbc:tidb://host:port/database`. If you have multiple TiDB server addresses and the amount of data to be inserted is huge, it would be better to use TiDB jdbc driver rather then MySQL jdbc driver. TiDB driver is a load-balancing driver, it will query all TiDB server addresses and pick one  randomly when establishing connections. |
| tidb.username                      | -                                                                              | JDBC username.                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| tidb.password                      | null                                                                           | JDBC password.                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| tidb.jdbc.connection-provider-impl | io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.BasicJdbcConnectionProvider | JDBC connection provider implements: set 'io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.HikariDataSourceJdbcConnectionProvider', TiBigData will use JDBC connection pool implemented by [HikariCP](https://github.com/brettwooldridge/HikariCP) to provider connection; set 'io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.BasicJdbcConnectionProvider', connection pool will not be used.                                                      |
| tidb.maximum.pool.size             | 10                                                                             | Connection pool size.                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| tidb.minimum.idle.size             | 10                                                                             | The minimum number of idle connections that HikariCP tries to maintain in the pool.                                                                                                                                                                                                                                                                                                                                                                       |
| tidb.write_mode                    | append                                                                         | TiDB sink write mode: `upsert` or `append`. You could config it in `tidb.properties`, or set it by `SET SESSION tidb.write_mode='upsert'` within a session. TiDB primary key columns and unique key columns will be mapped as presto table properties `primary_key` and `unique_key`.                                                                                                                                                                     |
| tidb.replica-read                  | leader                                                                         | Read data from specified role. The optional roles are leader, follower and learner. You can also specify multiple roles, and we will pick the roles you specify in order.                                                                                                                                                                                                                                                                                 |
| tidb.replica-read.label            | null                                                                           | Only select TiKV store match specified labels. Format: label_x=value_x,label_y=value_y                                                                                                                                                                                                                                                                                                                                                                    |
| tidb.replica-read.whitelist        | null                                                                           | Only select TiKV store with given ip addresses.                                                                                                                                                                                                                                                                                                                                                                                                           |
| tidb.replica-read.blacklist        | null                                                                           | Do not select TiKV store with given ip addresses.                                                                                                                                                                                                                                                                                                                                                                                                         |
| tidb.snapshot_timestamp            | null                                                                           | It is available for TiDB connector to read snapshot. You could set it by `SET SESSION tidb.snapshot_timestamp='2021-01-01T14:00:00+08:00'` and unset it by `SET SESSION tidb.snapshot_timestamp=''` within a session. The format of timestamp may refer to `java.time.format.DateTimeFormatter#ISO_ZONED_DATE_TIME`.                                                                                                                                      |
| tidb.dns.search                    | null                                                                           | Append dns search suffix to host names. It's especially necessary to map K8S cluster local name to FQDN.                                                                                                                                                                                                                                                                                                                                                  |