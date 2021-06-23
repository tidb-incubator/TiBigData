# PrestoSQL-TiDB-Connector

## Build

```bash
git clone git@github.com:pingcap-incubator/TiBigData.git
cd TiBigData
# need Java 11
mvn clean package -DskipTests -am -pl prestosql
tar -zxf prestosql/target/prestosql-connector-0.0.4-SNAPSHOT-plugin.tar.gz -C prestosql/target
cp -r prestosql/target/prestosql-connector-0.0.4-SNAPSHOT/tidb ${PRESTO_HOME}/plugin
cp ${YOUR_MYSQL_JDBC_DRIVER_PATH}/mysql-connector-java-${version}.jar ${PRESTO_HOME}/plugin/tidb
```

## DataTypes

|     TiDB     |  PrestoSQL   |
| :----------: | :----------: |
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

## Configuration

| Configration            | Default Value | Description                                                  |
| :---------------------- | :------------ | :----------------------------------------------------------- |
| tidb.database.url       | -             | TiDB connector has a built-in JDBC connection pool implemented by [HikariCP](https://github.com/brettwooldridge/HikariCP), you should provide your own TiDB server address with a jdbc url format:  `jdbc:mysql://host:port/database` or `jdbc:tidb://host:port/database`. If you have multiple TiDB server addresses and the amount of data to be inserted is huge, it would be better to use TiDB jdbc driver rather then MySQL jdbc driver. TiDB driver is a load-balancing driver, it will query all TiDB server addresses and pick one  randomly when establishing connections. |
| tidb.username           | -             | JDBC username.                                               |
| tidb.password           | null          | JDBC password.                                               |
| tidb.maximum.pool.size  | 10            | Connection pool size.                                        |
| tidb.minimum.idle.size  | 10            | The minimum number of idle connections that HikariCP tries to maintain in the pool. |
| tidb.write_mode         | append        | TiDB sink write mode: `upsert` or `append`. You could config it in `tidb.properties`, or set it by `SET SESSION tidb.write_mode='upsert'` within a session. TiDB primary key columns and unique key columns will be mapped as presto table properties `primary_key` and `unique_key`. |
| tidb.replica-read              | leader / follower / leader_and_follower | Read data from specified role. |
| tidb.replica-read.label        | null          | Only select TiKV store match specified labels. Format: label_x=value_x,label_y=value_y |
| tidb.replica-read.whitelist    | null          | Only select TiKV store with given ip addresses. |
| tidb.replica-read.blacklist    | null          | Do not select TiKV store with given ip addresses. |
| tidb.filter-push-down   | false         | Support filter push down.                                    |
| tidb.snapshot_timestamp | null          | It is available for TiDB connector to read snapshot. You could set it by `SET SESSION tidb.snapshot_timestamp='2021-01-01T14:00:00+08:00'` and unset it by `SET SESSION tidb.snapshot_timestamp=''` within a session. The format of timestamp may refer to `java.time.format.DateTimeFormatter#ISO_ZONED_DATE_TIME`. |
| tidb.dns.search | null | Append dns search suffix to host names. It's especially necessary to map K8S cluster local name to FQDN. |

## Usage

### Properties

```bash
vim ${PRESTO_HOME}/etc/catalog/tidb.properties
```

The file `tidb.properties` like :

```properties
# connector name, must be tidb
connector.name=tidb
tidb.database.url=jdbc:mysql://host:port/database
tidb.username=root
tidb.password=123456
tidb.maximum.pool.size=1
tidb.minimum.idle.size=1
tidb.write_mode=upsert
```

Then restart your presto cluster and use presto-cli to connect presto coordinator:

```bash
presto-cli --server ${COORDINATOR_HOST}:${PORT} --catalog tidb --schema ${TIDB_DATABASE} --user ${USERNAME}
```

#### Example

```sql
-- show tables
SHOW TABLES;
-- show databases
SHOW SCHEMAS;
-- create table
CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (id INT, name VARCHAR(255), sex VARCHAR(255));
-- show table schema
SHOW CREATE TABLE ${TABLE_NAME};
-- drop table 
DROP TABLE IF EXISTS ${TABLE_NAME};
-- query
SELECT * FROM ${TABLE_NAME} LIMIT 100;
-- rename table
ALTER TABLE ${TABLE_NAME} RENAME TO ${NEW_TABLE_NAME};
-- add column
ALTER TABLE ${TABLE_NAME} ADD COLUMN ${COLUMN_NAME} ${COLUMN_TYPE};
-- drop column
ALTER TABLE ${TABLE_NAME} DROP COLUMN ${COLUMN_NAME};
-- rename column
ALTER TABLE ${TABLE_NAME} RENAME COLUMN ${COLUMN_NAME} TO ${NEW_COLUMN_NAME};
-- synchronize data between tidb table and other tables
CREATE TABLE ${CATALOG}.${DATABASE}.${TABLE} AS SELECT * FROM ${CATALOG}.${DATABASE}.${TABLE};
-- tidb to hive
CREATE TABLE hive.default.people AS SELECT * FROM tidb.default.people;
-- hive to tidb
CREATE TABLE tidb.default.people AS SELECT * FROM hive.default.people;
-- enable upsert mode
SET SESSION tidb.write_mode='upsert';
```
#### Insert and Upsert

You could create a tidb table which contains most tidb types by the following script.

```sql
CREATE TABLE `default`.`test_tidb_type`(
 c1     tinyint,
 c2     smallint,
 c3     mediumint,
 c4     int,
 c5     bigint,
 c6     char(10),
 c7     varchar(20),
 c8     tinytext,
 c9     mediumtext,
 c10    text,
 c11    longtext,
 c12    binary(20),
 c13    varbinary(20),
 c14    tinyblob,
 c15    mediumblob,
 c16    blob,
 c17    longblob,
 c18    float,
 c19    double,
 c20    decimal(6,3),
 c21    date,
 c22    time,
 c23    datetime,
 c24    timestamp,
 c25    year,
 c26    boolean,
 c27    json,
 c28    enum('1','2','3'),
 c29    set('a','b','c')
);
```

Then insert data and query this test table:

```sql
INSERT INTO test_tidb_type(
 c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,
 c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,
 c21,c22,c23,c24,c25,c26,c27,c28,c29
) 
VALUES (
 tinyint '1',
 smallint '2',
 int '3',
 int '4',
 bigint '5',
 'chartype',
 'varchartype',
 'tinytexttype',
 'mediumtexttype',
 'texttype',
 'longtexttype',
 varbinary 'binarytype',
 varbinary 'varbinarytype',
 varbinary 'tinyblobtype',
 varbinary 'mediumblobtype',
 varbinary 'blobtype',
 varbinary 'longblobtype',
 1.234,
 2.456789,
 123.456,
 date '2020-08-10',
 time '15:30:29',
 timestamp '2020-08-10 15:30:29',
 timestamp '2020-08-10 16:30:29',
 smallint '2020',
 tinyint '1',
 '{"a":1,"b":2}',
 '1',
 'a'
);
-- query
select * from test_tidb_type;
```
