# Trino-TiDB-Connector

## Build

```bash
# get project
git clone git@github.com:tidb-incubator/TiBigData.git
cd TiBigData

# compile and package, must be Java 11
mvn clean package -DskipTests -am -pl trino

# unpack it and copy it into trino plugin folder
tar -zxf trino/target/trino-connector-0.0.5-SNAPSHOT-plugin.tar.gz -C trino/target
cp -r trino/target/trino-connector-0.0.5-SNAPSHOT/tidb ${TRINO_HOME}/plugin

# need a jdbc driver
cp ${YOUR_MYSQL_JDBC_DRIVER_PATH}/mysql-connector-java-${version}.jar ${TRINO_HOME}/plugin/tidb

# or, you can find it in the plugin folder of mysql
cp -rf plugin/mysql/mysql-connector-java-${version}.jar plugin/tidb/
```

## DataTypes

|     TiDB     |    Trino     |
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
| tidb.database.url              |  -             | You should provide your own TiDB server address with a jdbc url format:  `jdbc:mysql://host:port/database` or `jdbc:tidb://host:port/database`. If you have multiple TiDB server addresses and the amount of data to be inserted is huge, it would be better to use TiDB jdbc driver rather then MySQL jdbc driver. TiDB driver is a load-balancing driver, it will query all TiDB server addresses and pick one  randomly when establishing connections. |
| tidb.username           | -             | JDBC username.                                               |
| tidb.password           | null          | JDBC password.                                               |
| tidb.jdbc.connection-provider-impl                  | io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.BasicJdbcConnectionProvider | JDBC connection provider implements: set 'io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.HikariDataSourceJdbcConnectionProvider', TiBigData will use JDBC connection pool implemented by [HikariCP](https://github.com/brettwooldridge/HikariCP) to provider connection; set 'io.tidb.bigdata.tidb.JdbcConnectionProviderFactory.BasicJdbcConnectionProvider', connection pool will not be used.  |
| tidb.maximum.pool.size  | 10            | Connection pool size.                                        |
| tidb.minimum.idle.size  | 10            | The minimum number of idle connections that HikariCP tries to maintain in the pool. |
| tidb.write_mode         | append        | TiDB sink write mode: `upsert` or `append`. You could config it in `tidb.properties`, or set it by `SET SESSION tidb.write_mode='upsert'` within a session. TiDB primary key columns and unique key columns will be mapped as trino table properties `primary_key` and `unique_key`. |
| tidb.replica-read              | leader | Read data from specified role. The optional roles are leader, follower and learner. You can also specify multiple roles, and we will pick the roles you specify in order. |
| tidb.replica-read.label        | null          | Only select TiKV store match specified labels. Format: label_x=value_x,label_y=value_y |
| tidb.replica-read.whitelist    | null          | Only select TiKV store with given ip addresses. |
| tidb.replica-read.blacklist    | null          | Do not select TiKV store with given ip addresses. |
| tidb.snapshot_timestamp | null          | It is available for TiDB connector to read snapshot. You could set it by `SET SESSION tidb.snapshot_timestamp='2021-01-01T14:00:00+08:00'` and unset it by `SET SESSION tidb.snapshot_timestamp=''` within a session. The format of timestamp may refer to `java.time.format.DateTimeFormatter#ISO_ZONED_DATE_TIME`. |
| tidb.dns.search | null | Append dns search suffix to host names. It's especially necessary to map K8S cluster local name to FQDN. |

## Usage

### Properties

```bash
vim ${TRINO_HOME}/etc/catalog/tidb.properties
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

Then restart your trino cluster and use trino-cli to connect trino coordinator:

```bash
./trino-cli-${version}-executable.jar --server ${COORDINATOR_HOST}:${PORT} --catalog tidb --schema ${TIDB_DATABASE} --user ${USERNAME}
```

### Example

**Connection**

```
$ ./trino-cli-359-executable.jar --server localhost:8080 --catalog tidb --schema test --user test
```

**Query**

```
trino:test> show tables;
 Table  
--------
 table1 
(1 row)

Query 20211122_160258_00008_u6h64, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0.23 [1 rows, 20B] [4 rows/s, 86B/s]

trino:test> select * from table1;
 data1 | data2 
-------+-------
 aaa   |   111 
(1 row)

Query 20211122_160305_00010_u6h64, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0.78 [1 rows, 0B] [1 rows/s, 0B/s]
```

**Simple Insert**

```
trino:test> insert into table1 values('bbb', 222);
INSERT: 1 row

Query 20211122_161035_00014_u6h64, FINISHED, 1 node
Splits: 35 total, 35 done (100.00%)
0.23 [0 rows, 0B] [0 rows/s, 0B/s]

trino:test> select * from table1;
 data1 | data2 
-------+-------
 aaa   |   111 
 bbb   |   222 
(2 rows)

Query 20211122_161041_00015_u6h64, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0.63 [2 rows, 0B] [3 rows/s, 0B/s]
```

**Upsert**

setting `tidb.properties` `tidb.write_mode=upsert`

or

```sql
SET SESSION tidb.write_mode='upsert';
```

**TiDB all variable test inserts**

You could create a in tidb table which contains most tidb types by the following script.

>NOTE: Execute it in TiDB

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

Then insert data and query this test table in trino:

>NOTE: Execute it in Trino

```
trino> INSERT INTO tidb.test.test_tidb_type (
    ->  c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,
    ->  c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,
    ->  c21,c22,c23,c24,c25,c26,c27,c28,c29
    -> )
    -> VALUES (
    ->  tinyint '1',
    ->  smallint '2',
    ->  int '3',
    ->  int '4',
    ->  bigint '5',
    ->  'chartype',
    ->  'varchartype',
    ->  'tinytexttype',
    ->  'mediumtexttype',
    ->  'texttype',
    ->  'longtexttype',
    ->  varbinary 'binarytype',
    ->  varbinary 'varbinarytype',
    ->  varbinary 'tinyblobtype',
    ->  varbinary 'mediumblobtype',
    ->  varbinary 'blobtype',
    ->  varbinary 'longblobtype',
    ->  1.234,
    ->  2.456789,
    ->  123.456,
    ->  date '2020-08-10',
    ->  time '15:30:29',
    ->  timestamp '2020-08-10 15:30:29',
    ->  timestamp '2020-08-10 16:30:29',
    ->  smallint '2020',
    ->  tinyint '1',
    ->  '{"a":1,"b":2}',
    ->  '1',
    ->  'a'
    -> );
INSERT: 1 row

Query 20211123_102946_00000_iic32, FINISHED, 1 node
Splits: 35 total, 35 done (100.00%)
1.95 [0 rows, 0B] [0 rows/s, 0B/s]

trino> select * from tidb.test.test_tidb_type\G;
-[ RECORD 1 ]----------------------------------------
c1  | 1
c2  | 2
c3  | 3
c4  | 4
c5  | 5
c6  | chartype
c7  | varchartype
c8  | tinytexttype
c9  | mediumtexttype
c10 | texttype
c11 | longtexttype
c12 | 62 69 6e 61 72 79 74 79 70 65 00 00 00 00 00 00
    | 00 00 00 00
c13 | 76 61 72 62 69 6e 61 72 79 74 79 70 65
c14 | 74 69 6e 79 62 6c 6f 62 74 79 70 65
c15 | 6d 65 64 69 75 6d 62 6c 6f 62 74 79 70 65
c16 | 62 6c 6f 62 74 79 70 65
c17 | 6c 6f 6e 67 62 6c 6f 62 74 79 70 65
c18 | 1.234
c19 | 2.456789
c20 | 123.456
c21 | 2020-08-10
c22 | 19:33:20
c23 | 2020-08-10 15:30:29
c24 | 2020-08-10 16:30:29
c25 | 2020
c26 | 1
c27 | {"a":1,"b":2}
c28 | 1
c29 | a

Query 20211123_103042_00002_iic32, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0.84 [1 rows, 0B] [1 rows/s, 0B/s]
```