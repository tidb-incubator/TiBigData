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
| tidb.database.url       | -             | TiDB connector has a built-in JDBC connection pool implemented by [HikariCP](https://github.com/brettwooldridge/HikariCP), you should provide your own TiDB server address with a jdbc url format:  `jdbc:mysql://host:port/database` or `jdbc:tidb://host:port/database`. If you have multiple TiDB server addresses and the amount of data to be inserted is huge, it would be better to use TiDB jdbc driver rather then MySQL jdbc driver. TiDB driver is a load-balancing driver, it will query all TiDB server addresses and pick one  randomly when establishing connections. |
| tidb.username           | -             | JDBC username.                                               |
| tidb.password           | null          | JDBC password.                                               |
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

**Insert**

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

You could create a tidb table which contains most tidb types by the following script.

```
trino:test> CREATE TABLE tidb.test."test_tidb_type"(
         ->  c1     tinyint,
         ->  c2     smallint,
         ->  c3     int,
         ->  c4     bigint,
         ->  c5     char(10),
         ->  c6     varchar(20),
         ->  c7     varbinary,
         ->  c8     real,
         ->  c9     double,
         ->  c10    decimal(6,3),
         ->  c11    date,
         ->  c12    time,
         ->  c13    timestamp,
         ->  c14    boolean,
         ->  c15    json
         -> );
CREATE TABLE

trino:test> select * from tidb.test."test_tidb_type";
 c1 | c2 | c3 | c4 | c5 | c6 | c7 | c8 | c9 | c10 | c11 | c12 | c13 | c14 | c15
----+----+----+----+----+----+----+----+----+-----+-----+-----+-----+-----+-----
(0 rows)

Query 20211123_060724_00020_nm3jm, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
1.00 [0 rows, 0B] [0 rows/s, 0B/s]
```

```
trino:test> INSERT INTO tidb.test.test_tidb_type(
         ->     c1,c2,c3,c4,c5,
         ->     c6,c7,c8,c9,c10,
         ->     c11,c12,c13,c14,c15
         -> )
         -> VALUES (
         ->     tinyint '1',
         ->     smallint '2',
         ->     int '3',
         ->     bigint '4',
         ->     'char5',
         ->     'varchar6',
         ->     varbinary '712',
         ->     real '8.3e0',
         ->     91.8,
         ->     10.123,
         ->     date '2020-08-11',
         ->     time '15:30:12',
         ->     timestamp '2020-08-10 15:30:13',
         ->     tinyint '1',
         ->     '{"a":15,"b":2}'
         -> );
INSERT: 1 row

Query 20211123_062238_00048_nm3jm, FINISHED, 1 node
Splits: 35 total, 35 done (100.00%)
0.25 [0 rows, 0B] [0 rows/s, 0B/s]

trino:test> select * from tidb.test.test_tidb_type;
 c1 | c2 | c3 | c4 |  c5   |    c6    |    c7    | c8  |  c9  |  c10   |    c11     |     c12      |           c13           | c14 |      c15
----+----+----+----+-------+----------+----------+-----+------+--------+------------+--------------+-------------------------+-----+----------------
  1 |  2 |  3 |  4 | char5 | varchar6 | 37 31 32 | 8.3 | 91.8 | 10.123 | 2020-08-11 | 13:20:00.000 | 2020-08-10 15:30:13.000 |   1 | {"a":15,"b":2}
(1 row)

Query 20211123_062322_00052_nm3jm, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
1.04 [1 rows, 0B] [0 rows/s, 0B/s]
```