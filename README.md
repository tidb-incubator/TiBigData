# TiBigData

[License](https://github.com/pingcap-incubator/TiBigData/blob/master/LICENSE)

Misc BigData components for TiDB, Presto & Flink connectors for example.

## License

TiBigData project is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Getting Started

### Implements

|        | Flink                  | Presto                 |
| ------ | ---------------------- | ---------------------- |
| source | `tikv-java-client`     | `tikv-java-client`     |
| sink   | `JdbcDynamicTableSink` | `mysql-connector-java` |

#### Configuration

| Configration                | Scope                                     | Default Value | Description                                                  |
| --------------------------- | ----------------------------------------- | ------------- | ------------------------------------------------------------ |
| tidb.database.url      | Presto and Flink                          | -             | TiDB connector has a built-in JDBC connection pool implemented by [HikariCP](https://github.com/brettwooldridge/HikariCP), you should provide your own TiDB server address with a jdbc url format:  `jdbc:mysql://host:port/database` or `jdbc:tidb://host:port/database`. If you have multiple TiDB server addresses and the amount of data to be inserted is huge, it would be better to use TiDB jdbc driver rather then MySQL jdbc driver. TiDB driver is a load-balancing driver, it will query all TiDB server addresses and pick one  randomly when establishing connections. |
| tidb.username          | Presto and Flink                          | -             | JDBC username.                                               |
| tidb.password          | Presto and Flink                          | null          | JDBC password.                                               |
| tidb.maximum.pool.size | Presto and Flink                          | 10            | Connection pool size.                                       |
| tidb.minimum.idle.size | Presto and Flink                          | 10           | The minimum number of idle connections that HikariCP tries to maintain in the pool. |
| tidb.write_mode             | Presto and Flink                     | append        | TiDB sink write mode: `upsert` or `append`.  For presto, you could config it in `tidb.properties`, or set it by `SET SESSION tidb.write_mode='upsert'` within a session. |
| tidb.replica-read |Presto and Flink|false|Read data from follower.|
| tidb.database.name          | Flink SQL only, it is no need for catalog | null          | database name.                                               |
| tidb.table.name             | Flink SQL only, it is no need for catalog | null          | table name.                                                  |
| timestamp-format.${columnName} | Flink SQL only | null | For each column, you could specify timestamp format in two cases: 1. TiDB `timestamp` is mapped to Flink `string`; 2. TiDB `varchar` is mapped to Flink `timestamp`. Format of timestamp may refer to `java.time.format.DateTimeFormatter`, like `yyyy-MM-dd HH:mm:ss.SSS`. |
| sink.buffer-flush.max-rows | Flink | 100 | The max size of buffered records before flush. Can be set to zero to disable it. |
| sink.buffer-flush.interval | Flink | 1s | The flush interval mills, over this time, asynchronous threads will flush data. Can be set to `'0'` to disable it. Note, `'sink.buffer-flush.max-rows'` can be set to `'0'` with the flush interval set allowing for complete async processing of buffered actions. |
| sink.max-retries | Flink | 3 | The max retry times if writing records to database failed. |
| tidb.filter-push-down | Flink | false | Support filter push down. |


TiDB Flink sink supports all sink properties of  [flink-connector-jdbc](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/jdbc.html), because it is implemented by JdbcDynamicTableSink.

#### DataType Mapping

| index |    TiDB    |     Flink(deault)     | Prestodb  | Prestosql |
| ----- | :--------: | :-------------------: | :-------: | :-------: |
| 1     |  TINYINT   |  DataTypes.TINYINT()  |  TINYINT  |  TINYINT  |
| 2     |  SMALLINT  | DataTypes.SMALLINT()  | SMALLINT  | SMALLINT  |
| 3     | MEDIUMINT  |    DataTypes.INT()    |    INT    |    INT    |
| 4     |    INT     |    DataTypes.INT()    |    INT    |    INT    |
| 5     |   BIGINT   |  DataTypes.BIGINT()   |  BIGINT   |  BIGINT   |
| 6     |    CHAR    |  DataTypes.STRING()   |  VARCHAR  |  VARCHAR  |
| 7     |  VARCHAR   |  DataTypes.STRING()   |  VARCHAR  |  VARCHAR  |
| 8     |  TINYTEXT  |  DataTypes.STRING()   |  VARCHAR  |  VARCHAR  |
| 9     | MEDIUMTEXT |  DataTypes.STRING()   |  VARCHAR  |  VARCHAR  |
| 10    |    TEXT    |  DataTypes.STRING()   |  VARCHAR  |  VARCHAR  |
| 11    |  LONGTEXT  |  DataTypes.STRING()   |  VARCHAR  |  VARCHAR  |
| 12    |   BINARY   |   DataTypes.BYTES()   | VARBINARY | VARBINARY |
| 13    | VARBINARY  |   DataTypes.BYTES()   | VARBINARY | VARBINARY |
| 14    |  TINYBLOB  |   DataTypes.BYTES()   | VARBINARY | VARBINARY |
| 15    | MEDIUMBLOB |   DataTypes.BYTES()   | VARBINARY | VARBINARY |
| 16    |    BLOB    |   DataTypes.BYTES()   | VARBINARY | VARBINARY |
| 17    |  LONGBLOB  |   DataTypes.BYTES()   | VARBINARY | VARBINARY |
| 18    |   FLOAT    |   DataTypes.FLOAT()   |   REAL    |   REAL    |
| 19    |   DOUBLE   |  DataTypes.DOUBLE()   |  DOUBLE   |  DOUBLE   |
| 20    |  DECIMAL   |  DataTypes.DECIMAL()  |  DECIMAL  |  DECIMAL  |
| 21    |    DATE    |   DataTypes.DATE()    |   DATE    |   DATE    |
| 22    |    TIME    |   DataTypes.TIME()    |   TIME    |   TIME    |
| 23    |  DATETIME  | DataTypes.TIMESTAMP() | TIMESTAMP | TIMESTAMP |
| 24    | TIMESTAMP  | DataTypes.TIMESTAMP() | TIMESTAMP | TIMESTAMP |
| 25    |    YEAR    | DataTypes.SMALLINT()  | SMALLINT  | SMALLINT  |
| 26    |    BOOL    |  DataTypes.BOOLEAN()  |  TINYINT  |  TINYINT  |
| 27    |    JSON    |  DataTypes.STRING()   |   JSON    |  VARCHAR  |
| 28    |    ENUM    |  DataTypes.STRING()   |  VARCHAR  |  VARCHAR  |
| 29    |    SET     |  DataTypes.STRING()   |  VARCHAR  |  VARCHAR  |

### Build

```bash
git clone git@github.com:pingcap-incubator/TiBigData.git
cd TiBigData
mvn clean package -DskipTests
```

### Test Table

You could create a sample tidb table which contains most tidb types by the following script.

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



### Presto-TiDB-Connector

#### Install TiDB Plugin

##### prestodb

```bash
tar -zxf prestodb/target/prestodb-connector-0.0.1-SNAPSHOT-plugin.tar.gz -C prestodb/target
cp -r prestodb/target/prestodb-connector-0.0.1-SNAPSHOT/tidb ${PRESTO_HOME}/plugin
cp ${YOUR_MYSQL_JDBC_DRIVER_PATH}/mysql-connector-java-${version}.jar ${PRESTO_HOME}/plugin/tidb
```

##### prostosql

```bash
tar -zxf prestosql/target/prestosql-connector-0.0.1-SNAPSHOT-plugin.tar.gz -C prestosql/target
cp -r prestosql/target/prestosql-connector-0.0.1-SNAPSHOT/tidb ${PRESTO_HOME}/plugin
cp ${YOUR_MYSQL_JDBC_DRIVER_PATH}/mysql-connector-java-${version}.jar ${PRESTO_HOME}/plugin/tidb
```

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
tidb.maximum.pool.size=10
tidb.minimum.idle.size=0
tidb.write_mode=upsert
```

then restart your presto cluster and use presto-cli to connect presto coordinator:

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
You can also insert and query test table:

```sql
-- prestodb
INSERT INTO test_tidb_type
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
 json '{"a":1,"b":2}',
 '1',
 'a'
);

-- prestosql
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
)

-- query
select * from test_tidb_type;
```

If there is primary key or unique key in tidb table, you can enable upsert mode by `SET SESSION tidb.write_mode='upsert'`. TiDB primary key columns and unique key columns will be mapped as presto table properties `primary_key` and `unique_key`.


### Flink-TiDB-Connector

```bash
cp flink/target/flink-tidb-connector-0.0.1-SNAPSHOT.jar ${FLINK_HOME}/lib
bin/flink run -c com.zhihu.tibigdata.flink.tidb.examples.TiDBCatalogDemo lib/flink-tidb-connector-0.0.1-SNAPSHOT.jar --tidb.database.url ${DATABASE_URL} --tidb.username ${USERNAME} --tidb.password ${PASSWORD} --tidb.database.name ${TIDB_DATABASE} --tidb.table.name ${TABLE_NAME}
```

The output can be found in console, like:

```bash
Job has been submitted with JobID 3a64ea7affe969eef77790048923b5b6
+----------------------+--------------------------------+--------------------------------+
|                   id |                           name |                            sex |
+----------------------+--------------------------------+--------------------------------+
|                    1 |                           java |                              1 |
|                    2 |                          scala |                              2 |
|                    3 |                         python |                              1 |
+----------------------+--------------------------------+--------------------------------+
3 rows in set
```



#### TiDBCatalog

The above example is implemented by TiDBCatalog.

```java
public class TiDBCatalogDemo {

  public static void main(String[] args) {
    // properties
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    final Map<String, String> properties = parameterTool.toMap();
    final String databaseName = parameterTool.getRequired("tidb.database.name");
    final String tableName = parameterTool.getRequired("tidb.table.name");
    // env
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    // register TiDBCatalog
    TiDBCatalog catalog = new TiDBCatalog(properties);
    catalog.open();
    tableEnvironment.registerCatalog("tidb", catalog);
    tableEnvironment.useCatalog("tidb");
    // query and print
    String sql = String.format("SELECT * FROM `%s`.`%s` LIMIT 100", databaseName, tableName);
    TableResult tableResult = tableEnvironment.executeSql(sql);
    System.out.println(tableResult.getTableSchema());
    tableResult.print();
  }
}
```

You could submit DDL  by TiDBCatalog, such as create table, drop table: 

```java
public class TestCreateTable {

  public static void main(String[] args) throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("tidb.database.url", "jdbc:mysql://host:port/database");
    properties.put("tidb.username", "root");
    properties.put("tidb.password", "123456");
    TiDBCatalog catalog = new TiDBCatalog(properties);
    catalog.open();
    String sql = "CREATE TABLE IF NOT EXISTS people(id INT, name VARCHAR(255), sex ENUM('1','2'))";
    catalog.sqlUpdate(sql);
    catalog.close();
  }
}
```

If you want to specify the type by yourself, please use `Flink SQL`. It supports most type conversions, such as INT to BIGINT, STRING to LONG, LONG to BYTES.


#### Flink SQL

##### Flink SQL Programming

```java
public class TestFlinkSql {

  public static void main(String[] args) {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    tableEnvironment.executeSql("CREATE TABLE tidb(\n"
        + " c1     tinyint,\n"
        + " c2     smallint,\n"
        + " c3     int,\n"
        + " c4     int,\n"
        + " c5     bigint,\n"
        + " c6     char(10),\n"
        + " c7     varchar(20),\n"
        + " c8     string,\n"
        + " c9     string,\n"
        + " c10    string,\n"
        + " c11    string,\n"
        + " c12    string,\n"
        + " c13    string,\n"
        + " c14    string,\n"
        + " c15    string,\n"
        + " c16    string,\n"
        + " c17    string,\n"
        + " c18    float,\n"
        + " c19    double,\n"
        + " c20    decimal(6,3),\n"
        + " c21    date,\n"
        + " c22    time,\n"
        + " c23    timestamp,\n"
        + " c24    timestamp,\n"
        + " c25    smallint,\n"
        + " c26    boolean,\n"
        + " c27    string,\n"
        + " c28    string,\n"
        + " c29    string\n"
        + ") WITH (\n"
        + "  'connector' = 'tidb',\n"
        + "  'tidb.database.url' = 'jdbc:mysql://host:port/database',\n"
        + "  'tidb.username' = 'root',\n"
        + "  'tidb.password' = '123456',\n"
        + "  'tidb.database.name' = 'database',\n"
        + "  'tidb.table.name' = 'test_tidb_type'\n"
        + ")"
    );
    tableEnvironment.executeSql("SELECT * FROM tidb LIMIT 100").print();
  }
```

##### Flink SQL Client

On the one hand, you can use TiDB Catalog in flink sql client by environment file:  `env.yaml`.

```yaml
catalogs:
   - name: tidb
     type: tidb
     tidb.database.url: jdbc:mysql://host:port/database
     tidb.username: root
     tidb.password: 123456

execution:
        planner: blink
        type: batch
        parallelism: 1
```

then run sql-client and query tidb table:

```sql
bin/sql-client.sh embedded -e env.yaml
-- set result format
SET execution.result-mode=tableau;
-- query
SELECT * FROM `tidb`.`default`.`test_tidb_type` LIMIT 100;
```

On the other hand, you can also create mapping table by yourself: 

```sql
-- run flink sql client
bin/sql-client.sh embedded
-- create a flink table mapping to tidb table
CREATE TABLE tidb(
 c1     tinyint,
 c2     smallint,
 c3     int,
 c4     int,
 c5     bigint,
 c6     char(10),
 c7     varchar(20),
 c8     string,
 c9     string,
 c10    string,
 c11    string,
 c12    string,
 c13    string,
 c14    string,
 c15    string,
 c16    string,
 c17    string,
 c18    float,
 c19    double,
 c20    decimal(6,3),
 c21    date,
 c22    time,
 c23    timestamp,
 c24    timestamp,
 c25    smallint,
 c26    boolean,
 c27    string,
 c28    string,
 c29    string
) WITH (
  'connector' = 'tidb',
  'tidb.database.url' = 'jdbc:mysql://host:port/database',
  'tidb.username' = 'root',
  'tidb.password' = '123456',
  'tidb.database.name' = 'database',
  'tidb.maximum.pool.size' = '10',
  'tidb.minimum.idle.size' = '0',
  'tidb.table.name' = 'test_tidb_type',
  'tidb.write_mode' = 'upsert',
  'sink.buffer-flush.max-rows' = '0'
);
-- insert data
INSERT INTO tidb
VALUES (
 cast(1 as tinyint) ,
 cast(1 as smallint) ,
 cast(1 as int) ,
 cast(1 as int) ,
 cast(1 as bigint) ,
 cast('chartype' as char(10)),
 cast('varchartype' as varchar(20)),
 cast('tinytexttype' as string),
 cast('mediumtexttype' as string),
 cast('texttype' as string),
 cast('longtexttype' as string),
 cast('binarytype' as string),
 cast('varbinarytype' as string),
 cast('tinyblobtype' as string),
 cast('mediumblobtype' as string),
 cast('blobtype' as string),
 cast('longblobtype' as string),
 cast(1.234 as float),
 cast(2.456789 as double),
 cast(123.456 as decimal(6,3)),
 cast('2020-08-10' as date),
 cast('15:30:29' as time),
 cast('2020-08-10 15:30:29' as timestamp),
 cast('2020-08-10 16:30:29' as timestamp),
 cast(2020 as smallint),
 true,
 cast('{"a":1,"b":2}' as string),
 cast('1' as string),
 cast('a' as string)
);
-- set result format
SET execution.result-mode=tableau;
-- query
SELECT * FROM tidb LIMIT 100;
```
