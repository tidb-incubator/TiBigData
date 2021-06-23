# Flink-TiDB-Connector

## Component

| Name                     | Supported | Description                                                  |
| ------------------------ | --------- | ------------------------------------------------------------ |
| `DynamicTableSource` | true      | Implemented by `tikv-java-client`                            |
| `DynamicTableSink`   | true      | Implemented by `JdbcDynamicTableSink` now, and it will be implemented by `tikv-java-client` in the future. |
| `TableFactory`       | true      |                                                              |
| `CatalogFactory`     | true      |                                                              |

## Version

`Flink-1.11` and `Flink-1.12` are supported.

## Build

```bash
git clone git@github.com:pingcap-incubator/TiBigData.git
cd TiBigData
mvn clean package -DskipTests -am -pl flink/flink-${FLINK_VERSION}
cp flink/flink-${FLINK_VERSION}/target/flink-tidb-connector-${FLINK_VERSION}-0.0.4-SNAPSHOT.jar ${FLINK_HOME}/lib
```

Then restart your Flink cluster.

## Demo

```bash
bin/flink run -c io.tidb.bigdata.flink.tidb.examples.TiDBCatalogDemo lib/flink-tidb-connector-${FLINK_VERSION}-0.0.4-SNAPSHOT.jar --tidb.database.url ${DATABASE_URL} --tidb.username ${USERNAME} --tidb.password ${PASSWORD} --tidb.database.name ${TIDB_DATABASE} --tidb.table.name ${TABLE_NAME}
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

## DataTypes

|    TiDB    |     Flink(Catalog)     |
| :--------: | :-------------------: |
|  TINYINT   |  TINYINT  |
|  SMALLINT  | SMALLINT  |
| MEDIUMINT  |    INT    |
|    INT     |    INT    |
|   BIGINT   |  BIGINT   |
|    CHAR    |  STRING   |
|  VARCHAR   |  STRING   |
|  TINYTEXT  |  STRING   |
| MEDIUMTEXT |  STRING   |
|    TEXT    |  STRING   |
|  LONGTEXT  |  STRING   |
|   BINARY   |   BYTES   |
| VARBINARY  |   BYTES   |
|  TINYBLOB  |   BYTES   |
| MEDIUMBLOB |   BYTES   |
|    BLOB    |   BYTES   |
|  LONGBLOB  |   BYTES   |
|   FLOAT    |   FLOAT   |
|   DOUBLE   |  DOUBLE   |
| DECIMAL(p,s) |  DECIMAL(p,s)  |
|    DATE    |   DATE    |
|    TIME    |   TIME    |
|  DATETIME  | TIMESTAMP |
| TIMESTAMP  | TIMESTAMP |
|    YEAR    | SMALLINT  |
|    BOOL    |  BOOLEAN  |
|    JSON    |  STRING   |
|    ENUM    |  STRING   |
|    SET     |  STRING   |

If you want to specify the type by yourself, please use `Flink SQL`. It supports most type conversions, such as INT to BIGINT, STRING to LONG.

## Configuration

| Configration                   |  Default Value | Description                                                  |
| :----------------------------- |  :------------ | :----------------------------------------------------------- |
| tidb.database.url              |  -             | TiDB connector has a built-in JDBC connection pool implemented by [HikariCP](https://github.com/brettwooldridge/HikariCP), you should provide your own TiDB server address with a jdbc url format:  `jdbc:mysql://host:port/database` or `jdbc:tidb://host:port/database`. If you have multiple TiDB server addresses and the amount of data to be inserted is huge, it would be better to use TiDB jdbc driver rather then MySQL jdbc driver. TiDB driver is a load-balancing driver, it will query all TiDB server addresses and pick one  randomly when establishing connections. |
| tidb.username                  | -             | JDBC username.                                               |
| tidb.password                  |null          | JDBC password.                                               |
| tidb.maximum.pool.size         | 10            | Connection pool size.                                        |
| tidb.minimum.idle.size         | 10            | The minimum number of idle connections that HikariCP tries to maintain in the pool. |
| tidb.write_mode                | append        | TiDB sink write mode: `upsert` or `append`. |
| tidb.replica-read              | leader / follower / leader_and_follower | Read data from specified role. |
| tidb.replica-read.label        | null          | Only select TiKV store match specified labels. Format: label_x=value_x,label_y=value_y |
| tidb.replica-read.whitelist    | null          | Only select TiKV store with given ip addresses. |
| tidb.replica-read.blacklist    | null          | Do not select TiKV store with given ip addresses. |
| tidb.database.name             | null          | Database name. It is required for table factory, no need for catalog. |
| tidb.table.name                | null          | Table name. It is required for table factory, no need for catalog. |
| timestamp-format.${columnName} | null          | For each column, you could specify timestamp format in two cases: 1. TiDB `timestamp` is mapped to Flink `string`; 2. TiDB `varchar` is mapped to Flink `timestamp`. Format of timestamp may refer to `java.time.format.DateTimeFormatter`, like `yyyy-MM-dd HH:mm:ss.SSS`. It is optional for table factory, no need for catalog. |
| sink.buffer-flush.max-rows     | 100           | The max size of buffered records before flush. Can be set to zero to disable it. |
| sink.buffer-flush.interval     | 1s            | The flush interval mills, over this time, asynchronous threads will flush data. Can be set to `'0'` to disable it. Note, `'sink.buffer-flush.max-rows'` can be set to `'0'` with the flush interval set allowing for complete async processing of buffered actions. |
| sink.max-retries               | 3             | The max retry times if writing records to database failed.   |
| tidb.filter-push-down          | false         | Support filter push down. It is only available for version 1.12. |
| tidb.snapshot_timestamp        | null          | It is available for TiDB connector to read snapshot. You could configure it in table properties. The format of timestamp may refer to `java.time.format.DateTimeFormatter#ISO_ZONED_DATE_TIME`. |
| tidb.dns.search | null | Append dns search suffix to host names. It's especially necessary to map K8S cluster local name to FQDN. |

TiDB Flink sink supports all sink properties of  [`flink-connector-jdbc`](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/jdbc.html), because it is implemented by `JdbcDynamicTableSink`.

## Usage

### TiDBCatalog
The above demo is implemented by TiDBCatalog.

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
        .inBatchMode().build();
    TableEnvironment tableEnvironment = TableEnvironment.create(settings);
    // register TiDBCatalog
    TiDBCatalog catalog = new TiDBCatalog(properties);
    catalog.open();
    tableEnvironment.registerCatalog("tidb", catalog);
    // query and print
    String sql = String.format("SELECT * FROM `tidb`.`%s`.`%s` LIMIT 100", databaseName, tableName);
    System.out.println("Flink SQL: " + sql);
    TableResult tableResult = tableEnvironment.executeSql(sql);
    System.out.println("TableSchema: \n" + tableResult.getTableSchema());
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

### FlinkTableFactory

You could use `FlinkTableFactory` like: 

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

### Flink SQL Client

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

On the one hand, you can create mapping table by yourself: 

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
  'tidb.maximum.pool.size' = '1',
  'tidb.minimum.idle.size' = '1',
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

On the other hand, you can also use TiDBCatalog in flink sql client by environment file:  `env.yaml`.

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
