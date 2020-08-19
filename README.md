# TiBigData

[License](https://github.com/pingcap-incubator/TiBigData/blob/master/LICENSE)

Misc BigData components for TiDB, Presto & Flink connectors for example.

## License

TiBigData project is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Getting Started

### Implements

|        | Flink                  | Presto              |
| ------ | ---------------------- | ------------------- |
| source | `tikv-java-client`     | `tikv-java-client`  |
| sink   | `JdbcDynamicTableSink` | `mysql-jdbc` client |

#### Configuration

| Configration                | Default Value | Description                                                  | Scope                                     |
| --------------------------- | ------------- | ------------------------------------------------------------ | ----------------------------------------- |
| tidb.jdbc.database.url      | -             | TiDB connector has a built-in JDBC connection pool implemented by [HikariCP](https://github.com/brettwooldridge/HikariCP), you should provide  your own TiDB server address with a jdbc url format: `jdbc:mysql://host:port/database`. | both of presto and flink                  |
| tidb.jdbc.username          | -             | JDBC username                                                | both of presto and flink                  |
| tidb.jdbc.password          | null          | JDBC password                                                | both of presto and flink                  |
| tidb.jdbc.maximum.pool.size | 10            | connection pool size                                         | both of presto and flink                  |
| tidb.jdbc.minimum.idle.size | 0             | the minimum number of idle connections that HikariCP tries to maintain in the pool. | both of presto and flink                  |
| tidb.database.name          | null          | database name                                                | flink sql only, it is no need for catalog |
| tidb.table.name             | null          | table name                                                   | flink sql only, it is no need for catalog |

TiDB Flink sink support supports all sink properties of  [flink-connector-jdbc](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/jdbc.html), because it is implemented by JdbcDynamicTableSink.

#### DataType Mapping

| index |    TiDB    |     Flink(deault)     | Prestodb  | Prestosql |
| ----- | :--------: | :-------------------: | --------- | --------- |
| 1     |  TINYINT   |  DataTypes.TINYINT()  | TINYINT   | TINYINT   |
| 2     |  SMALLINT  | DataTypes.SMALLINT()  | SMALLINT  | SMALLINT  |
| 3     | MEDIUMINT  |    DataTypes.INT()    | INT       | INT       |
| 4     |    INT     |    DataTypes.INT()    | INT       | INT       |
| 5     |   BIGINT   |  DataTypes.BIGINT()   | BIGINT    | BIGINT    |
| 6     |    CHAR    |  DataTypes.STRING()   | VARCHAR   | VARCHAR   |
| 7     |  VARCHAR   |  DataTypes.STRING()   | VARCHAR   | VARCHAR   |
| 8     |  TINYTEXT  |  DataTypes.STRING()   | VARCHAR   | VARCHAR   |
| 9     | MEDIUMTEXT |  DataTypes.STRING()   | VARCHAR   | VARCHAR   |
| 10    |    TEXT    |  DataTypes.STRING()   | VARCHAR   | VARCHAR   |
| 11    |  LONGTEXT  |  DataTypes.STRING()   | VARCHAR   | VARCHAR   |
| 12    |   BINARY   |  DataTypes.STRING()   | VARBINARY | VARBINARY |
| 13    | VARBINARY  |  DataTypes.STRING()   | VARBINARY | VARBINARY |
| 14    |  TINYBLOB  |  DataTypes.STRING()   | VARBINARY | VARBINARY |
| 15    | MEDIUMBLOB |  DataTypes.STRING()   | VARBINARY | VARBINARY |
| 16    |    BLOB    |  DataTypes.STRING()   | VARBINARY | VARBINARY |
| 17    |  LONGBLOB  |  DataTypes.STRING()   | VARBINARY | VARBINARY |
| 18    |   FLOAT    |   DataTypes.FLOAT()   | REAL      | REAL      |
| 19    |   DOUBLE   |  DataTypes.DOUBLE()   | DOUBLE    | DOUBLE    |
| 20    |  DECIMAL   |  DataTypes.DECIMAL()  | DECIMAL   | DECIMAL   |
| 21    |    DATE    |   DataTypes.DATE()    | DATE      | DATE      |
| 22    |    TIME    |   DataTypes.TIME()    | TIME      | TIME      |
| 23    |  DATETIME  | DataTypes.TIMESTAMP() | TIMESTAMP | TIMESTAMP |
| 24    | TIMESTAMP  | DataTypes.TIMESTAMP() | TIMESTAMP | TIMESTAMP |
| 25    |    YEAR    | DataTypes.SMALLINT()  | SMALLINT  | SMALLINT  |
| 26    |    BOOL    |  DataTypes.BOOLEAN()  | TINYINT   | TINYINT   |
| 27    |    JSON    |  DataTypes.STRING()   | JSON      | VARCHAR   |
| 28    |    ENUM    |  DataTypes.STRING()   | VARCHAR   | VARCHAR   |

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
 c28    enum('1','2','3')
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
tidb.jdbc.database.url=jdbc:mysql://host:port/database
tidb.jdbc.username=root
tidb.jdbc.password=123456
tidb.jdbc.maximum.pool.size=10
tidb.jdbc.minimum.idle.size=0
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
```
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
 '1'
);

-- prestosql
INSERT INTO test_tidb_type(
 c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,
 c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,
 c21,c22,c23,c24,c25,c26,c27,c28
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
 '1'
)

-- query
select * from test_tidb_type;
```




### Flink-TiDB-Connector

```bash
cp flink/target/flink-tidb-connector-0.0.1-SNAPSHOT.jar ${FLINK_HOME}/lib
bin/flink run -c com.zhihu.tibigdata.flink.tidb.TiDBCatalogDemo lib/flink-tidb-connector-0.0.1-SNAPSHOT.jar --tidb.jdbc.database.url ${DATABASE_URL} --tidb.jdbc.username ${USERNAME} --tidb.jdbc.password ${PASSWORD} --tidb.database.name ${TIDB_DATABASE} --tidb.table.name ${TABLE_NAME}
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
    final Map<String, String> properties = (Map) parameterTool.getProperties();
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
    Properties properties = new Properties();
    properties.setProperty("tidb.jdbc.database.url", "jdbc:mysql://host:port/database");
    properties.setProperty("tidb.jdbc.username", "root");
    properties.setProperty("tidb.jdbc.password", "123456");
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
        + " c28    string\n"
        + ") WITH (\n"
        + "  'connector' = 'tidb',\n"
        + "  'tidb.jdbc.database.url' = 'jdbc:mysql://host:port/database',\n"
        + "  'tidb.jdbc.username' = 'root',\n"
        + "  'tidb.jdbc.password' = '123456',\n"
        + "  'tidb.database.name' = 'database',\n"
        + "  'tidb.table.name' = 'test_tidb_type'\n"
        + ")"
    );
    tableEnvironment.executeSql("SELECT * FROM tidb LIMIT 100").print();
  }
```

##### Flink SQL Client

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
 c28    string
) WITH (
  'connector' = 'tidb',
  'tidb.jdbc.database.url' = 'jdbc:mysql://host:port/database',
  'tidb.jdbc.username' = 'root',
  'tidb.jdbc.password' = '123456',
  'tidb.database.name' = 'database',
  'tidb.jdbc.maximum.pool.size' = '10',
  'tidb.jdbc.minimum.idle.size' = '0',
  'tidb.table.name' = 'test_tidb_type'
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
 cast('1' as string)
);
-- set result format
SET execution.result-mode=tableau;
-- query
SELECT * FROM tidb LIMIT 100;
```
