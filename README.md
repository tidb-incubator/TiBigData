# TiBigData

[License](https://github.com/pingcap-incubator/TiBigData/blob/master/LICENSE)

Misc BigData components for TiDB, Presto & Flink connectors for example.

## License

TiBigData project is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Getting Started

### Build

```bash
git clone git@github.com:pingcap-incubator/TiBigData.git
cd TiBigData
mvn clean package -DskipTests
```

#### Configuration

| Configration                | Default Value | Description                                                  |
| --------------------------- | ------------- | ------------------------------------------------------------ |
| tidb.jdbc.database.url      | -             | TiDB connector has a built-in JDBC connection pool implemented by [HikariCP](https://github.com/brettwooldridge/HikariCP), you should provide  your own TiDB server address with a jdbc url format: `jdbc:mysql://host:port/database`. |
| tidb.jdbc.username          | -             | JDBC username                                                |
| tidb.jdbc.password          | null          | JDBC password                                                |
| tidb.jdbc.maximum.pool.size | 10            | connection pool size                                         |
| tidb.jdbc.minimum.idle.size | 0             | the minimum number of idle connections that HikariCP tries to maintain in the pool. |

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



#### Examples

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
-- synchronize tidb table to other catalogs
CREATE TABLE ${CATALOG}.${DATABASE}.${TABLE} AS SELECT * FROM ${CATALOG}.${DATABASE}.${TABLE};
CREATE TABLE hive.default.people AS SELECT * FROM tidb.default.people;
```

### Flink-TiDB-Connector

```bash
cp flink/target/flink-tidb-connector-0.0.1-SNAPSHOT.jar ${FLINK_HOME}/lib
bin/flink run -c com.zhihu.flink.tidb.examples.TiDBCatalogDemo lib/flink-tidb-connector-0.0.1-SNAPSHOT.jar --tidb.jdbc.database.url ${DATABASE_URL} --tidb.jdbc.username ${USERNAME} --tidb.jdbc.password ${PASSWORD} --tidb.database.name ${TIDB_DATABASE} --tidb.table.name ${TABLE_NAME}
```

The output can be found in console:

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
    final Properties properties = parameterTool.getProperties();
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
    tableEnvironment.executeSql(sql).print();
  }
}
```

The DataType Mapping of TiDBCatalog  is:

| TiDB DataType |    Flink DataType     |
| :-----------: | :-------------------: |
|   SMALLINT    |  DataTypes.BIGINT()   |
|   MEDIUMINT   |  DataTypes.BIGINT()   |
|      INT      |  DataTypes.BIGINT()   |
|    BIGINT     |  DataTypes.BIGINT()   |
|    TINYINT    |  DataTypes.BIGINT()   |
|    VARCHAR    |  DataTypes.STRING()   |
|     TEXT      |  DataTypes.STRING()   |
|     DATE      |   DataTypes.DATE()    |
|     FLOAT     |   DataTypes.FLOAT()   |
|    DOUBLE     |  DataTypes.DOUBLE()   |
|    DECIMAL    |  DataTypes.DECIMAL()  |
|   DATETIME    | DataTypes.TIMESTAMP() |
|   TIMESTAMP   | DataTypes.TIMESTAMP() |
|     TIME      |   DataTypes.TIME()    |
|     YEAR      |  DataTypes.BIGINT()   |
|     CHAR      |  DataTypes.STRING()   |
|   TINYBLOB    |   DataTypes.BYTES()   |
|   TINYTEXT    |  DataTypes.STRING()   |
|     BLOB      |   DataTypes.BYTES()   |
|  MEDIUMBLOB   |   DataTypes.BYTES()   |
|  MEDIUMTEXT   |  DataTypes.STRING()   |
|   LONGBLOB    |   DataTypes.BYTES()   |
|   LONGTEXT    |  DataTypes.STRING()   |
|     ENUM      |  DataTypes.STRING()   |
|     BOOL      |  DataTypes.BOOLEAN()  |
|    BINARY     |   DataTypes.BYTES()   |
|   VARBINARY   |   DataTypes.BYTES()   |
|     JSON      |  DataTypes.STRING()   |

If you set properties of TiDB server, you could submit DDL  by TiDBCatalog, such as create table, drop table: 

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

it is implemented by `mysql-connector-java`. If you want to specify the type by yourself, please use `TiDBTableSource` or `Flink SQL`. It supports most type conversions, such as INT to BIGINT, STRING to LONG, LONG to BYTES.

#### TiDBTableSource

```java
public class TestTiDBTableSource {

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty("tidb.jdbc.database.url", "jdbc:mysql://host:port/database");
    properties.setProperty("tidb.jdbc.username", "username");
    properties.setProperty("tidb.jdbc.password", "password");
    properties.setProperty("tidb.jdbc.maximum.pool.size", "10");
    properties.setProperty("tidb.jdbc.minimum.idle.size", "0");
    properties.setProperty("tidb.database.name", "database");
    properties.setProperty("tidb.table.name", "table");
    final String[] fieldNames = {"id", "name", "sex"};
    final DataType[] fieldTypes = {DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()};
    // get env
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    // build TableSchema
    TableSchema tableSchema = TableSchema.builder().fields(fieldNames, fieldTypes).build();
    // build TiDBTableSource
    TiDBTableSource tableSource = new TiDBTableSource(tableSchema, properties);
    // register TiDB table
    tableEnvironment.registerTableSource("tidb", tableSource);
    // query and print
    TableResult tableResult = tableEnvironment.executeSql("SELECT * FROM tidb LIMIT 100");
    tableResult.print();
  }
}
```

#### Flink SQL

##### Sample Data

You could create a sample tidb table which contains most tidb types by the following script.

```sql
CREATE TABLE test_tidb_type(
 c1     tinyint(4),
 c2     smallint(6),
 c3     mediumint(9),
 c4     int(11),
 c5     bigint(20),
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
 c25    year(4),
 c26    boolean,
 c27    json,
 c28    enum('1','2','3')
);

INSERT INTO test_tidb_type VALUES
(1,2,3,4,5,'chartype','varchartype','tinytexttype','mediumtexttype','texttype','longtexttype','binarytype','varbinarytype','tinyblobtype','mediumblobtype','blobtype','longblobtype',1.234,2.456789,123.456,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,2020,true,'{"a":1,"b":2}','1'),
(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
```

##### Flink SQL Programming

```java
public class TestFlinkS {

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
        + " c12    BYTES,\n"
        + " c13    BYTES,\n"
        + " c14    BYTES,\n"
        + " c15    BYTES,\n"
        + " c16    BYTES,\n"
        + " c17    BYTES,\n"
        + " c18    float,\n"
        + " c19    double,\n"
        + " c20    decimal(6,3),\n"
        + " c21    date,\n"
        + " c22    time,\n"
        + " c23    timestamp,\n"
        + " c24    timestamp,\n"
        + " c25    int,\n"
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

```shell
# run flink sql client
bin/sql-client.sh embedded
# create a flink table mapping to tidb table
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
 c12    BYTES,
 c13    BYTES,
 c14    BYTES,
 c15    BYTES,
 c16    BYTES,
 c17    BYTES,
 c18    float,
 c19    double,
 c20    decimal(6,3),
 c21    date,
 c22    time,
 c23    timestamp,
 c24    timestamp,
 c25    int,
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
# set result format
SET execution.result-mode=tableau;
# query
SELECT * FROM tidb LIMIT 100;
```



## TODO

1. Support writing data in Presto-TiDB-Connector;
2. TiDBTableSink.