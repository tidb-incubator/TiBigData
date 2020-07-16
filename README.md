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
mvn clean package
```

### Presto-TiDB-Connector

#### Install TiDB Plugin

##### prestodb

```bash
tar -zxf prestodb/target/prestodb-connector-0.0.1-SNAPSHOT-plugin.tar.gz -C prestodb/target
cp -r prestodb/target/prestodb-connector-0.0.1-SNAPSHOT/tidb ${PRESTO_HOME}/plugin
```

##### prostosql

```bash
tar -zxf prestosql/target/prestosql-connector-0.0.1-SNAPSHOT-plugin.tar.gz -C prestosql/target
cp -r prestosql/target/prestosql-connector-0.0.1-SNAPSHOT/tidb ${PRESTO_HOME}/plugin
```

#### Configuration

```bash
vim ${PRESTO_HOME}/etc/catalog/tidb.properties
```

The file `tidb.properties` like :

```properties
# connector name
connector.name=tidb
# your tidb pd addresses
presto.tidb.pd.addresses=host1:port1,host2:port2,host3:port3
```

then restart your presto cluster and use presto-cli to connect presto coordinator:

```bash
presto-cli --server ${COORDINATOR_HOST}:${PORT} --catalog tidb --schema ${TIDB_DATABASE} --user ${USERNAME}
SHOW TABLES;
SELECT * FROM ${TABLE_NAME} LIMIT 100;
```

### Flink-TiDB-Connector

```bash
cp flink/target/flink-tidb-connector-0.0.1-SNAPSHOT.jar ${FLINK_HOME}/lib
bin/flink run -c com.zhihu.flink.tidb.examples.TiDBCatalogDemo lib/flink-tidb-connector-0.0.1-SNAPSHOT.jar --pd.addresses host1:port1,host2:port2,host3:port3 --database.name ${TIDB_DATABASE} --table.name ${TABLE_NAME}
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
  
  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    final String pdAddresses = parameterTool.getRequired("pd.addresses");
    final String tableName = parameterTool.getRequired("table.name");
    final String databaseName = parameterTool.getRequired("database.name");
    // env
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    // register TiDBCatalog
    Properties properties = new Properties();
    properties.setProperty("pd.addresses", pdAddresses);
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
    properties.setProperty("pd.addresses", "host1:port1,host2:port2,host3:port3");
    // optional properties
    properties.setProperty("database.url", "jdbc:mysql://host:port/database");
    properties.setProperty("username", "root");
    properties.setProperty("password", "123456");
    TiDBCatalog catalog = new TiDBCatalog(properties);
    catalog.open();
    String sql = "CREATE TABLE IF NOT EXISTS people(id INT, name VARCHAR(255), sex ENUM('1','2'))";
    catalog.sqlUpdate(sql);
    catalog.close();
  }
}
```

it is implemented by `mysql-connector-java`.



 #### TiDBTableSource

If you want to specify the type by yourself, please use TiDBTableSource. It supports most type conversions, such as INT to BIGINT, STRING to LONG, LONG to BYTES.

```java
public class TestTiDBTableSource {

  public static void main(String[] args) throws Exception {
    final String pdAddresses = "your pdAddresses";
    final String databaseName = "flink";
    final String tableName = "people";
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
    TiDBTableSource tableSource = TiDBTableSource.builder()
        .setPdAddresses(pdAddresses)
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setTableSchema(tableSchema)
        .build();
    // register TiDB table
    tableEnvironment.registerTableSource("tidb", tableSource);
    // query and print
    tableEnvironment.executeSql("SELECT * FROM tidb LIMIT 100").print();
  }
}
```



## TODO

1. Support write data in Presto-TiDB-Connector;
2. TiDBTableSink.