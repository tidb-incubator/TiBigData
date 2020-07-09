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

The output can be found in taskexecutor logs, check your flink log directory for log files with this pattern:
```flink-${username}-taskexecutor-1-${host}.out```.

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
    TiDBCatalog catalog = new TiDBCatalog(pdAddresses);
    catalog.open();
    tableEnvironment.registerCatalog("tidb", catalog);
    tableEnvironment.useCatalog("tidb");
    // query and print
    String sql = String.format("SELECT * FROM `%s`.`%s`", databaseName, tableName);
    Table table = tableEnvironment.sqlQuery(sql);
    table.printSchema();
    tableEnvironment.toAppendStream(table, Row.class).print();
    // execute
    tableEnvironment.execute("Test TiDB Catalog");
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
    Table table = tableEnvironment.fromTableSource(tableSource);
    tableEnvironment.createTemporaryView("tidb", table);
    // query and print
    Table resTable = tableEnvironment.sqlQuery("SELECT * FROM tidb");
    resTable.printSchema();
    DataStream<Row> rowDataStream = tableEnvironment.toAppendStream(resTable, Row.class);
    rowDataStream.print();
    // execute
    tableEnvironment.execute("Test TiDB TableSource");
  }
}
```



## TODO

1. Upgrade flink version to 1.11;
2. Support write data in Presto-TiDB-Connector;
3. TiDBTableSink.