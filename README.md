# TiBigData
[License](https://github.com/pingcap-incubator/TiBigData/blob/master/LICENSE)

Misc BigData components for TiDB, Presto & Flink connectors for example.

## License
TiBigData project is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Flink-TiDB-Connector

### Usage 

#### Use TiDBCatalog
```java
public class TestTiDBCatalog{
  public static void main(String[] args) throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
    // register TiDBCatalog
    String pdAddresses="host1:port1,host2:port2,host3:port3";
    TiDBCatalog catalog = new TiDBCatalog("tidb", pdAddresses);
    catalog.open();
    tEnv.registerCatalog("tidb", catalog);
    // query and print
    tEnv.useCatalog("tidb");
    tEnv.useDatabase("default");
    Table table = tEnv.sqlQuery("SELECT * FROM `tidb`.`default`.`tableName`");
    table.printSchema();
    tEnv.toAppendStream(table, Row.class).print();
    // execute
    tEnv.execute("Test TiDB Catalog");
  }
}
```

#### Use TiDBTableSource
```java
public class TestTiDBTableSource {
  public static void main(String[] args) throws Exception {
    String pdAddresses = "your pdAddresses";
    String databaseName = "flink";
    String tableName = "people";
    String[] fieldNames = {"id", "name", "sex"};
    DataType[] fieldTypes = {DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()};
    // get env
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
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
    Table table = tEnv.fromTableSource(tableSource);
    tEnv.createTemporaryView("tidb", table);
    // query and print
    Table resTable = tEnv.sqlQuery("SELECT * FROM tidb");
    resTable.printSchema();
    DataStream<Row> rowDataStream = tEnv.toAppendStream(resTable, Row.class);
    rowDataStream.print();
    // execute
    tEnv.execute("Test TiDB TableSource");
  }
}
```

#### DataTypes
|      TiDB DataType      |  Flink Optianl DataType(TiDBTableSource)|   Flink Default DataType(TiDBCatalog)   |
| :---------------------: | :-------------------------------------: | :-------------------------------------: |
|        SMALLINT         |           DataTypes.BIGINT()            |           DataTypes.BIGINT()            |
|        MEDIUMINT        |           DataTypes.BIGINT()            |           DataTypes.BIGINT()            |
|           INT           |           DataTypes.BIGINT()            |           DataTypes.BIGINT()            |
|         BIGINT          |           DataTypes.BIGINT()            |           DataTypes.BIGINT()            |
|         TINYINT         |           DataTypes.BIGINT()            |           DataTypes.BIGINT()            |
|         VARCHAR         |           DataTypes.VARCHAR()           |           DataTypes.VARCHAR()           |
|          TEXT           |           DataTypes.STRING()            |           DataTypes.STRING()            |
|          DATE           |            DataTypes.DATE()             |            DataTypes.DATE()             |
|          FLOAT          |            DataTypes.FLOAT()            |            DataTypes.FLOAT()            |
|         DOUBLE          |           DataTypes.DOUBLE()            |           DataTypes.DOUBLE()            |
|         DECIMAL         |           DataTypes.DECIMAL()           |           DataTypes.DECIMAL()           |
|        DATETIME         |          DataTypes.TIMESTAMP()          |          DataTypes.TIMESTAMP()          |
|        TIMESTAMP        |          DataTypes.TIMESTAMP()          |          DataTypes.TIMESTAMP()          |
|          TIME           |            DataTypes.TIME()             |            DataTypes.TIME()             |
|          YEAR           |           DataTypes.BIGINT()            |           DataTypes.BIGINT()            |
|          CHAR           |            DataTypes.CHAR()             |            DataTypes.CHAR()             |
|        TINYBLOB         | DataTypes.STRING() or DataTypes.BYTES() |            DataTypes.STRING()			  |
|        TINYTEXT         |           DataTypes.STRING()            |           DataTypes.STRING()            |
|          BLOB           | DataTypes.STRING() or DataTypes.BYTES() | 			DataTypes.STRING() 			  |
|       MEDIUMBLOB        | DataTypes.STRING() or DataTypes.BYTES() | 			DataTypes.STRING() 		  	  |
|       MEDIUMTEXT        | DataTypes.STRING() or DataTypes.BYTES() | 			DataTypes.STRING() 			  |
|        LONGBLOB         | DataTypes.STRING() or DataTypes.BYTES() | 			DataTypes.STRING()		 	  |
|        LONGTEXT         |           DataTypes.STRING()            |           DataTypes.STRING()            |
| ENUM(data1,data2,data3) |           DataTypes.STRING()            |           DataTypes.STRING()            |
|          BOOL           |           DataTypes.BOOLEAN()           |           DataTypes.BOOLEAN()           |
|         BINARY          |           DataTypes.BINARY()            |           DataTypes.BINARY()            |
|        VARBINARY        |          DataTypes.VARBINARY()          |          DataTypes.VARBINARY()          |
|          JSON           |           DataTypes.STRING()            |           DataTypes.STRING()            |

