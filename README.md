# TiBigData
[License](https://github.com/zhihu/presto-connectors/blob/master/LICENSE)

## Presto Connectors
Misc connectors for Presto. TiDB is the only implemented connector at this time.

## Flink Connectors
### flink-tidb-connector
#### Usage
```java
public class TestTiDBTableSource {
  public static void main(String[] args) throws Exception {
    String pdAddresses = "your pdAddresses";
    String databaseName = "flink";
    String tableName = "people";
    String[] fieldNames = {"id", "name", "sex"};
    DataType[] fieldTypes = {DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()};
    
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
    // build TableSchema
    TableSchema tableSchema = TableSchema.builder().fields(fieldNames, fieldTypes).build();
    // build TiDBTableSource
    TiDBTableSource tiDBTableSource = TiDBTableSource.builder()
        .setPdAddresses(pdAddresses)
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .setTableSchema(tableSchema)
        .build();
    // register TiDB table
    Table table = tEnv.fromTableSource(tiDBTableSource);
    tEnv.createTemporaryView("tidb", table);
    // query and print
    Table resTable = tEnv.sqlQuery("SELECT * FROM tidb");
    resTable.printSchema();
    DataStream<Row> rowDataStream = tEnv.toAppendStream(resTable, Row.class);
    rowDataStream.print();

    tEnv.execute("Test TiDBTableSource");
  }
}
```
#### DataTypes
|      TiDB DataType      |             Flink DataType              |
| :---------------------: | :-------------------------------------: |
|        SMALLINT         |           DataTypes.BIGINT()            |
|        MEDIUMINT        |           DataTypes.BIGINT()            |
|           INT           |           DataTypes.BIGINT()            |
|         BIGINT          |           DataTypes.BIGINT()            |
|         TINYINT         |           DataTypes.BIGINT()            |
|         VARCHAR         |           DataTypes.VARCHAR()           |
|          TEXT           |           DataTypes.STRING()            |
|          DATE           |            DataTypes.DATE()             |
|          FLOAT          |            DataTypes.FLOAT()            |
|         DOUBLE          |           DataTypes.DOUBLE()            |
|         DECIMAL         |           DataTypes.DECIMAL()           |
|        DATETIME         |          DataTypes.TIMESTAMP()          |
|        TIMESTAMP        |          DataTypes.TIMESTAMP()          |
|          TIME           |            DataTypes.TIME()             |
|          YEAR           |           DataTypes.BIGINT()            |
|          CHAR           |            DataTypes.CHAR()             |
|        TINYBLOB         | DataTypes.STRING() or DataTypes.BYTES() |
|        TINYTEXT         |           DataTypes.STRING()            |
|          BLOB           | DataTypes.STRING() or DataTypes.BYTES() |
|       MEDIUMBLOB        | DataTypes.STRING() or DataTypes.BYTES() |
|       MEDIUMTEXT        | DataTypes.STRING() or DataTypes.BYTES() |
|        LONGBLOB         | DataTypes.STRING() or DataTypes.BYTES() |
|        LONGTEXT         |           DataTypes.STRING()            |
| ENUM(data1,data2,data3) |              same as data*              |
|          BOOL           |           DataTypes.BOOLEAN()           |
|         BINARY          |           DataTypes.BINARY()            |
|        VARBINARY        |          DataTypes.VARBINARY()          |
|          JSON           |           DataTypes.STRING()            |


## License
TiBigData project is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
