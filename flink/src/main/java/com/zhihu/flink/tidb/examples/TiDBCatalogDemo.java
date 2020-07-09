package com.zhihu.flink.tidb.examples;

import com.zhihu.flink.tidb.catalog.TiDBCatalog;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

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
