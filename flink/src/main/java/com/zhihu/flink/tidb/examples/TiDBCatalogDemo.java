package com.zhihu.flink.tidb.examples;

import com.zhihu.flink.tidb.catalog.TiDBCatalog;
import java.util.Properties;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TiDBCatalogDemo {

  public static void main(String[] args) {
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
