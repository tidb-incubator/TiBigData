package com.zhihu.tibigdata.tidb;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class TestSqlUtils {

  @Test
  public void testGetCreateTableSql() {
    String databaseName = "default";
    String tableName = "people";
    ImmutableList<String> columnNames = ImmutableList.of("id", "name", "sex");
    ImmutableList<String> columnTypes = ImmutableList.of("int", "varchar(255)", "varchar(255)");
    System.out.println(
        SqlUtils.getCreateTableSql(databaseName, tableName, columnNames, columnTypes, true));
    System.out.println(
        SqlUtils.getCreateTableSql(databaseName, tableName, columnNames, columnTypes, false));
    System.out.println(SqlUtils.getInsertSql(databaseName, tableName, columnNames));
  }

}
