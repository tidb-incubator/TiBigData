package com.zhihu.presto.tidb;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class TestClientSession {

  @Test
  public void testGetCreateTableSql() {
    String databaseName = "default";
    String tableName = "people";
    ImmutableList<String> columnNames = ImmutableList.of("id", "name", "sex");
    ImmutableList<String> columnTypes = ImmutableList.of("int", "varchar(255)", "varchar(255)");
    System.out.println(
        ClientSession.getCreateTableSql(databaseName, tableName, columnNames, columnTypes, true));
    System.out.println(
        ClientSession.getCreateTableSql(databaseName, tableName, columnNames, columnTypes, false));

  }

}
