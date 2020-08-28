package com.zhihu.tibigdata.tidb;

import static com.zhihu.tibigdata.tidb.SqlUtils.getCreateTableSql;
import static com.zhihu.tibigdata.tidb.SqlUtils.getInsertSql;
import static com.zhihu.tibigdata.tidb.SqlUtils.getUpsertSql;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class TestSqlUtils {

  String databaseName = "default";
  String tableName = "people";
  ImmutableList<String> columnNames = ImmutableList.of("id", "name", "sex");
  ImmutableList<String> columnTypes = ImmutableList.of("int", "varchar(255)", "varchar(255)");
  ImmutableList<String> primaryKeyColumns1 = ImmutableList.of("id");
  ImmutableList<String> primaryKeyColumns2 = ImmutableList.of("id", "name");

  @Test
  public void testGetCreateTableSql() {
    System.out.println(
        getCreateTableSql(databaseName, tableName, columnNames, columnTypes, null, true));
    System.out.println(
        getCreateTableSql(databaseName, tableName, columnNames, columnTypes, primaryKeyColumns1,
            true));
    System.out.println(
        getCreateTableSql(databaseName, tableName, columnNames, columnTypes, primaryKeyColumns2,
            false));
  }

  @Test
  public void testInsertSql() {
    System.out.println(getInsertSql(databaseName, tableName, columnNames));
  }

  @Test
  public void testUpsertSql() {
    System.out.println(getUpsertSql(databaseName, tableName, columnNames, null));
    System.out.println(getUpsertSql(databaseName, tableName, columnNames, primaryKeyColumns1));
    System.out.println(getUpsertSql(databaseName, tableName, columnNames, primaryKeyColumns2));
  }

}
