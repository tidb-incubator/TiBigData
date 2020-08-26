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
  ImmutableList<String> primaryKeys1 = ImmutableList.of("id");
  ImmutableList<String> primaryKeys2 = ImmutableList.of("id", "name");

  @Test
  public void testGetCreateTableSql() {
    System.out.println(
        getCreateTableSql(databaseName, tableName, columnNames, columnTypes, null, true));
    System.out.println(
        getCreateTableSql(databaseName, tableName, columnNames, columnTypes, primaryKeys1, true));
    System.out.println(
        getCreateTableSql(databaseName, tableName, columnNames, columnTypes, primaryKeys2, false));
  }

  @Test
  public void testInsertSql() {
    System.out.println(getInsertSql(databaseName, tableName, columnNames));
  }

  @Test
  public void testUpsertSql() {
    System.out.println(getUpsertSql(databaseName, tableName, columnNames, null));
    System.out.println(getUpsertSql(databaseName, tableName, columnNames, primaryKeys1));
    System.out.println(getUpsertSql(databaseName, tableName, columnNames, primaryKeys2));
  }

}
