package com.zhihu.tibigdata.tidb;

import static com.zhihu.tibigdata.tidb.SqlUtils.getCreateTableSql;
import static com.zhihu.tibigdata.tidb.SqlUtils.getInsertSql;
import static com.zhihu.tibigdata.tidb.SqlUtils.getUpsertSql;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class TestSqlUtils {

  String databaseName = "default_database";
  String tableName = "default_table";
  ImmutableList<String> columnNames = ImmutableList.of("c1", "c2", "c3", "c4", "c5", "c6");
  ImmutableList<String> columnTypes = ImmutableList
      .of("int", "bigint", "varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)");
  ImmutableList<String> primaryKeyColumns = ImmutableList.of("c1", "c2");
  ImmutableList<String> unqiueKeyColumns = ImmutableList.of("c3", "c4");

  @Test
  public void testGetCreateTableSql() {
    System.out.println(
        getCreateTableSql(databaseName, tableName, columnNames, columnTypes, primaryKeyColumns,
            unqiueKeyColumns, true));
    System.out.println(
        getCreateTableSql(databaseName, tableName, columnNames, columnTypes, primaryKeyColumns,
            unqiueKeyColumns, false));
    System.out.println(
        getCreateTableSql(databaseName, tableName, columnNames, columnTypes, null,
            unqiueKeyColumns, false));
    System.out.println(
        getCreateTableSql(databaseName, tableName, columnNames, columnTypes, primaryKeyColumns,
            null, false));
    System.out.println(
        getCreateTableSql(databaseName, tableName, columnNames, columnTypes, null,
            null, false));
  }

  @Test
  public void testInsertSql() {
    System.out.println(getInsertSql(databaseName, tableName, columnNames));
  }

  @Test
  public void testUpsertSql() {
    System.out.println(getUpsertSql(databaseName, tableName, columnNames));
  }

}
