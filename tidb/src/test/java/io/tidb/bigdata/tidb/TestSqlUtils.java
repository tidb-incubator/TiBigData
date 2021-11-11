package io.tidb.bigdata.tidb;

import static io.tidb.bigdata.tidb.SqlUtils.getCreateTableSql;
import static io.tidb.bigdata.tidb.SqlUtils.getInsertSql;
import static io.tidb.bigdata.tidb.SqlUtils.getUpsertSql;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

public class TestSqlUtils {

  ClientSession session = ClientSession.createWithSingleConnection(
      new ClientConfig(ConfigUtils.getProperties()));

  String databaseName = "default_database";
  String tableName = "default_table";
  ImmutableList<String> columnNames = ImmutableList.of("c1", "c2", "c3", "c4", "c5", "c6");
  ImmutableList<String> columnTypes = ImmutableList
      .of("int", "bigint", "varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)");
  ImmutableList<String> primaryKeyColumns = ImmutableList.of("c1", "c2");
  ImmutableList<String> unqiueKeyColumns = ImmutableList.of("c3", "c4");

  @Test
  public void testGetCreateTableSql() {
    Assert.assertEquals("CREATE TABLE IF NOT EXISTS `default_database`.`default_table`(\n"
            + "`c1` int,\n"
            + "`c2` bigint,\n"
            + "`c3` varchar(255),\n"
            + "`c4` varchar(255),\n"
            + "`c5` varchar(255),\n"
            + "`c6` varchar(255),\n"
            + "PRIMARY KEY(`c1`,`c2`),\n"
            + "UNIQUE KEY(`c3`,`c4`)\n"
            + ")",
        getCreateTableSql(databaseName, tableName, columnNames, columnTypes, primaryKeyColumns,
            unqiueKeyColumns, true));
    Assert.assertEquals("CREATE TABLE  `default_database`.`default_table`(\n"
            + "`c1` int,\n"
            + "`c2` bigint,\n"
            + "`c3` varchar(255),\n"
            + "`c4` varchar(255),\n"
            + "`c5` varchar(255),\n"
            + "`c6` varchar(255),\n"
            + "PRIMARY KEY(`c1`,`c2`),\n"
            + "UNIQUE KEY(`c3`,`c4`)\n"
            + ")",
        getCreateTableSql(databaseName, tableName, columnNames, columnTypes, primaryKeyColumns,
            unqiueKeyColumns, false));
    Assert.assertEquals("CREATE TABLE  `default_database`.`default_table`(\n"
            + "`c1` int,\n"
            + "`c2` bigint,\n"
            + "`c3` varchar(255),\n"
            + "`c4` varchar(255),\n"
            + "`c5` varchar(255),\n"
            + "`c6` varchar(255),\n"
            + "UNIQUE KEY(`c3`,`c4`)\n"
            + ")",
        getCreateTableSql(databaseName, tableName, columnNames, columnTypes, null,
            unqiueKeyColumns, false));
    Assert.assertEquals("CREATE TABLE  `default_database`.`default_table`(\n"
            + "`c1` int,\n"
            + "`c2` bigint,\n"
            + "`c3` varchar(255),\n"
            + "`c4` varchar(255),\n"
            + "`c5` varchar(255),\n"
            + "`c6` varchar(255),\n"
            + "PRIMARY KEY(`c1`,`c2`)\n"
            + ")",
        getCreateTableSql(databaseName, tableName, columnNames, columnTypes, primaryKeyColumns,
            null, false));
    Assert.assertEquals("CREATE TABLE  `default_database`.`default_table`(\n"
            + "`c1` int,\n"
            + "`c2` bigint,\n"
            + "`c3` varchar(255),\n"
            + "`c4` varchar(255),\n"
            + "`c5` varchar(255),\n"
            + "`c6` varchar(255)\n"
            + ")",
        getCreateTableSql(databaseName, tableName, columnNames, columnTypes, null,
            null, false));
  }

  @Test
  public void testInsertSql() {
    Assert.assertEquals(
        "INSERT INTO `default_database`.`default_table`(`c1`,`c2`,`c3`,`c4`,`c5`,`c6`) VALUES(?,?,?,?,?,?)",
        getInsertSql(databaseName, tableName, columnNames));
  }

  @Test
  public void testUpsertSql() {
    Assert.assertEquals(
        "INSERT INTO `default_database`.`default_table`(`c1`,`c2`,`c3`,`c4`,`c5`,`c6`) VALUES(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE `c1`=VALUES(`c1`),`c2`=VALUES(`c2`),`c3`=VALUES(`c3`),`c4`=VALUES(`c4`),`c5`=VALUES(`c5`),`c6`=VALUES(`c6`)",
        getUpsertSql(databaseName, tableName, columnNames));
  }

  @Test
  public void testPrintColumnMapping() {
    String[] columns1 = {"c1", "c2", "c3", "c4"};
    String[] columns2 = {"c1", "c2", "c3"};
    Assert.assertEquals("`c1` -> `c1`,\n"
        + "`c2` -> `c2`,\n"
        + "`c3` -> `c3`,\n"
        + "`c4` -> `  `", SqlUtils.printColumnMapping(columns1, columns2));
    Assert.assertEquals("`c1` -> `c1`,\n"
        + "`c2` -> `c2`,\n"
        + "`c3` -> `c3`,\n"
        + "`  ` -> `c4`", SqlUtils.printColumnMapping(columns2, columns1));
    Assert.assertEquals("`c1` -> `c1`,\n"
        + "`c2` -> `c2`,\n"
        + "`c3` -> `c3`,\n"
        + "`c4` -> `c4`", SqlUtils.printColumnMapping(columns1, columns1));
  }

}
