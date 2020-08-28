package com.zhihu.tibigdata.tidb;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.nCopies;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SqlUtils {

  public static final String QUERY_PD_SQL =
      "SELECT `INSTANCE` FROM `INFORMATION_SCHEMA`.`CLUSTER_INFO` WHERE `TYPE` = 'pd'";

  private static List<String> concatNameType(List<String> columnNames, List<String> columnTypes,
      List<String> primaryKeyColumns) {
    List<String> nameType = new ArrayList<>();
    for (int i = 0; i < columnNames.size(); i++) {
      nameType.add(format("`%s` %s", columnNames.get(i), columnTypes.get(i)));
    }
    if (primaryKeyColumns != null && primaryKeyColumns.size() != 0) {
      nameType.add(format("PRIMARY KEY(%s)",
          primaryKeyColumns.stream().map(pk -> "`" + pk + "`").collect(Collectors.joining(","))));
    }
    return nameType;
  }


  public static String getCreateTableSql(String databaseName, String tableName,
      List<String> columnNames, List<String> columnTypes, List<String> primaryKeyColumns,
      boolean ignoreIfExists) {
    return format("CREATE TABLE %s `%s`.`%s`(\n%s\n)",
        ignoreIfExists ? "IF NOT EXISTS" : "",
        databaseName,
        tableName,
        join(",\n", concatNameType(columnNames, columnTypes, primaryKeyColumns))
    );
  }

  public static String getInsertSql(String databaseName, String tableName,
      List<String> columnNames) {
    return format(
        "INSERT INTO `%s`.`%s`(%s) VALUES(%s) ",
        databaseName,
        tableName,
        columnNames.stream().map(name -> format("`%s`", name)).collect(Collectors.joining(",")),
        join(",", nCopies(columnNames.size(), "?"))
    );
  }

  public static String getUpsertSql(String databaseName, String tableName,
      List<String> columnNames, List<String> primaryKeyColumns) {
    String insertSql = getInsertSql(databaseName, tableName, columnNames);
    if (primaryKeyColumns == null || primaryKeyColumns.size() == 0) {
      return insertSql;
    }
    return format("%s ON DUPLICATE KEY UPDATE %s", insertSql,
        columnNames.stream().filter(name -> !primaryKeyColumns.contains(name))
            .map(pk -> format("`%s`=VALUES(`%s`)", pk, pk))
            .collect(Collectors.joining(",")));
  }

}
