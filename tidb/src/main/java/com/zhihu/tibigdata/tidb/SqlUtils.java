package com.zhihu.tibigdata.tidb;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.nCopies;

import java.util.List;
import java.util.stream.Collectors;

public class SqlUtils {

  public static String getCreateTableSql(String databaseName, String tableName,
      List<String> columnNames, List<String> columnTypes, boolean ignoreIfExists) {
    StringBuilder stringBuilder = new StringBuilder(
        format("CREATE TABLE %s `%s`.`%s`(\n", ignoreIfExists ? "IF NOT EXISTS" : "",
            databaseName, tableName));
    for (int i = 0; i < columnNames.size(); i++) {
      stringBuilder
          .append("`")
          .append(columnNames.get(i))
          .append("`")
          .append(" ")
          .append(columnTypes.get(i));
      if (i < columnNames.size() - 1) {
        stringBuilder.append(",");
      }
      stringBuilder.append("\n");
    }
    stringBuilder.append(")");
    return stringBuilder.toString();
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

}
