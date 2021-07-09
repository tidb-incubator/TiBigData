package io.tidb.bigdata.flink.connector.table;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.table.JdbcRowDataLookupFunction;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.LookupTableSource.LookupContext;
import org.apache.flink.table.connector.source.LookupTableSource.LookupRuntimeProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

public class LookupTableSourceHelper {
  private final JdbcLookupOptions lookupOptions;

  public LookupTableSourceHelper(
      JdbcLookupOptions lookupOptions) {
    this.lookupOptions = lookupOptions;
  }

  public LookupRuntimeProvider getLookupRuntimeProvider(
      ResolvedCatalogTable table, LookupContext context) {
    String[] keyNames = new String[context.getKeys().length];
    ResolvedSchema schema = table.getResolvedSchema();
    Column[] columns = schema.getColumns().stream()
        .filter(Column::isPhysical).toArray(Column[]::new);
    Map<String, String> properties = table.getOptions();
    for (int i = 0; i < keyNames.length; i++) {
      int[] innerKeyArr = context.getKeys()[i];
      Preconditions.checkArgument(
          innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
      keyNames[i] = columns[innerKeyArr[0]].getName();
    }
    final RowType rowType = (RowType) schema.toSourceRowDataType().getLogicalType();

    return TableFunctionProvider.of(
        new JdbcRowDataLookupFunction(
            JdbcUtils.getJdbcOptions(properties),
            lookupOptions,
            Arrays.stream(columns).map(Column::getName).toArray(String[]::new),
            Arrays.stream(columns).map(Column::getDataType).toArray(
                org.apache.flink.table.types.DataType[]::new),
            keyNames,
            rowType));
  }
}