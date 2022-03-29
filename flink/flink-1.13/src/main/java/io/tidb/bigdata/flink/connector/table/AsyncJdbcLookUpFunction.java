/*
 * Copyright 2021 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.flink.connector.table;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcRowDataLookupFunction;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ly
 */
public class AsyncJdbcLookUpFunction extends AsyncTableFunction<RowData> {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcRowDataLookupFunction.class);
  private static final long serialVersionUID = 2L;

  private final String query;
  private final DataType[] keyTypes;
  private final String[] keyNames;
  private final long cacheMaxSize;
  private final long cacheExpireMs;
  private final int maxPoolSize;
  private final TiDBRowConverter jdbcRowConverter;
  private final JdbcOptions options;
  private final TiDBRowConverter lookupKeyRowConverter;
  private transient JDBCPool pool;
  private transient Cache<RowData, List<RowData>> cache;
  private transient PreparedQuery<RowSet<Row>> preparedQuery;

  public AsyncJdbcLookUpFunction(
      JdbcOptions options,
      AsyncLookupOptions lookupOptions,
      String[] fieldNames,
      DataType[] fieldTypes,
      String[] keyNames,
      RowType rowType) {

    checkNotNull(options, "No JdbcOptions supplied.");
    checkNotNull(fieldNames, "No fieldNames supplied.");
    checkNotNull(fieldTypes, "No fieldTypes supplied.");
    checkNotNull(keyNames, "No keyNames supplied.");
    this.options = options;
    this.keyNames = keyNames;
    List<String> nameList = Arrays.asList(fieldNames);
    this.keyTypes =
        Arrays.stream(keyNames)
            .map(
                s -> {
                  checkArgument(
                      nameList.contains(s),
                      "keyName %s can't find in fieldNames %s.",
                      s,
                      nameList);
                  return fieldTypes[nameList.indexOf(s)];
                })
            .toArray(DataType[]::new);
    this.cacheMaxSize = lookupOptions.getCacheMaxSize();
    this.cacheExpireMs = lookupOptions.getCacheExpireMs();
    this.maxPoolSize = lookupOptions.getMaxPoolSize();
    this.query = JdbcUtils.getSelectFromStatement(options.getTableName(), fieldNames, keyNames);
    this.jdbcRowConverter = new TiDBRowConverter(rowType);
    this.lookupKeyRowConverter =
        new TiDBRowConverter(
            RowType.of(
                Arrays.stream(keyTypes)
                    .map(DataType::getLogicalType)
                    .toArray(LogicalType[]::new)));
  }

  @Override
  public void open(FunctionContext context) {
    String userName = options.getUsername().isPresent() ? options.getUsername().get() : "";
    String passWord = options.getPassword().isPresent() ? options.getPassword().get() : "";
    this.pool = JDBCPool.pool(Vertx.vertx(),
        new JDBCConnectOptions()
            // H2 connection string
            .setJdbcUrl(options.getDbURL())
            // username
            .setUser(userName)
            // password
            .setPassword(passWord),
        // configure the pool
        new PoolOptions().setMaxSize(maxPoolSize)
    );
    this.preparedQuery = this.pool.preparedQuery(query);
  }

  public void eval(CompletableFuture<Collection<RowData>> result, Object... keys)
      throws SQLException {
    RowData rowData = GenericRowData.of(keys);
    // handle the failure
    preparedQuery
        .execute(lookupKeyRowConverter.toExternal(rowData))
        .onFailure(Throwable::printStackTrace)
        .onSuccess(rows -> {
          ArrayList<RowData> rowsList = new ArrayList<>();
          for (Row row : rows) {
            RowData r = null;
            try {
              r = jdbcRowConverter.toInternal(row);
            } catch (SQLException troubles) {
              troubles.printStackTrace();
            }
            rowsList.add(r);
          }
          result.complete(rowsList);
        });

  }

  @Override
  public void close() {
    if (pool != null) {
      pool.close();
    }
  }
}
