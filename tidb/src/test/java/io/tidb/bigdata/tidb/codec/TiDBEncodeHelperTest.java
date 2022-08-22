/*
 * Copyright 2022 TiDB Project Authors.
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

package io.tidb.bigdata.tidb.codec;

import static io.tidb.bigdata.tidb.codec.TiDBEncodeHelper.VERSION;

import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.ConfigUtils;
import io.tidb.bigdata.tidb.meta.TiColumnInfo;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import io.tidb.bigdata.tidb.row.Row;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.tikv.common.Snapshot;
import org.tikv.common.StoreVersion;
import org.tikv.common.meta.TiTimestamp;

@Category(IntegrationTest.class)
public class TiDBEncodeHelperTest {

  private String databaseName = "test";
  private ClientSession session;
  private boolean enableNewRowFormat;

  @Before
  public void init() {
    Map<String, String> properties = ConfigUtils.defaultProperties();
    session = ClientSession.create(new ClientConfig(properties));
    if (StoreVersion.minTiKVVersion(VERSION, session.getTiSession().getPDClient())) {
      this.enableNewRowFormat = session.getRowFormatVersion() == 2;
    } else {
      this.enableNewRowFormat = false;
    }
  }

  private void checkRows(String tableName) throws Exception {
    TiTableInfo tiTableInfo = session.getTableMust(databaseName, tableName);
    TiTimestamp timestamp = session.getSnapshotVersion();
    Snapshot snapshot = session.getTiSession().createSnapshot(timestamp);
    List<Row> rows = session.fetchAllRows(databaseName, tableName);
    for (Row row : rows) {
      byte[] key = TiDBEncodeHelper.encodeRowKeyWithNotNullUniqueIndex(snapshot, tiTableInfo, row);
      byte[] value = snapshot.get(key);
      List<TiColumnInfo> columns = tiTableInfo.getColumns();
      Object[] objects = new Object[columns.size()];
      for (int i = 0; i < objects.length; i++) {
        TiColumnInfo tiColumnInfo = columns.get(i);
        Object object = row.get(i, tiColumnInfo.getType());
        objects[i] = object;
      }
      byte[] value1 =
          TableCodec.encodeRow(columns, objects, tiTableInfo.isPkHandle(), enableNewRowFormat);
      Assert.assertArrayEquals(value1, value);
    }
  }

  @Test
  public void testEncodeRowKey() throws Exception {
    // auto increment primary key
    String tableName = "table1";
    String dbTable = String.format("`%s`.`%s`", databaseName, tableName);
    List<String> list = Collections.nCopies(100, "(null,'zs')");
    session.sqlUpdate(
        "DROP TABLE IF EXISTS " + dbTable,
        String.format(
            "CREATE TABLE IF NOT EXISTS %s (`c1` INT PRIMARY KEY AUTO_INCREMENT, `c2` VARCHAR(16) )",
            dbTable),
        String.format("INSERT INTO %s VALUES %s", dbTable, String.join(",", list)));
    checkRows(tableName);

    // primary key without auto increment
    tableName = "table2";
    dbTable = String.format("`%s`.`%s`", databaseName, tableName);
    list =
        IntStream.range(1, 101)
            .mapToObj(i -> String.format("(%s,'zs')", i))
            .collect(Collectors.toList());
    session.sqlUpdate(
        "DROP TABLE IF EXISTS " + dbTable,
        String.format(
            "CREATE TABLE IF NOT EXISTS %s (`c1` INT PRIMARY KEY, `c2` VARCHAR(16) )", dbTable),
        String.format("INSERT INTO %s VALUES %s", dbTable, String.join(",", list)));
    checkRows(tableName);

    // primary key and unique key
    tableName = "table3";
    dbTable = String.format("`%s`.`%s`", databaseName, tableName);
    list =
        IntStream.range(1, 101)
            .mapToObj(i -> String.format("(%s,'zs',%s)", i, i))
            .collect(Collectors.toList());
    session.sqlUpdate(
        "DROP TABLE IF EXISTS " + dbTable,
        String.format(
            "CREATE TABLE IF NOT EXISTS %s (`c1` INT PRIMARY KEY, `c2` VARCHAR(16),`c3` INT UNIQUE KEY)",
            dbTable),
        String.format("INSERT INTO %s VALUES %s", dbTable, String.join(",", list)));
    checkRows(tableName);

    // unique key
    tableName = "table4";
    dbTable = String.format("`%s`.`%s`", databaseName, tableName);
    list =
        IntStream.range(1, 101)
            .mapToObj(i -> String.format("(%s,'zs')", i))
            .collect(Collectors.toList());
    session.sqlUpdate(
        "DROP TABLE IF EXISTS " + dbTable,
        String.format(
            "CREATE TABLE IF NOT EXISTS %s (`c1` INT UNIQUE KEY NOT NULL, `c2` VARCHAR(16))",
            dbTable),
        String.format("INSERT INTO %s VALUES %s", dbTable, String.join(",", list)));
    checkRows(tableName);

    // string primary key
    tableName = "table5";
    dbTable = String.format("`%s`.`%s`", databaseName, tableName);
    list =
        IntStream.range(1, 101)
            .mapToObj(i -> String.format("('%s','zs')", UUID.randomUUID()))
            .collect(Collectors.toList());
    session.sqlUpdate(
        "DROP TABLE IF EXISTS " + dbTable,
        String.format(
            "CREATE TABLE IF NOT EXISTS %s (`c1` VARCHAR(255) UNIQUE KEY NOT NULL, `c2` VARCHAR(16))",
            dbTable),
        String.format("INSERT INTO %s VALUES %s", dbTable, String.join(",", list)));
    checkRows(tableName);
  }
}
