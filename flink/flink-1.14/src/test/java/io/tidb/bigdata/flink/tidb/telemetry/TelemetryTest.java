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

package io.tidb.bigdata.flink.tidb.telemetry;

import static java.lang.String.format;

import io.tidb.bigdata.flink.telemetry.FlinkTeleMsg;
import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.ConfigUtils;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.TableUtils;
import java.util.Map;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TelemetryTest extends FlinkTestBase {

  @Test
  public void testTelemetry() throws InterruptedException {
    Map<String, String> properties = ConfigUtils.defaultProperties();
    properties.put("type", "tidb");
    properties.put("tidb.telemetry.enable", "false");
    TableEnvironment tableEnvironment = getBatchTableEnvironment();
    Assert.assertThrows(NullPointerException.class, FlinkTeleMsg::getInstance);
    String createCatalogSql =
        format("CREATE CATALOG `tidb` WITH ( %s )", TableUtils.toSqlProperties(properties));
    tableEnvironment.executeSql(createCatalogSql);
    String showDatabases = String.format("SHOW DATABASES");
    tableEnvironment.executeSql(showDatabases);
    Thread.sleep(1000);
    Assert.assertThrows(NullPointerException.class, FlinkTeleMsg::getInstance);
    properties.put("tidb.telemetry.enable", "true");
    createCatalogSql =
        format("CREATE CATALOG `tidb2` WITH ( %s )", TableUtils.toSqlProperties(properties));
    tableEnvironment.executeSql(createCatalogSql);
    tableEnvironment.executeSql(showDatabases);
    Thread.sleep(1000);
    Assert.assertEquals(false, FlinkTeleMsg.getInstance().shouldSendMsg());
  }
}
