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

package io.tidb.bigdata.flink.tidb.catalog;

import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.IntegrationTest;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TiDBTableFactoryTest extends FlinkTestBase {

  @Test
  public void testSupportedTableFactory() throws Exception {

    TableEnvironment tableEnvironment = getTableEnvironment();

    tableEnvironment.executeSql(
        "CREATE TABLE `people`(\n"
            + "  `id` INT,\n"
            + "  `name` STRING\n"
            + ") WITH (\n"
            + "  'connector' = 'tidb',\n"
            + "  'tidb.database.url' = 'jdbc:mysql://localhost:4000/',\n"
            + "  'tidb.username' = 'root',\n"
            + "  'tidb.password' = '',\n"
            + "  'tidb.database.name' = 'test',\n"
            + "  'tidb.table.name' = 'people'\n"
            + ")");
  }
}
