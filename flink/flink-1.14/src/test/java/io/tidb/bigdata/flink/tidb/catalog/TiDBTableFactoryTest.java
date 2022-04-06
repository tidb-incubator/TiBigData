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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;

import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.IntegrationTest;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category(IntegrationTest.class)
public class TiDBTableFactoryTest extends FlinkTestBase {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testUnsupportedTableFactory() throws Exception {
    exceptionRule.expect(allOf(
        isA(ValidationException.class),
        hasProperty("message",
            containsString("Unable to create a source for reading table"))
    ));

    exceptionRule.expectCause(allOf(
        isA(ValidationException.class),
        hasProperty("message",
            containsString("Cannot discover a connector using option: 'connector'='tidb'"))
    ));

    StreamTableEnvironment tableEnvironment = getBatchModeStreamTableEnvironment();
    tableEnvironment.sqlUpdate("CREATE TABLE `people`(\n"
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

    tableEnvironment.sqlUpdate(
        "SELECT * FROM people");
    tableEnvironment.execute("test");
  }
}