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

package io.tidb.bigdata.flink.tidb;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.ClassRule;

public abstract class FlinkTestBase {

  @ClassRule public static final TiDBTestDatabase testDatabase = new TiDBTestDatabase();

  public static final String CATALOG_NAME = "tidb";

  public static final String DATABASE_NAME = "tiflink_test";

  public static final String CREATE_DATABASE_SQL =
      String.format("CREATE DATABASE IF NOT EXISTS `%s`", DATABASE_NAME);

  protected TableEnvironment getTableEnvironment() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    return TableEnvironment.create(settings);
  }
}
