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

package io.tibigdata.prestosql;

import static io.prestosql.testing.MaterializedResult.DEFAULT_PRECISION;

import com.google.common.collect.ImmutableList;
import io.prestosql.testing.MaterializedRow;
import io.tidb.bigdata.test.IntegrationTest;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class PrestoTest {

  private final TiDBQueryRunner tiDBQueryRunner = new TiDBQueryRunner();

  public void preCommand() {
    tiDBQueryRunner.execute("CREATE TABLE sample_table(c1 int,c2 varchar)");
    tiDBQueryRunner.execute("INSERT INTO sample_table values(1,'zs')");
    tiDBQueryRunner.execute("INSERT INTO sample_table values(2,'ls')");
  }

  public void afterCommand() {
    tiDBQueryRunner.execute("DROP TABLE sample_table");
  }

  @Test
  public void test() {
    preCommand();
    try {
      String sql = "SELECT * FROM sample_table";
      List<MaterializedRow> targetRows = ImmutableList
          .of(new MaterializedRow(DEFAULT_PRECISION, 1, "zs"),
              new MaterializedRow(DEFAULT_PRECISION, 2, "ls"));
      tiDBQueryRunner.verifySqlResult(sql, targetRows);

      sql = "SELECT c1,c2 FROM sample_table WHERE c1 = 1";
      targetRows = ImmutableList
          .of(new MaterializedRow(DEFAULT_PRECISION, 1, "zs"));
      tiDBQueryRunner.verifySqlResult(sql, targetRows);

      sql = "SELECT * FROM sample_table WHERE c1 = 1 OR c1 = 2";
      targetRows = ImmutableList
          .of(new MaterializedRow(DEFAULT_PRECISION, 1, "zs"),
              new MaterializedRow(DEFAULT_PRECISION, 2, "ls"));
      tiDBQueryRunner.verifySqlResult(sql, targetRows);
    } catch (Exception e) {
      throw e;
    } finally {
      afterCommand();
    }
  }

}
