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

package io.tibigdata.trino;

import static io.trino.testing.TestingSession.testSessionBuilder;

import io.tidb.bigdata.trino.ConnectorsPlugin;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import java.util.List;
import java.util.Map;
import org.junit.Assert;

public class TiDBQueryRunner {

  private final Session session = testSessionBuilder().setCatalog("tidb").setSchema("test")
      .build();

  private final QueryRunner queryRunner;

  {
    try {
      queryRunner = DistributedQueryRunner.builder(session).setNodeCount(3).build();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    Map<String, String> properties = ConfigUtils.getProperties();
    queryRunner.installPlugin(new ConnectorsPlugin());
    queryRunner.createCatalog("tidb", "tidb", properties);
  }

  public void execute(String sql) {
    queryRunner.execute(sql);
  }

  public void verifySqlResult(String sql, List<MaterializedRow> targetRows) {
    MaterializedResult materializedResult = queryRunner.execute(sql);
    List<MaterializedRow> queryRows = materializedResult.getMaterializedRows();
    Assert.assertEquals(targetRows, queryRows);
  }


}
