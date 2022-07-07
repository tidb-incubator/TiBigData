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

package io.tidb.bigdata.tidb.allocator;

import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.ConfigUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class RowIDAllocatorTest {

  @Test
  public void testCreateRowIDAllocator() throws Exception {
    int threads = 50;
    int count = threads * 10;
    int step = 10000;
    ExecutorService executorService = null;
    try (ClientSession session = ClientSession.create(
        new ClientConfig(ConfigUtils.defaultProperties()))) {
      executorService = Executors.newFixedThreadPool(threads);
      String databaseName = "test";
      String tableName = "test_row_id_allocator";
      session.sqlUpdate(
          String.format("DROP TABLE IF EXISTS `%s`.`%s`", databaseName, tableName),
          String.format("CREATE TABLE `%s`.`%s`(id bigint)", databaseName, tableName));
      List<FutureTask<RowIDAllocator>> tasks = new ArrayList<>();

      for (int i = 1; i <= count; i++) {
        FutureTask<RowIDAllocator> task =
            new FutureTask<>(
                () -> session.createRowIdAllocator(databaseName, tableName, step));
        tasks.add(task);
        executorService.submit(task);
      }
      long sum = 0;
      for (FutureTask<RowIDAllocator> task : tasks) {
        RowIDAllocator rowIDAllocator = task.get(10, TimeUnit.SECONDS);
        sum += rowIDAllocator.getEnd() - rowIDAllocator.getStart();
      }
      Assert.assertEquals(step * count, sum);
    } finally {
      if (executorService != null) {
        executorService.shutdownNow();
      }
    }

  }
}
