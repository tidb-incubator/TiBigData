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

package io.tidb.bigdata.flink.tidb.sink;

import io.tidb.bigdata.flink.tidb.FlinkTestBase;
import io.tidb.bigdata.test.ConfigUtils;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.RandomUtils;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.codec.TiDBEncodeHelper;
import io.tidb.bigdata.tidb.TiDBWriteHelper;
import io.tidb.bigdata.tidb.allocator.DynamicRowIDAllocator;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.row.ObjectRowImpl;

@Category(IntegrationTest.class)
public class TiDBWriteHelperTest extends FlinkTestBase {

  @Test
  public void testWrite() {
    ClientSession clientSession = ClientSession.create(
        new ClientConfig(ConfigUtils.defaultProperties()));
    String tableName = RandomUtils.randomString();
    clientSession.sqlUpdate(String.format(
        "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n" + "(\n" + "    c1  bigint,\n" + "    UNIQUE KEY(c1)"
            + ")", DATABASE_NAME, tableName));
    writeData(clientSession, tableName);
    writeData(clientSession, tableName);
  }

  private void writeData(ClientSession clientSession, String tableName) {
    TiTimestamp timestamp = clientSession.getSnapshotVersion();
    DynamicRowIDAllocator rowIDAllocator = new DynamicRowIDAllocator(clientSession, DATABASE_NAME,
        tableName, 100);
    TiDBEncodeHelper tiDBEncodeHelper = new TiDBEncodeHelper(clientSession, timestamp, DATABASE_NAME,
        tableName, false, true, rowIDAllocator);
    TiDBWriteHelper tiDBWriteHelper = new TiDBWriteHelper(clientSession.getTiSession(),
        timestamp.getVersion());
    List<BytePairWrapper> pairs = LongStream.range(0, 1000)
        .mapToObj(i -> ObjectRowImpl.create(new Long[]{i}))
        .map(tiDBEncodeHelper::generateKeyValuesByRow).flatMap(Collection::stream)
        .collect(Collectors.toList());
    tiDBWriteHelper.preWriteFirst(pairs);
    tiDBWriteHelper.commitPrimaryKey();
    tiDBWriteHelper.close();
  }
}