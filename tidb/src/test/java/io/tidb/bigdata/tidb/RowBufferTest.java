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

package io.tidb.bigdata.tidb;

import com.google.common.collect.ImmutableList;
import io.tidb.bigdata.test.ConfigUtils;
import io.tidb.bigdata.test.IntegrationTest;
import io.tidb.bigdata.test.RandomUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.row.ObjectRowImpl;
import org.tikv.common.row.Row;

@Category(IntegrationTest.class)
public class RowBufferTest {

  ClientSession clientSession = ClientSession.create(
      new ClientConfig(ConfigUtils.defaultProperties()));

  @Test
  public void testDeduplicateRowBuffer() {
    String tableName = RandomUtils.randomString();
    clientSession.sqlUpdate(String.format("CREATE TABLE IF NOT EXISTS `%s`\n"
        + "(\n"
        + "    c1  int,\n"
        + "    c2  int,\n"
        + "    c3  int,\n"
        + "    c4  int,\n"
        + "    c5  int,\n"
        + "    c6  int,\n"
        + "    c7  int,\n"
        + "    PRIMARY KEY(c1,c2),\n"
        + "    UNIQUE KEY(c3),\n"
        + "    UNIQUE KEY(c4,c5),\n"
        + "    UNIQUE KEY(c6)"
        + ")", tableName));
    TiTableInfo tiTableInfo = clientSession.getTableMust("test", tableName);
    RowBuffer buffer = RowBuffer.createDeduplicateRowBuffer(tiTableInfo, true, 1000);
    Row row1 = ObjectRowImpl.create(new Object[]{1, 2, 3, 4, 5, 6, 7});
    Row row2 = ObjectRowImpl.create(
        new Object[]{
            1,
            2,
            RandomUtils.randomInt(),
            RandomUtils.randomInt(),
            RandomUtils.randomInt(),
            RandomUtils.randomInt(),
            RandomUtils.randomInt()});
    Row row3 = ObjectRowImpl.create(
        new Object[]{
            RandomUtils.randomInt(),
            RandomUtils.randomInt(),
            3,
            RandomUtils.randomInt(),
            RandomUtils.randomInt(),
            RandomUtils.randomInt(),
            RandomUtils.randomInt()});
    Row row4 = ObjectRowImpl.create(
        new Object[]{
            RandomUtils.randomInt(),
            RandomUtils.randomInt(),
            RandomUtils.randomInt(),
            4,
            5,
            RandomUtils.randomInt(),
            RandomUtils.randomInt()});
    Row row5 = ObjectRowImpl.create(
        new Object[]{
            RandomUtils.randomInt(),
            RandomUtils.randomInt(),
            RandomUtils.randomInt(),
            RandomUtils.randomInt(),
            RandomUtils.randomInt(),
            6,
            RandomUtils.randomInt()});
    Row row6 = ObjectRowImpl.create(new Object[]{2, 3, 4, 5, 6, 7, 8});
    Row row7 = ObjectRowImpl.create(new Object[]{2, 4, 6, 7, 8, 9, 10});
    buffer.add(row1);
    buffer.add(row2);
    buffer.add(row3);
    buffer.add(row4);
    buffer.add(row5);
    buffer.add(row6);
    buffer.add(row7);
    Assert.assertEquals(ImmutableList.of(row1, row6, row7), buffer.getRows());
  }

}