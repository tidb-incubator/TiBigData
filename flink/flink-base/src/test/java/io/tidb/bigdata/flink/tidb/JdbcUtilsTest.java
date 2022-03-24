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

import org.junit.Assert;
import org.junit.Test;

public class JdbcUtilsTest {

  @Test
  public void testRewriteJdbcUrlPath() {
    String database = "test";
    String[] inputs = new String[]{
        "jdbc:mysql://127.0.0.1:4000/test1?serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=false",
        "jdbc:mysql://127.0.0.1:4000/?serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=false",
        "jdbc:mysql://127.0.0.1:4000/test2",
        "jdbc:tidb://127.0.0.1:4000/test3?serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=false",
        "jdbc:tidb://127.0.0.1:4000/?serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=false",
        "jdbc:tidb://127.0.0.1:4000/test4"
    };
    String[] outputs = new String[]{
        "jdbc:mysql://127.0.0.1:4000/test?serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=false",
        "jdbc:mysql://127.0.0.1:4000/test?serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=false",
        "jdbc:mysql://127.0.0.1:4000/test",
        "jdbc:tidb://127.0.0.1:4000/test?serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=false",
        "jdbc:tidb://127.0.0.1:4000/test?serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=false",
        "jdbc:tidb://127.0.0.1:4000/test"
    };
    for (int i = 0; i < inputs.length; i++) {
      Assert.assertEquals(JdbcUtils.rewriteJdbcUrlPath(inputs[i], database), outputs[i]);
    }
  }

}
