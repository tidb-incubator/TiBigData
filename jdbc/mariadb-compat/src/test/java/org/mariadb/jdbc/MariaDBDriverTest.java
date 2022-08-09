/*
 * Copyright 2021 TiDB Project Authors.
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

package org.mariadb.jdbc;

import io.tidb.bigdata.test.IntegrationTest;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class MariaDBDriverTest {

  public static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";

  @Test
  public void testMariaDBDriver() throws ClassNotFoundException, SQLException {
    Class.forName(JDBC_DRIVER);
    Assert.assertNotNull(
        DriverManager.getDriver("jdbc:mariadb://127.0.0.1:4000?user=root&password="));
    Assert.assertTrue(
        DriverManager.getDriver("jdbc:mariadb://127.0.0.1:4000?user=root&password=")
            instanceof Driver);
  }
}
