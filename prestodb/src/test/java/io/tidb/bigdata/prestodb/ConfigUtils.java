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

package io.tidb.bigdata.prestodb;

import static io.tidb.bigdata.tidb.ClientConfig.DATABASE_URL;
import static io.tidb.bigdata.tidb.ClientConfig.PASSWORD;
import static io.tidb.bigdata.tidb.ClientConfig.USERNAME;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class ConfigUtils {

  public static final String TIDB_HOST = "TIDB_HOST";

  public static final String TIDB_PORT = "TIDB_PORT";

  public static final String TIDB_USER = "TIDB_USER";

  public static final String TIDB_PASSWORD = "TIDB_PASSWORD";

  public static final String tidbHost = getEnvOrDefault(TIDB_HOST, "127.0.0.1");

  public static final String tidbPort = getEnvOrDefault(TIDB_PORT, "4000");

  public static final String tidbUser = getEnvOrDefault(TIDB_USER, "root");

  public static final String tidbPassword = getEnvOrDefault(TIDB_PASSWORD, "");

  private static String getEnvOrDefault(String key, String default0) {
    String tmp = System.getenv(key);
    if (tmp != null && !tmp.equals("")) {
      return tmp;
    }

    tmp = System.getProperty(key);
    if (tmp != null && !tmp.equals("")) {
      return tmp;
    }

    return default0;
  }

  public static Map<String, String> getProperties() {
    return ImmutableMap.<String, String>builder()
        .put(
            DATABASE_URL,
            String.format(
                "jdbc:mysql://%s:%s/test?serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=false&enabledTLSProtocols=TLSv1,TLSv1.1,TLSv1.2",
                tidbHost, tidbPort))
        .put(USERNAME, tidbUser)
        .put(PASSWORD, tidbPassword)
        .build();
  }
}
