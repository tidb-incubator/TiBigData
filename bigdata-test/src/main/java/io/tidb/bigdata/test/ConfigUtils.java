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

package io.tidb.bigdata.test;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class ConfigUtils {

  public static final String DATABASE_URL = "tidb.database.url";
  public static final String USERNAME = "tidb.username";
  public static final String PASSWORD = "tidb.password";
  public static final String TIDB_HOST = "TIDB_HOST";
  public static final String TIDB_PORT = "TIDB_PORT";
  public static final String TIDB_USER = "TIDB_USER";
  public static final String TIDB_PASSWORD = "TIDB_PASSWORD";
  public static final String TELEMETRY_ENABLE = "tidb.telemetry.enable";

  public static final String tidbHost = getEnvOrDefault(TIDB_HOST, "127.0.0.1");

  public static final String tidbPort = getEnvOrDefault(TIDB_PORT, "4000");

  public static final String tidbUser = getEnvOrDefault(TIDB_USER, "root");

  public static final String tidbPassword = getEnvOrDefault(TIDB_PASSWORD, "");

  public static final String telemetryEnable = getEnvOrDefault(TELEMETRY_ENABLE, "false");

  private static String getEnvOrDefault(String key, String defaultValue) {
    String env = System.getenv(key);
    if (StringUtils.isNotEmpty(env)) {
      return env;
    }
    return System.getProperty(key, defaultValue);
  }

  public static Map<String, String> defaultProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(
        DATABASE_URL,
        String.format(
            "jdbc:mysql://%s:%s/?serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL"
                + "&tinyInt1isBit=false&enabledTLSProtocols=TLSv1,TLSv1.1,TLSv1.2",
            tidbHost, tidbPort));
    properties.put(USERNAME, tidbUser);
    properties.put(PASSWORD, tidbPassword);
    properties.put(TELEMETRY_ENABLE, telemetryEnable);
    return properties;
  }
}
