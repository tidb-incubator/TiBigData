package io.tidb.bigdata.test;

import java.util.HashMap;
import java.util.Map;

public class ConfigUtils {

  public static final String DATABASE_URL = "tidb.database.url";
  public static final String USERNAME = "tidb.username";
  public static final String PASSWORD = "tidb.password";
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

  public static Map<String, String> defaultProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(DATABASE_URL, String.format(
        "jdbc:mysql://%s:%s/test?serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL"
            + "&tinyInt1isBit=false&enabledTLSProtocols=TLSv1,TLSv1.1,TLSv1.2",
        tidbHost, tidbPort));
    properties.put(USERNAME, tidbUser);
    properties.put(PASSWORD, tidbPassword);
    return properties;
  }

}
