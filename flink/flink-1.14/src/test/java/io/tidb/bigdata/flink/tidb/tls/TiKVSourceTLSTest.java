package io.tidb.bigdata.flink.tidb.tls;

import static io.tidb.bigdata.tidb.ClientConfig.*;
import static java.lang.String.format;

import io.tidb.bigdata.test.TableUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TiKVSourceTLSTest {

  public static final String tidbHost = "127.0.0.1";
  public static final String tidbPort = "4000";
  public static final String tidbUser = "root";
  public static final String tidbPassword = "";

  @Before
  public void beforeTest() {
    org.junit.Assume.assumeTrue(enableTLS());
  }

  public Boolean enableTLS() {
    return Boolean.parseBoolean(System.getenv("TLS_ENABLE"));
  }

  @Test
  public void testTLS() {
    Map<String, String> properties = new HashMap<>();
    properties.put(
        DATABASE_URL,
        format(
            "jdbc:mysql://%s:%s/test?serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL"
                + "&tinyInt1isBit=false&enabledTLSProtocols=TLSv1,TLSv1.1,TLSv1.2"
                + "&useSSL=true&requireSSL=true&verifyServerCertificate=false",
            tidbHost, tidbPort));
    properties.put(USERNAME, tidbUser);
    properties.put(PASSWORD, tidbPassword);
    properties.put(CLUSTER_TLS_ENABLE, "true");
    properties.put(CLUSTER_TLS_CA, "/config/cert/pem/root.pem");
    properties.put(CLUSTER_TLS_CERT, "/config/cert/pem/client.pem");
    properties.put(CLUSTER_TLS_KEY, "/config/cert/pem/client-pkcs8.key");
    properties.put("type", "tidb");

    TableEnvironment tableEnvironment = getTableEnvironment();
    String createCatalogSql =
        format("CREATE CATALOG `tidb` WITH ( %s )", TableUtils.toSqlProperties(properties));
    tableEnvironment.executeSql(createCatalogSql);
    Assert.assertFalse(tableEnvironment.getCatalog("tidb").get().listDatabases().isEmpty());
  }

  @Test
  public void testJKS() {
    Map<String, String> properties = new HashMap<>();
    properties.put(
        DATABASE_URL,
        format(
            "jdbc:mysql://%s:%s/test?serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL"
                + "&tinyInt1isBit=false&enabledTLSProtocols=TLSv1,TLSv1.1,TLSv1.2"
                + "&useSSL=true&requireSSL=true&verifyServerCertificate=false",
            tidbHost, tidbPort));
    properties.put(USERNAME, tidbUser);
    properties.put(PASSWORD, tidbPassword);
    properties.put(CLUSTER_TLS_ENABLE, "true");
    properties.put(CLUSTER_JKS_ENABLE, "true");
    properties.put(CLUSTER_JKS_TRUST_PATH, "/config/cert/jks/server-cert-store");
    properties.put(CLUSTER_JKS_TRUST_PASSWORD, "12345678");
    properties.put(CLUSTER_JKS_KEY_PATH, "/config/cert/jks/client-keystore.p12");
    properties.put(CLUSTER_JKS_KEY_PASSWORD, "123456");
    properties.put("type", "tidb");

    TableEnvironment tableEnvironment = getTableEnvironment();
    String createCatalogSql =
        format("CREATE CATALOG `tidb` WITH ( %s )", TableUtils.toSqlProperties(properties));
    tableEnvironment.executeSql(createCatalogSql);
    Assert.assertFalse(tableEnvironment.getCatalog("tidb").get().listDatabases().isEmpty());
  }

  protected TableEnvironment getTableEnvironment() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    return TableEnvironment.create(settings);
  }
}
