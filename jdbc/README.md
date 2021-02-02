# TiDB-JDBC

TiDB driver is a load-balancing driver.

## Configuration

| Configration       | Default Value                            | Description                                                  |
| :----------------- | :--------------------------------------- | :----------------------------------------------------------- |
| url.provider       | org.tikv.bigdata.jdbc.DefaultUrlProvider | TiDB driver will query all TiDB server addresses. You could implements the interface `Function<Collection<String>, Collection<String>>` to specify load balancing policy and address mapping policy. The default implementation is `org.tikv.bigdata.jdbc.DefaultUrlProvider`. It will pick one  randomly when establishing connections. |
| tidb.urls-cache-ms | 0                                        | In most cases, we use connection pool to establish connections and connections are created in batches. It would be better to cache TiDB server addresses rather than query TiDB server addresses for each connection. |

## Usage

```java
public class TiDBDriverTest {

  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("org.tikv.bigdata.jdbc.TiDBDriver");
    String url = "jdbc:tidb://localhost:4000/test"
        + "?user=root"
        + "&password="
        + "&url.provider=org.tikv.bigdata.jdbc.DefaultUrlProvider"
        + "&tidb.urls-cache-ms=3000";
    try (Connection connection = DriverManager.getConnection(url);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW TABLES ")) {
      while (resultSet.next()) {
        System.out.println(resultSet.getString(1));
      }
    }
  }
}
```