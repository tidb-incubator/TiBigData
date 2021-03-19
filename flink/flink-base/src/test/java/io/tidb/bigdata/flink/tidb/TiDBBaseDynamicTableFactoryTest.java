package io.tidb.bigdata.flink.tidb;

import org.apache.flink.table.connector.source.DynamicTableSource;
import org.junit.Assert;
import org.junit.Test;

public class TiDBBaseDynamicTableFactoryTest extends TiDBBaseDynamicTableFactory {

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    return null;
  }

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
      Assert.assertEquals(rewriteJdbcUrlPath(inputs[i], database), outputs[i]);
    }
  }

}
