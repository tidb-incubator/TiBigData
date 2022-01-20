package io.tidb.bigdata.hive;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Test;

public class JobConfTest {

  @Test
  public void testToMap() {
    Map<String, String> map = new HashMap<>();
    map.put("1", "a");
    map.put("2", "b");
    JobConf jobConf = new JobConf();
    map.forEach(jobConf::set);
    Map<String, String> map1 = jobConf.getPropsWithPrefix("");
    map.forEach((k, v) -> Assert.assertEquals(map1.get(k), v));
  }

}
