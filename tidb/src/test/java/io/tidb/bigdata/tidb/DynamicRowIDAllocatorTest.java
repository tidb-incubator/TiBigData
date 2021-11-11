package io.tidb.bigdata.tidb;

import org.junit.Assert;
import org.junit.Test;

public class DynamicRowIDAllocatorTest {

  ClientSession session = ClientSession.createWithSingleConnection(
      new ClientConfig(ConfigUtils.getProperties()));

  @Test
  public void testDynamicRowIDAllocator() throws Exception {
    session.dropTable("test", "test1", true);
    session.sqlUpdate("CREATE TABLE test1(c1 bigint)");
    DynamicRowIDAllocator dynamicRowIDAllocator = new DynamicRowIDAllocator(session, "test",
        "test1", 10);
    int index = 0;
    for (int i = 1; i <= 1000; i++) {
      Assert.assertEquals(++index, dynamicRowIDAllocator.getSharedRowId());
    }
    for (int i = 1; i <= 1000; i++) {
      Assert.assertEquals(++index, dynamicRowIDAllocator.getAutoIncId());
    }
    dynamicRowIDAllocator.close();

    session.dropTable("test", "test2", true);
    session.sqlUpdate("CREATE TABLE test2(c1 bigint)");
    dynamicRowIDAllocator = new DynamicRowIDAllocator(session, "test",
        "test2", 1000000);
    index = 0;
    for (int i = 1; i <= 1000; i++) {
      Assert.assertEquals(++index, dynamicRowIDAllocator.getSharedRowId());
    }
    for (int i = 1; i <= 1000; i++) {
      Assert.assertEquals(++index, dynamicRowIDAllocator.getAutoIncId());
    }
    dynamicRowIDAllocator.close();
  }

}