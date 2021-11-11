package io.tidb.bigdata.tidb;

import com.pingcap.tikv.allocator.RowIDAllocator;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.row.ObjectRowImpl;
import org.tikv.common.row.Row;
import org.tikv.common.util.Pair;

public class TiDBEncodeHelperTest {

  ClientSession session = ClientSession.createWithSingleConnection(
      new ClientConfig(ConfigUtils.getProperties()));

  @Test
  public void testRowIDAllocator() {
    String databaseName = "test";
    String tableName = "people";
    session.sqlUpdate("CREATE TABLE IF NOT EXISTS people(id int,name varchar(255))");
    RowIDAllocator rowIDAllocator = session.createRowIdAllocator(databaseName, tableName, 30000);
    System.out.println(rowIDAllocator.getStart());
    System.out.println(rowIDAllocator.getEnd());
  }

  @Test
  public void testEncode() {
    session.sqlUpdate("DROP TABLE IF EXISTS table1");
    session.sqlUpdate("CREATE TABLE IF NOT EXISTS table1(id int,name varchar(255))");
    TiDBEncodeHelper tiDBEncodeHelper = new TiDBEncodeHelper(session, session.getTimestamp(),
        "test", "table1");
    Row row = ObjectRowImpl.create(new Object[]{1L, "zs"});
    List<BytePairWrapper> pairs = tiDBEncodeHelper.generateKeyValuesByRow(row);
    for (BytePairWrapper pair : pairs) {
      System.out.printf("%s:%s%n", Arrays.toString(pair.getKey()),
          Arrays.toString(pair.getValue()));
    }
  }

  @Test
  public void testEncodeWithPrimaryKey() {
    session.sqlUpdate("DROP TABLE IF EXISTS table2");
    session.sqlUpdate(
        "CREATE TABLE IF NOT EXISTS table2(id int,name varchar(255),primary key(id) NONCLUSTERED) ");
    session.sqlUpdate("INSERT INTO table2 VALUES(1,'zs')");
    TiDBEncodeHelper tiDBEncodeHelper = new TiDBEncodeHelper(session, session.getTimestamp(),
        "test", "table2");
    Row row = ObjectRowImpl.create(new Object[]{1L, "zs"});
    List<BytePairWrapper> pairs = tiDBEncodeHelper.generateKeyValuesByRow(row);
    for (BytePairWrapper pair : pairs) {
      System.out.printf("%s:%s%n", Arrays.toString(pair.getKey()),
          Arrays.toString(pair.getValue()));
    }
  }

  @Test
  public void testEncodeWithAutoIncrementKey() {
    session.sqlUpdate("DROP TABLE IF EXISTS table3");
    session.sqlUpdate(
        "CREATE TABLE IF NOT EXISTS table3(id int auto_increment,name varchar(255),primary key(id) NONCLUSTERED) ");
    session.sqlUpdate("INSERT INTO table3 VALUES(1,'zs')");
    TiDBEncodeHelper tiDBEncodeHelper = new TiDBEncodeHelper(session, session.getTimestamp(),
        "test", "table3");
    Row row = ObjectRowImpl.create(new Object[]{null, "zs"});
    List<BytePairWrapper> pairs = tiDBEncodeHelper.generateKeyValuesByRow(row);
    for (BytePairWrapper pair : pairs) {
      System.out.printf("%s:%s%n", Arrays.toString(pair.getKey()), Arrays.toString(pair.getValue()));
    }
  }

  @Test
  public void testEncodeWithUniqueKey() {
    session.sqlUpdate("DROP TABLE IF EXISTS table4");
    session.sqlUpdate(
        "CREATE TABLE IF NOT EXISTS table4(id int auto_increment,name varchar(255),unique key(id))");
    session.sqlUpdate("INSERT INTO table4 VALUES(1,'zs')");
    TiDBEncodeHelper tiDBEncodeHelper = new TiDBEncodeHelper(session, session.getTimestamp(),
        "test", "table4");
    Row row = ObjectRowImpl.create(new Object[]{1L, "zs"});
    List<BytePairWrapper> pairs = tiDBEncodeHelper.generateKeyValuesByRow(row);
    for (BytePairWrapper pair : pairs) {
      System.out.printf("%s:%s%n", Arrays.toString(pair.getKey()), Arrays.toString(pair.getValue()));
    }
  }

}