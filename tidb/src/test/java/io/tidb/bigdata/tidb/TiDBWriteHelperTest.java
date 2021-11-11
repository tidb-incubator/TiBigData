package io.tidb.bigdata.tidb;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.Snapshot;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.row.ObjectRowImpl;
import org.tikv.common.row.Row;

public class TiDBWriteHelperTest {

  ClientSession session = ClientSession.createWithSingleConnection(
      new ClientConfig(ConfigUtils.getProperties()));

  @Test
  public void testInsertRow() {
    session.sqlUpdate("DROP TABLE IF EXISTS table1");
    session.sqlUpdate("CREATE TABLE IF NOT EXISTS table1(c1 int,c2 int,c3 int, c4 int)");
    TiDBEncodeHelper tiDBEncodeHelper = new TiDBEncodeHelper(session, session.getTimestamp(),
        "test", "table1");
    Row row1 = ObjectRowImpl.create(new Object[]{1L, 1L, 1L, 1L});
    Row row2 = ObjectRowImpl.create(new Object[]{2L, 2L, 2L, 2L});
    List<BytePairWrapper> pairs1 = tiDBEncodeHelper.generateKeyValuesByRow(row1);
    List<BytePairWrapper> pairs2 = tiDBEncodeHelper.generateKeyValuesByRow(row2);
    long startTs = session.getTimestamp().getVersion();
    TiDBWriteHelper tiDBWriteHelper = new TiDBWriteHelper(session.getTiSession(), startTs);
    tiDBWriteHelper.preWriteFirst(pairs1);
    tiDBWriteHelper.preWriteSecondKeys(pairs2);
    tiDBWriteHelper.commitPrimaryKey();
    tiDBWriteHelper.close();
  }

  @Test
  public void testDistributeInsertRow() {
    session.sqlUpdate("DROP TABLE IF EXISTS table1");
    session.sqlUpdate("CREATE TABLE IF NOT EXISTS table1(c1 int,c2 int,c3 int, c4 int)");
    TiDBEncodeHelper tiDBEncodeHelper = new TiDBEncodeHelper(session, session.getTimestamp(),
        "test", "table1");
    Row row1 = ObjectRowImpl.create(new Object[]{1L, 1L, 1L, 1L});
    Row row2 = ObjectRowImpl.create(new Object[]{2L, 2L, 2L, 2L});
    List<BytePairWrapper> pairs1 = tiDBEncodeHelper.generateKeyValuesByRow(row1);
    List<BytePairWrapper> pairs2 = tiDBEncodeHelper.generateKeyValuesByRow(row2);
    long startTs = session.getTimestamp().getVersion();
    TiDBWriteHelper tiDBWriteHelper1 = new TiDBWriteHelper(session.getTiSession(), startTs);
    tiDBWriteHelper1.preWriteFirst(pairs1);
    TiDBWriteHelper tiDBWriteHelper2 = new TiDBWriteHelper(session.getTiSession(), startTs,
        tiDBWriteHelper1.getPrimaryKeyMust());
    tiDBWriteHelper2.preWriteSecondKeys(pairs2);
    tiDBWriteHelper2.close();
    tiDBWriteHelper1.commitPrimaryKey();
    tiDBWriteHelper1.close();
  }

  @Test
  public void testInsertRowWithPrimaryKey() {
    session.sqlUpdate("DROP TABLE IF EXISTS table2");
    session.sqlUpdate(
        "CREATE TABLE IF NOT EXISTS table2(c1 int,c2 int,c3 int, c4 int,PRIMARY KEY(c1) NONCLUSTERED) ");
    TiDBEncodeHelper tiDBEncodeHelper = new TiDBEncodeHelper(session, session.getTimestamp(),
        "test", "table2");
    Row row = ObjectRowImpl.create(new Object[]{1L, 1L, 1L, 1L});
    List<BytePairWrapper> pairs = tiDBEncodeHelper.generateKeyValuesByRow(row);
    long startTs = session.getTimestamp().getVersion();
    TiDBWriteHelper tiDBWriteHelper = new TiDBWriteHelper(session.getTiSession(), startTs);
    tiDBWriteHelper.preWriteFirst(pairs);
    tiDBWriteHelper.commitPrimaryKey();
    tiDBWriteHelper.close();
  }

  @Test
  public void testInsertRowWithUniqueKey() {
    session.sqlUpdate("DROP TABLE IF EXISTS table3");
    session.sqlUpdate(
        "CREATE TABLE IF NOT EXISTS table3(c1 int,c2 int,c3 int, c4 int,UNIQUE KEY(c1)) ");
    TiDBEncodeHelper tiDBEncodeHelper = new TiDBEncodeHelper(session, session.getTimestamp(),
        "test", "table3");
    Row row = ObjectRowImpl.create(new Object[]{1L, 1L, 1L, 1L});
    List<BytePairWrapper> pairs = tiDBEncodeHelper.generateKeyValuesByRow(row);
    long startTs = session.getTimestamp().getVersion();
    TiDBWriteHelper tiDBWriteHelper = new TiDBWriteHelper(session.getTiSession(), startTs);
    tiDBWriteHelper.preWriteFirst(pairs);
    tiDBWriteHelper.commitPrimaryKey();
    tiDBWriteHelper.close();
  }

  @Test
  public void testUUIDAsPrimaryKey() {
    session.sqlUpdate("DROP TABLE IF EXISTS table4");
    session.sqlUpdate(
        "CREATE TABLE IF NOT EXISTS table4(c1 int,c2 int,c3 int, c4 int) ");
    TiDBEncodeHelper tiDBEncodeHelper = new TiDBEncodeHelper(session, session.getTimestamp(),
        "test", "table4");
    Row row = ObjectRowImpl.create(new Object[]{1L, 1L, 1L, 1L});
    List<BytePairWrapper> pairs = tiDBEncodeHelper.generateKeyValuesByRow(row);
    long startTs = session.getTimestamp().getVersion();
    TiDBWriteHelper tiDBWriteHelper = new TiDBWriteHelper(session.getTiSession(), startTs);
    tiDBWriteHelper.preWriteFirst(
        ImmutableList.of(
            new BytePairWrapper(UUID.randomUUID().toString().getBytes(), new byte[0])));
    tiDBWriteHelper.preWriteSecondKeys(pairs);
    tiDBWriteHelper.commitPrimaryKey();
    tiDBWriteHelper.close();
  }

  @Test
  public void testReadEmptyValue() {
    byte[] key = UUID.randomUUID().toString().getBytes();
    TiTimestamp timestamp = session.getTimestamp();
    Snapshot snapshot = session.getTiSession().createSnapshot(timestamp);
    byte[] bytes = snapshot.get(key);
    Assert.assertArrayEquals(bytes, new byte[0]);
  }

}