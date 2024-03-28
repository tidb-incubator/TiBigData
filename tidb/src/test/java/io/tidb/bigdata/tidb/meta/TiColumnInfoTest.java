package io.tidb.bigdata.tidb.meta;

import io.tidb.bigdata.tidb.types.TimestampType;
import org.junit.Test;

public class TiColumnInfoTest {

  @Test
  public void testToProto() {
    TiColumnInfo columnInfo =
        new TiColumnInfo(
            1L,
            "name",
            0,
            TimestampType.TIMESTAMP,
            SchemaState.StatePublic,
            "0000-00-00 00:00:00",
            "0000-00-00 00:00:00",
            "0000-00-00 00:00:00",
            "timestamp",
            1,
            "",
            false);
    System.out.println(columnInfo.getOriginDefaultValueAsByteString());
  }
}
