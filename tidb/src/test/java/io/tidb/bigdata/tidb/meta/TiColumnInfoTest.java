package io.tidb.bigdata.tidb.meta;

import static org.junit.Assert.assertEquals;

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
            "2024-04-01 00:00:00",
            "0000-00-00 00:00:00",
            "timestamp",
            1,
            "",
            false);
    assertEquals(
        "\000", columnInfo.getOriginDefaultValueAsByteString().toStringUtf8());
  }
}
