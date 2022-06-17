package io.tidb.bigdata.tidb;

import org.junit.Test;
import org.tikv.kvproto.Kvrpcpb.KeyError;

public class ProtobufTest {

  @Test
  public void testProtobufToString() {
    KeyError.getDefaultInstance().toString();
  }
}
