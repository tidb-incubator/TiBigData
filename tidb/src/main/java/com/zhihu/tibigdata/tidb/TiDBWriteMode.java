package com.zhihu.tibigdata.tidb;

public enum TiDBWriteMode {
  APPEND, UPSERT;

  public static TiDBWriteMode fromString(String string) {
    for (TiDBWriteMode value : TiDBWriteMode.values()) {
      if (value.name().equalsIgnoreCase(string)) {
        return value;
      }
    }
    throw new IllegalArgumentException(string);
  }
}
