package io.tidb.bigdata.flink.connector.source;

import java.util.Arrays;

public enum SnapshotSourceSemantic {
  AT_LEAST_ONCE("at-least-once"),
  EXACTLY_ONCE("exactly-once");

  private final String semantic;

  SnapshotSourceSemantic(String semantic) {
    this.semantic = semantic;
  }

  public String getSemantic() {
    return semantic;
  }

  public static SnapshotSourceSemantic fromString(String s) {
    return Arrays.stream(values())
        .filter(value -> value.getSemantic().equalsIgnoreCase(s))
        .findFirst()
        .orElseThrow(
            () -> new IllegalArgumentException("Unsupported snapshot source semantic: " + s));
  }
}
