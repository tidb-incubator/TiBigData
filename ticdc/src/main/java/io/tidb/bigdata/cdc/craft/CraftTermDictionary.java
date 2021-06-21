package io.tidb.bigdata.cdc.craft;

import java.io.IOException;

public class CraftTermDictionary {
  private final String[] terms;
  private static final CraftTermDictionary EMPTY_DICTIONARY = new CraftTermDictionary();

  private CraftTermDictionary() {
    terms = new String[0];
  }

  CraftTermDictionary(Codec codec) {
    this.terms = codec.decodeStringChunk((int) codec.decodeUvarint());
  }

  public static CraftTermDictionary empty() {
    return EMPTY_DICTIONARY;
  }

  public String decode(int id) throws IOException {
    if (id >= this.terms.length || id < 0) {
      throw new IllegalArgumentException("Invalid term id: " + id);
    }
    return this.terms[id];
  }

  public String decodeNullable(int id) throws IOException {
    if (id == -1) {
      return null;
    }
    return decode(id);
  }

  public String[] decodeChunk(Codec codec, int elements) throws IOException {
    long[] id = codec.decodeDeltaUvarintChunk(elements);
    String[] terms = new String[elements];
    for (int idx = 0; idx < elements; ++idx) {
      terms[idx] = decode((int) id[idx]);
    }
    return terms;
  }

  public String[] decodeNullableChunk(Codec codec, int elements) throws IOException {
    long[] id = codec.decodeDeltaVarintChunk(elements);
    String[] terms = new String[elements];
    for (int idx = 0; idx < elements; ++idx) {
      terms[idx] = decodeNullable((int) id[idx]);
    }
    return terms;
  }
}
