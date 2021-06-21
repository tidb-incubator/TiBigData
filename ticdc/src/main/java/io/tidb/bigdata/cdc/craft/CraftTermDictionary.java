package io.tidb.bigdata.cdc.craft;

import java.util.function.Function;

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

  public String decode(int id) {
    if (id >= this.terms.length || id < 0) {
      throw new IllegalArgumentException("Invalid term id: " + id);
    }
    return this.terms[id];
  }

  public String decodeNullable(int id) {
    if (id == -1) {
      return null;
    }
    return decode(id);
  }

  private String[] doDecodeChunk(Codec codec, int elements, Function<Integer, String> decoder) {
    long[] id = codec.decodeDeltaVarintChunk(elements);
    String[] terms = new String[elements];
    for (int idx = 0; idx < elements; ++idx) {
      terms[idx] = decoder.apply((int) id[idx]);
    }
    return terms;
  }

  public String[] decodeChunk(Codec codec, int elements) {
    return doDecodeChunk(codec, elements, this::decode);
  }

  public String[] decodeNullableChunk(Codec codec, int elements) {
    return doDecodeChunk(codec, elements, this::decodeNullable);
  }
}
