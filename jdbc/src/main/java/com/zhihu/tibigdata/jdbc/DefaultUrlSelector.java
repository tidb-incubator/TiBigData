package com.zhihu.tibigdata.jdbc;

import java.util.List;
import java.util.Random;
import java.util.function.Function;


public class DefaultUrlSelector implements Function<List<String>, String> {

  private final Random random = new Random();

  @Override
  public String apply(List<String> list) {
    return list.get(random.nextInt(list.size()));
  }

}
