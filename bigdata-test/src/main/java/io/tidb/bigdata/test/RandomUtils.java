package io.tidb.bigdata.test;

import java.util.Random;
import java.util.UUID;

public class RandomUtils {

  private static final Random random = new Random();


  public static String randomString() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  public static int randomInt() {
    return random.nextInt();
  }

}
