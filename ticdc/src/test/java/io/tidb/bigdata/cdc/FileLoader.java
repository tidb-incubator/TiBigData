/*
 * Copyright 2021 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.cdc;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import org.junit.Assert;

public class FileLoader {

  protected static File getFile(final String path) {
    final URL url = FileLoader.class.getClassLoader().getResource(path);
    Assert.assertNotNull(url);
    return new File(url.getFile());
  }

  protected static File[] listFiles(final String path) {
    return getFile(path).listFiles();
  }

  protected static byte[] getFileContent(final File file) throws IOException {
    return Files.readAllBytes(file.toPath());
  }

  protected static EventDecoder decode(final String fileName) throws IOException {
    return decode(getFile("key/" + fileName), getFile("value/" + fileName));
  }

  protected static EventDecoder decode(final File key, final File value) throws IOException {
    return EventDecoder
        .create(getFileContent(key), getFileContent(value), Misc.jackson().createParser());
  }

  protected static KeyDecoder decodeKey(final String key) throws IOException {
    return decodeKey(getFile("key/" + key));
  }

  protected static KeyDecoder decodeKey(final File key) throws IOException {
    return KeyDecoder.create(getFileContent(key), Misc.jackson().createParser());
  }

  protected static ValueDecoder decodeValue(final String value) throws IOException {
    return decodeValue(getFile("value/" + value));
  }

  protected static ValueDecoder decodeValue(final File value) throws IOException {
    return ValueDecoder.create(getFileContent(value), Misc.jackson().createParser());
  }
}
