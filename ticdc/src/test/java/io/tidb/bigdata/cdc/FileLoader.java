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
import java.util.Arrays;
import org.junit.Assert;

public class FileLoader {

  protected static FileWithFormat getFile(final String format, final String path,
      boolean required) {
    final URL url = FileLoader.class.getClassLoader().getResource(format + "/" + path);
    if (required) {
      Assert.assertNotNull(url);
    }
    if (url == null) {
      return null;
    }
    return new FileWithFormat(format, new File(url.getFile()));
  }

  protected static File[] listFiles(final String format, final String path) {
    return getFile(format, path, true).listFiles();
  }

  protected static FileWithFormat[] listFilesWithFormat(final String format, final String path) {
    return getFile(format, path, true).listFilesWithFormat();
  }

  protected static byte[] getFileContent(final FileWithFormat file) throws IOException {
    return Files.readAllBytes(file.file.toPath());
  }

  protected static EventDecoder decode(final String format, final String fileName)
      throws IOException {
    return decode(getFile(format, "key/" + fileName, false),
        getFile(format, "value/" + fileName, true));
  }

  protected static EventDecoder decode(final FileWithFormat key, final FileWithFormat value)
      throws IOException {
    switch (value.format) {
      case "json":
        return EventDecoder
            .json(getFileContent(key), getFileContent(value), ParserFactory.json());
      case "craft":
        return EventDecoder
            .craft(getFileContent(value), ParserFactory.craft());
      default:
        throw new RuntimeException("Unknown format" + key.format);
    }
  }

  protected static EventDecoder decode(final String format, final File key, final File value)
      throws IOException {
    return decode(new FileWithFormat(format, key), new FileWithFormat(format, value));
  }

  protected static KeyDecoder decodeKey(final String format, final String key) throws IOException {
    return decodeKey(getFile(format, "key/" + key, true));
  }

  protected static KeyDecoder decodeKey(final FileWithFormat key) throws IOException {
    switch (key.format) {
      case "json":
        return KeyDecoder.json(getFileContent(key), ParserFactory.json());
      case "craft":
        return KeyDecoder.craft(getFileContent(key), ParserFactory.craft());
      default:
        throw new RuntimeException("Not supported key decoder format: " + key.format);
    }
  }

  protected static ValueDecoder decodeValue(final String format, final String value)
      throws IOException {
    return decodeValue(getFile(format, "value/" + value, true));
  }

  protected static ValueDecoder decodeValue(final FileWithFormat value) throws IOException {
    switch (value.format) {
      case "json":
        return ValueDecoder.json(getFileContent(value), ParserFactory.json());
      case "craft":
        return ValueDecoder.craft(getFileContent(value), ParserFactory.craft());
      default:
        throw new RuntimeException("Not supported value decoder format: " + value.format);
    }
  }

  private static class FileWithFormat {

    private File file;
    private String format;

    private FileWithFormat(String format, File file) {
      this.format = format;
      this.file = file;
    }

    private FileWithFormat[] listFilesWithFormat() {
      return Arrays.stream(listFiles()).map(f -> new FileWithFormat(format, f))
          .toArray(FileWithFormat[]::new);
    }

    private File[] listFiles() {
      return file.listFiles();
    }
  }
}
