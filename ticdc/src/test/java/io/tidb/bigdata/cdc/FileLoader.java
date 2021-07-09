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

import io.tidb.bigdata.cdc.craft.CraftCodec;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import org.junit.Assert;

public class FileLoader {
  protected static String getFormat(final Codec codec) {
    if (codec.getClass() == CraftCodec.class) {
      return "craft";
    }
    return "json";
  }

  protected static FileWithCodec getFile(final Codec codec, final String path,
      boolean required) {
    final String format = getFormat(codec);
    final URL url = FileLoader.class.getClassLoader().getResource(format + "/" + path);
    if (required) {
      Assert.assertNotNull(url);
    }
    if (url == null) {
      return null;
    }
    return new FileWithCodec(codec, new File(url.getFile()));
  }

  protected static File[] listFiles(final Codec codec, final String path) {
    return getFile(codec, path, true).listFiles();
  }

  protected static FileWithCodec[] listFilesWithFormat(final Codec codec, final String path) {
    return getFile(codec, path, true).listFilesWithFormat();
  }

  protected static byte[] getFileContent(final FileWithCodec file) throws IOException {
    if (file == null) {
      return null;
    }
    return Files.readAllBytes(file.file.toPath());
  }

  protected static EventDecoder decode(final Codec codec, final String fileName)
      throws IOException {
    return decode(codec, getFile(codec, "key/" + fileName, false),
        getFile(codec, "value/" + fileName, true));
  }

  protected static EventDecoder decode(final Codec codec,
      final FileWithCodec key, final FileWithCodec value)
      throws IOException {
    return codec.decode(getFileContent(key), getFileContent(value));
  }

  protected static EventDecoder decode(final Codec codec, final File key, final File value)
      throws IOException {
    return decode(codec, new FileWithCodec(codec, key), new FileWithCodec(codec, value));
  }

  protected static KeyDecoder decodeKey(final Codec codec, final String key) throws IOException {
    return decodeKey(getFile(codec, "key/" + key, true));
  }

  protected static KeyDecoder decodeKey(final FileWithCodec key) throws IOException {
    return key.codec.key(getFileContent(key));
  }

  protected static ValueDecoder decodeValue(final Codec codec, final String value)
      throws IOException {
    return decodeValue(getFile(codec, "value/" + value, true));
  }

  protected static ValueDecoder decodeValue(final FileWithCodec value) throws IOException {
    return value.codec.value(getFileContent(value));
  }

  private static class FileWithCodec {

    private final File file;
    private final Codec codec;

    private FileWithCodec(Codec codec, File file) {
      this.codec = codec;
      this.file = file;
    }

    private FileWithCodec[] listFilesWithFormat() {
      return Arrays.stream(listFiles()).map(f -> new FileWithCodec(codec, f))
          .toArray(FileWithCodec[]::new);
    }

    private File[] listFiles() {
      return file.listFiles();
    }
  }
}
