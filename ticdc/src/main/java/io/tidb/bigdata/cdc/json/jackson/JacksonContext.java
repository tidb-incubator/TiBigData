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

package io.tidb.bigdata.cdc.json.jackson;

import static io.tidb.bigdata.cdc.Misc.uncheckedGetConstructor;
import static io.tidb.bigdata.cdc.Misc.uncheckedGetMethod;
import static io.tidb.bigdata.cdc.Misc.uncheckedLoadClass;
import static io.tidb.bigdata.cdc.Misc.uncheckedRun;

import io.tidb.bigdata.cdc.json.JsonNode;
import io.tidb.bigdata.cdc.json.JsonNode.Type;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import javax.validation.constraints.NotNull;

public class JacksonContext implements Serializable {

  private static final JacksonContext DEFAULT_CONTEXT;

  static {
    DEFAULT_CONTEXT = new JacksonContext("");
  }

  private String shadePrefix;
  private Constructor<Object> objectMapperConstructor;
  private Method createReader;
  private Method readTree;
  private Method has;
  private Method get;
  private Method binaryValue;
  private Method textValue;
  private Method numberValue;
  private Method intValue;
  private Method bigIntegerValue;
  private Method longValue;
  private Method booleanValue;
  private Method bigDecimalValue;
  private Method getNodeType;
  private Method fields;
  private Method valueOf;
  private Map<Object, Type> nodeTypesMapping;

  JacksonContext(@NotNull final String prefix) {
    shadePrefix = prefix;
    load();
  }

  static JacksonContext getDefaultContext() {
    return DEFAULT_CONTEXT;
  }

  private static String className(final String shadePrefix, final String name) {
    if (shadePrefix == null || shadePrefix.isEmpty()) {
      return name;
    } else {
      return shadePrefix + "." + name;
    }
  }

  private void load() {
    final Class<Object> objectMapperClass = uncheckedLoadClass(
        className(shadePrefix, "com.fasterxml.jackson.databind.ObjectMapper"));
    objectMapperConstructor = uncheckedGetConstructor(objectMapperClass);
    createReader = uncheckedGetMethod(objectMapperClass, "reader");
    final Class<Object> objectReaderClass = uncheckedLoadClass(
        className(shadePrefix, "com.fasterxml.jackson.databind.ObjectReader"));
    readTree = uncheckedGetMethod(objectReaderClass, "readTree", InputStream.class);
    final Class<Object> jsonNodeClass = uncheckedLoadClass(
        className(shadePrefix, "com.fasterxml.jackson.databind.JsonNode"));
    has = uncheckedGetMethod(jsonNodeClass, "has", String.class);
    get = uncheckedGetMethod(jsonNodeClass, "get", String.class);
    binaryValue = uncheckedGetMethod(jsonNodeClass, "binaryValue");
    textValue = uncheckedGetMethod(jsonNodeClass, "textValue");
    numberValue = uncheckedGetMethod(jsonNodeClass, "numberValue");
    intValue = uncheckedGetMethod(jsonNodeClass, "intValue");
    bigIntegerValue = uncheckedGetMethod(jsonNodeClass, "bigIntegerValue");
    longValue = uncheckedGetMethod(jsonNodeClass, "longValue");
    booleanValue = uncheckedGetMethod(jsonNodeClass, "booleanValue");
    bigDecimalValue = uncheckedGetMethod(jsonNodeClass, "decimalValue");
    getNodeType = uncheckedGetMethod(jsonNodeClass, "getNodeType");
    fields = uncheckedGetMethod(jsonNodeClass, "fields");
    final Class<Object> jsonNodeTypeClass = uncheckedLoadClass(
        className(shadePrefix, "com.fasterxml.jackson.databind.node.JsonNodeType"));
    valueOf = uncheckedGetMethod(jsonNodeTypeClass, "valueOf", String.class);
    nodeTypesMapping = new HashMap<>();
    nodeTypesMapping
        .put(uncheckedRun(() -> valueOf.invoke(null, "BOOLEAN")), JsonNode.Type.BOOLEAN);
    nodeTypesMapping.put(uncheckedRun(() -> valueOf.invoke(null, "NULL")), JsonNode.Type.NULL);
    nodeTypesMapping
        .put(uncheckedRun(() -> valueOf.invoke(null, "NUMBER")), JsonNode.Type.NUMBER);
    nodeTypesMapping
        .put(uncheckedRun(() -> valueOf.invoke(null, "OBJECT")), JsonNode.Type.OBJECT);
    nodeTypesMapping.put(uncheckedRun(() -> valueOf.invoke(null, "ARRAY")), JsonNode.Type.ARRAY);
    nodeTypesMapping
        .put(uncheckedRun(() -> valueOf.invoke(null, "STRING")), JsonNode.Type.STRING);
    nodeTypesMapping
        .put(uncheckedRun(() -> valueOf.invoke(null, "MISSING")), JsonNode.Type.MISSING);
  }

  private void readObject(final ObjectInputStream ois) throws ClassNotFoundException, IOException {
    shadePrefix = ois.readUTF();
    load();
  }

  private void writeObject(final ObjectOutputStream oos) throws IOException {
    oos.writeUTF(shadePrefix);
  }

  Object newMapper() {
    return uncheckedRun(() -> objectMapperConstructor.newInstance());
  }

  Object newReader(final Object mapper) {
    return uncheckedRun(() -> createReader.invoke(mapper));
  }

  Object readTree(final Object reader, final byte[] input) {
    return uncheckedRun(() -> readTree.invoke(reader, new ByteArrayInputStream(input)));
  }

  boolean has(final Object node, final String fieldName) {
    return uncheckedRun(() -> (boolean) has.invoke(node, fieldName));
  }

  Object get(final Object node, final String fieldName) {
    return uncheckedRun(() -> get.invoke(node, fieldName));
  }

  byte[] binaryValue(final Object node) {
    return uncheckedRun(() -> (byte[]) binaryValue.invoke(node));
  }

  String textValue(final Object node) {
    return uncheckedRun(() -> (String) textValue.invoke(node));
  }

  Number numberValue(final Object node) {
    return uncheckedRun(() -> (Number) numberValue.invoke(node));
  }

  int intValue(final Object node) {
    return uncheckedRun(() -> (int) intValue.invoke(node));
  }

  BigInteger bigIntegerValue(final Object node) {
    return uncheckedRun(() -> (BigInteger) bigIntegerValue.invoke(node));
  }

  long longValue(final Object node) {
    return uncheckedRun(() -> (long) longValue.invoke(node));
  }

  boolean booleanValue(final Object node) {
    return uncheckedRun(() -> (boolean) booleanValue.invoke(node));
  }

  BigDecimal bigDecimalValue(final Object node) {
    return uncheckedRun(() -> (BigDecimal) bigDecimalValue.invoke(node));
  }

  JsonNode.Type getNodeType(final Object node) {
    final Object type = uncheckedRun(() -> getNodeType.invoke(node));
    return Optional.ofNullable(nodeTypesMapping.get(type)).orElse(JsonNode.Type.NOT_SUPPORTED);
  }

  @SuppressWarnings("unchecked")
  Iterator<Entry<String, Object>> fields(final Object node) {
    return uncheckedRun(() -> (Iterator<Map.Entry<String, Object>>) fields.invoke(node));
  }
}
