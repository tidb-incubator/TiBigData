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

package io.tidb.bigdata.flink.format.cdc;

import static io.tidb.bigdata.flink.format.cdc.FormatOptions.EARLIEST_TIMESTAMP;
import static io.tidb.bigdata.flink.format.cdc.FormatOptions.EARLIEST_VERSION;
import static io.tidb.bigdata.flink.format.cdc.FormatOptions.IGNORE_PARSE_ERRORS;
import static io.tidb.bigdata.flink.format.cdc.FormatOptions.SCHEMA_INCLUDE;
import static io.tidb.bigdata.flink.format.cdc.FormatOptions.TABLE_INCLUDE;
import static io.tidb.bigdata.flink.format.cdc.FormatOptions.TYPE_INCLUDE;
import static io.tidb.bigdata.flink.format.cdc.FormatOptions.getEarliestTs;
import static io.tidb.bigdata.flink.format.cdc.FormatOptions.getOptionalSet;
import static io.tidb.bigdata.flink.format.cdc.FormatOptions.validateDecodingFormatOptions;

import io.tidb.bigdata.cdc.Key.Type;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;

public class CraftFormatFactory
    implements DeserializationFormatFactory, SerializationFormatFactory {

  public static final String IDENTIFIER = "ticdc-craft";

  @Override
  public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
      final Context context, final ReadableConfig formatOptions) {
    FactoryUtil.validateFactoryOptions(this, formatOptions);
    validateDecodingFormatOptions(formatOptions);

    final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
    final Set<Type> types =
        getOptionalSet(formatOptions, TYPE_INCLUDE, s -> Type.valueOf(s.toUpperCase()));
    final Set<String> schemas = getOptionalSet(formatOptions, SCHEMA_INCLUDE);
    final Set<String> tables = getOptionalSet(formatOptions, TABLE_INCLUDE);
    final long earliestTs = getEarliestTs(formatOptions);

    return new DecodingFormat<DeserializationSchema<RowData>>() {
      private List<String> metadataKeys = Collections.emptyList();

      @Override
      public DeserializationSchema<RowData> createRuntimeDecoder(
          DynamicTableSource.Context context, DataType physicalDataType) {

        final List<ReadableMetadata> readableMetadata =
            metadataKeys.stream()
                .map(
                    k ->
                        Stream.of(ReadableMetadata.values())
                            .filter(rm -> rm.key.equals(k))
                            .findFirst()
                            .orElseThrow(IllegalStateException::new))
                .collect(Collectors.toList());

        final List<DataTypes.Field> metadataFields =
            readableMetadata.stream()
                .map(m -> DataTypes.FIELD(m.key, m.type))
                .collect(Collectors.toList());

        final DataType producedDataType = DataTypeUtils
            .appendRowFields(physicalDataType, metadataFields);
        final RowType rowType = (RowType) physicalDataType.getLogicalType();
        final TypeInformation<RowData> resultTypeInfo =
            context.createTypeInformation(producedDataType);
        return new CraftDeserializationSchema(rowType, readableMetadata,
            resultTypeInfo, earliestTs, types, schemas, tables, ignoreParseErrors);
      }

      @Override
      public ChangelogMode getChangelogMode() {
        return CraftFormatFactory.getChangelogMode();
      }

      @Override
      public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        Stream.of(ReadableMetadata.values())
            .forEachOrdered(m -> metadataMap.put(m.key, m.type));
        return metadataMap;
      }

      @Override
      public void applyReadableMetadata(List<String> metadataKeys) {
        this.metadataKeys = metadataKeys;
      }
    };
  }

  public static ChangelogMode getChangelogMode() {
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.DELETE)
        .addContainedKind(RowKind.UPDATE_BEFORE)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .build();
  }

  @Override
  public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(Context context,
      ReadableConfig readableConfig) {
    return null;
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(IGNORE_PARSE_ERRORS);
    options.add(SCHEMA_INCLUDE);
    options.add(TABLE_INCLUDE);
    options.add(TYPE_INCLUDE);
    options.add(EARLIEST_VERSION);
    options.add(EARLIEST_TIMESTAMP);
    return options;
  }
}
