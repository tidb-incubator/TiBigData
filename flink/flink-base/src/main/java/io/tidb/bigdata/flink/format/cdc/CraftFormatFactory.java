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

import static io.tidb.bigdata.flink.format.cdc.FormatOptions.IGNORE_PARSE_ERRORS;
import static io.tidb.bigdata.flink.format.cdc.FormatOptions.SCHEMA_INCLUDE;
import static io.tidb.bigdata.flink.format.cdc.FormatOptions.TABLE_INCLUDE;
import static io.tidb.bigdata.flink.format.cdc.FormatOptions.validateDecodingFormatOptions;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

public class CraftFormatFactory
    implements DeserializationFormatFactory, SerializationFormatFactory {

  @Override
  public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
      final DynamicTableFactory.Context context, final ReadableConfig formatOptions) {
    FactoryUtil.validateFactoryOptions(this, formatOptions);
    validateDecodingFormatOptions(formatOptions);

    final boolean ignoreParseErrors = formatOptions.get(FormatOptions.IGNORE_PARSE_ERRORS);
    final String schema = formatOptions.getOptional(SCHEMA_INCLUDE).orElse(null);
    final String table = formatOptions.getOptional(TABLE_INCLUDE).orElse(null);

    return new DecodingFormat<DeserializationSchema<RowData>>() {
      @Override
      public DeserializationSchema<RowData> createRuntimeDecoder(
          DynamicTableSource.Context context, DataType producedDataType) {
        final RowType rowType = (RowType) producedDataType.getLogicalType();
        final TypeInformation<RowData> rowDataTypeInfo =
            (TypeInformation<RowData>) context.createTypeInformation(producedDataType);
        return CraftDeserializationSchema.builder(rowType, rowDataTypeInfo)
            .setIgnoreParseErrors(ignoreParseErrors)
            .setSchema(schema)
            .setTable(table)
            .build();
      }

      @Override
      public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
            .addContainedKind(RowKind.INSERT)
            .addContainedKind(RowKind.UPDATE_BEFORE)
            .addContainedKind(RowKind.UPDATE_AFTER)
            .addContainedKind(RowKind.DELETE)
            .build();
      }
    };
  }

  @Override
  public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(Context context,
      ReadableConfig readableConfig) {
    return null;
  }

  @Override
  public String factoryIdentifier() {
    return "ticdc-craft";
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
    return options;
  }
}