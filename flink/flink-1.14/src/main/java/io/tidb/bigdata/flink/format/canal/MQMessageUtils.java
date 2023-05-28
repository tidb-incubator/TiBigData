/*
 * Copyright 2023 TiDB Project Authors.
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

package io.tidb.bigdata.flink.format.canal;

import com.alibaba.otter.canal.common.utils.ExecutorTemplate;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import javax.annotation.Nullable;

public class MQMessageUtils {

  public static List<FlatMessage> getFlatMessages(Message message) {
    EntryRowData[] datas = MQMessageUtils.buildMessageData(message, null);
    return MQMessageUtils.messageConverter(datas, message.getId());
  }

  public static EntryRowData[] buildMessageData(
      Message message, @Nullable ThreadPoolExecutor executor) {
    Optional<ExecutorTemplate> template = Optional.ofNullable(executor).map(ExecutorTemplate::new);
    if (message.isRaw()) {
      List<ByteString> rawEntries = message.getRawEntries();
      final EntryRowData[] datas = new EntryRowData[rawEntries.size()];
      int i = 0;
      for (ByteString byteString : rawEntries) {
        final int index = i;
        Runnable task =
            () -> {
              try {
                Entry entry = Entry.parseFrom(byteString);
                RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
                datas[index] = new EntryRowData();
                datas[index].entry = entry;
                datas[index].rowChange = rowChange;
              } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
              }
            };
        if (template.isPresent()) {
          template.get().submit(task);
        } else {
          task.run();
        }
        i++;
      }
      template.ifPresent(ExecutorTemplate::waitForResult);
      return datas;
    } else {
      final EntryRowData[] datas = new EntryRowData[message.getEntries().size()];
      int i = 0;
      for (Entry entry : message.getEntries()) {
        final int index = i;
        Runnable task =
            () -> {
              try {
                RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
                datas[index] = new EntryRowData();
                datas[index].entry = entry;
                datas[index].rowChange = rowChange;
              } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
              }
            };
        if (template.isPresent()) {
          template.get().submit(task);
        } else {
          task.run();
        }
        i++;
      }
      template.ifPresent(ExecutorTemplate::waitForResult);
      return datas;
    }
  }

  public static List<FlatMessage> messageConverter(EntryRowData[] datas, long id) {
    List<FlatMessage> flatMessages = new ArrayList<>();
    for (EntryRowData entryRowData : datas) {
      Entry entry = entryRowData.entry;
      RowChange rowChange = entryRowData.rowChange;
      if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
          || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
        continue;
      }

      // build flatMessage
      CanalEntry.EventType eventType = rowChange.getEventType();
      FlatMessage flatMessage = new FlatMessage(id);
      flatMessages.add(flatMessage);
      flatMessage.setDatabase(entry.getHeader().getSchemaName());
      flatMessage.setTable(entry.getHeader().getTableName());
      flatMessage.setIsDdl(rowChange.getIsDdl());
      flatMessage.setType(eventType.toString());
      flatMessage.setEs(entry.getHeader().getExecuteTime());
      flatMessage.setTs(System.currentTimeMillis());
      flatMessage.setSql(rowChange.getSql());

      if (!rowChange.getIsDdl()) {
        Map<String, Integer> sqlType = new LinkedHashMap<>();
        Map<String, String> mysqlType = new LinkedHashMap<>();
        List<Map<String, String>> data = new ArrayList<>();
        List<Map<String, String>> old = new ArrayList<>();

        Set<String> updateSet = new HashSet<>();
        boolean hasInitPkNames = false;
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
          if (eventType != CanalEntry.EventType.INSERT
              && eventType != CanalEntry.EventType.UPDATE
              && eventType != CanalEntry.EventType.DELETE) {
            continue;
          }

          Map<String, String> row = new LinkedHashMap<>();
          List<CanalEntry.Column> columns;

          if (eventType == CanalEntry.EventType.DELETE) {
            columns = rowData.getBeforeColumnsList();
          } else {
            columns = rowData.getAfterColumnsList();
          }

          for (CanalEntry.Column column : columns) {
            if (!hasInitPkNames && column.getIsKey()) {
              flatMessage.addPkName(column.getName());
            }
            sqlType.put(column.getName(), column.getSqlType());
            mysqlType.put(column.getName(), column.getMysqlType());
            if (column.getIsNull()) {
              row.put(column.getName(), null);
            } else {
              row.put(column.getName(), column.getValue());
            }
            if (column.getUpdated()) {
              updateSet.add(column.getName());
            }
          }

          hasInitPkNames = true;
          if (!row.isEmpty()) {
            data.add(row);
          }

          if (eventType == CanalEntry.EventType.UPDATE) {
            Map<String, String> rowOld = new LinkedHashMap<>();
            for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
              if (updateSet.contains(column.getName())) {
                if (column.getIsNull()) {
                  rowOld.put(column.getName(), null);
                } else {
                  rowOld.put(column.getName(), column.getValue());
                }
              }
            }
            if (!rowOld.isEmpty()) {
              old.add(rowOld);
            }
          }
        }
        if (!sqlType.isEmpty()) {
          flatMessage.setSqlType(sqlType);
        }
        if (!mysqlType.isEmpty()) {
          flatMessage.setMysqlType(mysqlType);
        }
        if (!data.isEmpty()) {
          flatMessage.setData(data);
        }
        if (!old.isEmpty()) {
          flatMessage.setOld(old);
        }
      }
    }
    return flatMessages;
  }

  public static class EntryRowData {

    public Entry entry;
    public RowChange rowChange;
  }
}
