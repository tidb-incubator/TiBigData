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

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalPacket.Ack;
import com.alibaba.otter.canal.protocol.CanalPacket.Compression;
import com.alibaba.otter.canal.protocol.CanalPacket.Messages;
import com.alibaba.otter.canal.protocol.CanalPacket.Packet;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.ByteString;
import java.util.Iterator;

public class CanalMessageDeserializer {

  public static Message deserializer(byte[] data) {
    return deserializer(data, false);
  }

  public static Message deserializer(byte[] data, boolean lazyParseEntry) {
    try {
      if (data == null) {
        return null;
      } else {
        Packet p = Packet.parseFrom(data);
        switch (p.getType()) {
          case MESSAGES:
            if (!p.getCompression().equals(Compression.NONE)
                && !p.getCompression().equals(Compression.COMPRESSIONCOMPATIBLEPROTO2)) {
              throw new CanalClientException("compression is not supported in this connector");
            }

            Messages messages = Messages.parseFrom(p.getBody());
            Message result = new Message(messages.getBatchId());
            if (lazyParseEntry) {
              result.setRawEntries(messages.getMessagesList());
              result.setRaw(true);
            } else {
              Iterator var5 = messages.getMessagesList().iterator();

              while (var5.hasNext()) {
                ByteString byteString = (ByteString) var5.next();
                result.addEntry(Entry.parseFrom(byteString));
              }

              result.setRaw(false);
            }

            return result;
          case ACK:
            Ack ack = Ack.parseFrom(p.getBody());
            throw new CanalClientException(
                "something goes wrong with reason: " + ack.getErrorMessage());
          default:
            throw new CanalClientException("unexpected packet type: " + p.getType());
        }
      }
    } catch (Exception var7) {
      throw new CanalClientException("deserializer failed", var7);
    }
  }
}
