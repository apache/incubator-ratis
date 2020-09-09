/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.logservice.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.logservice.api.LogReader;
import org.apache.ratis.logservice.api.LogServiceConfiguration;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.proto.LogServiceProtos.LogServiceException;
import org.apache.ratis.logservice.proto.LogServiceProtos.ReadLogReplyProto;
import org.apache.ratis.logservice.util.LogServiceProtoUtil;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Log Reader implementation. This class is not thread-safe
 *
 */

public class LogReaderImpl implements LogReader {
  public static final Logger LOG = LoggerFactory.getLogger(LogReaderImpl.class);

  /*
   * Parent log stream
   */
  private LogStream parent;
  /*
   * Raft client
   */
  private RaftClient   raftClient;
  /*
   * Log service configuration object
   */
  private LogServiceConfiguration config;

  /*
   * offset
   */
  private long currentRecordId;

  public LogReaderImpl(LogStream logStream) {
    this.parent = logStream;
    this.raftClient = logStream.getRaftClient();
    this.config = logStream.getConfiguration();
  }

  @Override
  public void seek(long recordId) throws IOException {
    Preconditions.checkArgument(recordId >= 0, "recordId must be >= 0");
    this.currentRecordId = recordId;
  }

  @Override
  public ByteBuffer readNext() throws IOException {

    try {
      RaftClientReply reply =
          raftClient
              .sendReadOnly(Message.valueOf(LogServiceProtoUtil
                  .toReadLogRequestProto(parent.getName(), currentRecordId, 1).toByteString()));
      if (reply.getException() != null) {
        throw new IOException(reply.getException());
      }

      ReadLogReplyProto proto = ReadLogReplyProto.parseFrom(reply.getMessage().getContent());
      if (proto.hasException()) {
        LogServiceException e = proto.getException();
        throw new IOException(e.getErrorMsg());
      }

      currentRecordId++;

      if (proto.getLogRecordCount() > 0) {
        proto.getLogRecord(0);
        return ByteBuffer.wrap(proto.getLogRecord(0).toByteArray());
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void readNext(ByteBuffer buffer) throws IOException {

    Preconditions.checkNotNull(buffer, "buffer is NULL" );
    try {
      RaftClientReply reply = raftClient.sendReadOnly(Message.valueOf(LogServiceProtoUtil
          .toReadLogRequestProto(parent.getName(), currentRecordId, 1).toByteString()));
      if (reply.getException() != null) {
        throw new IOException(reply.getException());
      }

      ReadLogReplyProto proto = ReadLogReplyProto.parseFrom(reply.getMessage().getContent());
      if (proto.hasException()) {
        LogServiceException e = proto.getException();
        throw new IOException(e.getErrorMsg());
      }
      currentRecordId++;
      if (proto.getLogRecordCount() > 0) {
        // TODO limits
        buffer.put(proto.getLogRecord(0).toByteArray());
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<ByteBuffer> readBulk(int numRecords) throws IOException {
    Preconditions.checkArgument(numRecords > 0, "number of records must be greater than 0");

    try {
      RaftClientReply reply = raftClient
          .sendReadOnly(Message.valueOf(LogServiceProtoUtil
              .toReadLogRequestProto(parent.getName(), currentRecordId, numRecords).toByteString()));
      if (reply.getException() != null) {
        throw new IOException(reply.getException());
      }

      ReadLogReplyProto proto = ReadLogReplyProto.parseFrom(reply.getMessage().getContent());
      if (proto.hasException()) {
        LogServiceException e = proto.getException();
        throw new IOException(e.getErrorMsg());
      }
      int n = proto.getLogRecordCount();

      // TODO correct current record
      currentRecordId += n;
      List<ByteBuffer> ret = new ArrayList<ByteBuffer>();
      for (int i = 0; i < n; i++) {
        ret.add(ByteBuffer.wrap(proto.getLogRecord(i).toByteArray()));
      }
      return ret;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public int readBulk(ByteBuffer[] buffers) throws IOException {
    Preconditions.checkNotNull(buffers, "list of buffers is NULL" );
    Preconditions.checkArgument(buffers.length > 0, "list of buffers is empty");

    try {
      RaftClientReply reply = raftClient.sendReadOnly(Message.valueOf(LogServiceProtoUtil
          .toReadLogRequestProto(parent.getName(), currentRecordId, buffers.length).toByteString()));
      if (reply.getException() != null) {
        throw new IOException(reply.getException());
      }

      ReadLogReplyProto proto = ReadLogReplyProto.parseFrom(reply.getMessage().getContent());
      if (proto.hasException()) {
        LogServiceException e = proto.getException();
        throw new IOException(e.getErrorMsg());
      }
      // TODO correct current record
      int n = proto.getLogRecordCount();
      currentRecordId += n;
      for (int i = 0; i < n; i++) {
        buffers[i] = ByteBuffer.wrap(proto.getLogRecord(i).toByteArray());
      }
      return n;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getPosition() {
    return currentRecordId;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

}
