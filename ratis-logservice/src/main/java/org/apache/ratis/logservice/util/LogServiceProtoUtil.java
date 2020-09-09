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

package org.apache.ratis.logservice.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogStream.State;
import org.apache.ratis.logservice.proto.LogServiceProtos;
import org.apache.ratis.logservice.proto.LogServiceProtos.AppendLogEntryReplyProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.AppendLogEntryRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.ArchivalStatus;
import org.apache.ratis.logservice.proto.LogServiceProtos.ArchiveLogReplyProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.ArchiveLogRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.ChangeStateLogRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetExportInfoRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetLogLastCommittedIndexReplyProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetLogLastCommittedIndexRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetLogLengthReplyProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetLogLengthRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetLogSizeReplyProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetLogSizeRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetLogStartIndexReplyProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetLogStartIndexRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetStateReplyProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetStateRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.LogNameProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.LogServiceException;
import org.apache.ratis.logservice.proto.LogServiceProtos.LogServiceRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.LogStreamProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.LogStreamState;
import org.apache.ratis.logservice.proto.LogServiceProtos.ReadLogReplyProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.ReadLogRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.SyncLogReplyProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.SyncLogRequestProto;
import org.apache.ratis.logservice.server.ArchivalInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

public class LogServiceProtoUtil {

  public static LogNameProto toLogNameProto(LogName logName) {
    return LogNameProto.newBuilder().setName(logName.getName()).build();
  }

  public static LogName toLogName(LogServiceProtos.LogNameProto logNameProto) {
    return LogName.of(logNameProto.getName());
  }

  public static LogStreamProto toLogStreamProto(LogStream logStream) throws IOException {
    LogNameProto logNameProto =
            LogNameProto.newBuilder().setName(logStream.getName().getName()).build();
    LogStreamProto logStreamProto =
            LogStreamProto
                    .newBuilder()
                    .setLogName(logNameProto)
                    .setSize(logStream.getSize())
                    .setState(
                            logStream.getState().equals(State.OPEN) ? LogStreamState.OPEN : LogStreamState.CLOSED)
                    .build();
    return logStreamProto;
  }

  public static LogServiceRequestProto toChangeStateRequestProto(LogName logName, State state,
      boolean force) {
    LogNameProto logNameProto = LogNameProto.newBuilder().setName(logName.getName()).build();
    ChangeStateLogRequestProto changeLog =
        ChangeStateLogRequestProto.newBuilder().setLogName(logNameProto)
            .setState(LogStreamState.valueOf(state.name())).setForce(force).build();
    return LogServiceRequestProto.newBuilder().setChangeState(changeLog).build();
  }

  public static LogServiceRequestProto toChangeStateRequestProto(LogName logName, State state) {
    return toChangeStateRequestProto(logName, state, false);
  }

  public static LogServiceRequestProto toGetStateRequestProto(LogName logName) {
    LogNameProto logNameProto = LogNameProto.newBuilder().setName(logName.getName()).build();
    GetStateRequestProto getState =
        GetStateRequestProto.newBuilder().setLogName(logNameProto).build();
    return LogServiceRequestProto.newBuilder().setGetState(getState).build();
  }

  public static ArchiveLogReplyProto toArchiveLogReplyProto(Throwable t) {
    ArchiveLogReplyProto.Builder builder = ArchiveLogReplyProto.newBuilder();
    if (t != null) {
      builder.setException(toLogException(t));
    }
    return builder.build();
  }

  public static LogServiceRequestProto toGetSizeRequestProto(LogName name) {
    LogNameProto logNameProto =
        LogNameProto.newBuilder().setName(name.getName()).build();
    GetLogSizeRequestProto getLogSize = GetLogSizeRequestProto.newBuilder()
        .setLogName(logNameProto).build();
    return LogServiceRequestProto.newBuilder().setSizeRequest(getLogSize).build();
  }

  public static LogServiceRequestProto toGetLengthRequestProto(LogName name) {
    LogNameProto logNameProto =
        LogNameProto.newBuilder().setName(name.getName()).build();
    GetLogLengthRequestProto.Builder builder = GetLogLengthRequestProto.newBuilder();
    builder.setLogName(logNameProto);
    return LogServiceRequestProto.newBuilder().setLengthQuery(builder.build()).build();
  }

  public static LogServiceRequestProto toGetLastCommittedIndexRequestProto(LogName name) {
    LogNameProto logNameProto =
        LogNameProto.newBuilder().setName(name.getName()).build();
    GetLogLastCommittedIndexRequestProto.Builder builder =
        GetLogLastCommittedIndexRequestProto.newBuilder();
    builder.setLogName(logNameProto);
    return LogServiceRequestProto.newBuilder().setLastIndexQuery(builder.build()).build();
  }

  public static LogServiceRequestProto toGetStartIndexProto(LogName name) {
    LogNameProto logNameProto =
        LogNameProto.newBuilder().setName(name.getName()).build();
    GetLogStartIndexRequestProto.Builder builder = GetLogStartIndexRequestProto.newBuilder();
    builder.setLogName(logNameProto);
    return LogServiceRequestProto.newBuilder().setStartIndexQuery(builder.build()).build();
  }

  public static LogServiceRequestProto toReadLogRequestProto(LogName name, long start, int total) {
    LogNameProto logNameProto =
        LogNameProto.newBuilder().setName(name.getName()).build();
    ReadLogRequestProto.Builder builder = ReadLogRequestProto.newBuilder();
    builder.setLogName(logNameProto);
    builder.setStartRecordId(start);
    builder.setNumRecords(total);
    return LogServiceRequestProto.newBuilder().setReadNextQuery(builder.build()).build();
  }

  public static LogServiceRequestProto toSyncLogRequestProto(LogName name) {
    LogNameProto logNameProto =
        LogNameProto.newBuilder().setName(name.getName()).build();
    SyncLogRequestProto.Builder builder = SyncLogRequestProto.newBuilder();
    builder.setLogName(logNameProto);
    return LogServiceRequestProto.newBuilder().setSyncRequest(builder.build()).build();
  }

  public static LogServiceRequestProto toAppendEntryLogRequestProto(LogName name,
      List<byte[]> entries) {
    LogNameProto logNameProto =
        LogNameProto.newBuilder().setName(name.getName()).build();
    AppendLogEntryRequestProto.Builder builder = AppendLogEntryRequestProto.newBuilder();
    builder.setLogName(logNameProto);
    for (int i=0; i < entries.size(); i++) {
      builder.addData(ByteString.copyFrom(entries.get(i)));
    }
    return LogServiceRequestProto.newBuilder().setAppendRequest(builder.build()).build();
  }

  public static LogServiceRequestProto toAppendBBEntryLogRequestProto(LogName name,
      List<ByteBuffer> entries) {
    LogNameProto logNameProto =
        LogNameProto.newBuilder().setName(name.getName()).build();
    AppendLogEntryRequestProto.Builder builder = AppendLogEntryRequestProto.newBuilder();
    builder.setLogName(logNameProto);
    for (int i=0; i < entries.size(); i++) {
      ByteBuffer currentBuf = entries.get(i);
      // Save the current position
      int pos = currentBuf.position();
      builder.addData(ByteString.copyFrom(currentBuf));
      // Reset it after we're done reading the bytes
      currentBuf.position(pos);
    }
    return LogServiceRequestProto.newBuilder().setAppendRequest(builder.build()).build();
  }

  public static List<byte[]> toListByteArray(List<ByteString> list) {
    List<byte[]> retVal = new ArrayList<byte[]>(list.size());
    for(int i=0; i < list.size(); i++) {
      retVal.add(list.get(i).toByteArray());
    }
    return retVal;
  }

  public static GetStateReplyProto toGetStateReplyProto(State state) {
    return GetStateReplyProto.newBuilder().setState(LogStreamState.valueOf(state.name())).build();
  }

  public static GetLogLengthReplyProto toGetLogLengthReplyProto(long length, Throwable t) {
    GetLogLengthReplyProto.Builder builder = GetLogLengthReplyProto.newBuilder();
    if (t != null) {
      builder.setException(toLogException(t));
    } else {
      builder.setLength(length);
    }
    return builder.build();
  }

  public static GetLogSizeReplyProto toGetLogSizeReplyProto(long size, Throwable t) {
    GetLogSizeReplyProto.Builder builder = GetLogSizeReplyProto.newBuilder();
    if (t != null) {
      builder.setException(toLogException(t));
    } else {
      builder.setSize(size);
    }
    return builder.build();
  }

  public static GetLogStartIndexReplyProto toGetLogStartIndexReplyProto(long length, Throwable t) {
    GetLogStartIndexReplyProto.Builder builder = GetLogStartIndexReplyProto.newBuilder();
    if (t != null) {
      builder.setException(toLogException(t));
    } else {
      builder.setStartIndex(length);
    }
    return builder.build();
  }

  public static GetLogLastCommittedIndexReplyProto
        toGetLogLastIndexReplyProto(long lastIndex, Throwable t) {

    GetLogLastCommittedIndexReplyProto.Builder builder =
        GetLogLastCommittedIndexReplyProto.newBuilder();
    if (t != null) {
      builder.setException(toLogException(t));
    } else {
      builder.setLastCommittedIndex(lastIndex);
    }
    return builder.build();
  }

  public static ReadLogReplyProto toReadLogReplyProto(List<byte[]> entries, Throwable t) {
    ReadLogReplyProto.Builder builder = ReadLogReplyProto.newBuilder();
    if (t != null) {
      builder.setException(toLogException(t));
    } else {
      for(byte[] record: entries) {
        builder.addLogRecord( ByteString.copyFrom(record));
      }
    }
    return builder.build();
  }

  public static AppendLogEntryReplyProto toAppendLogReplyProto(List<Long> ids, Throwable t) {
    AppendLogEntryReplyProto.Builder builder = AppendLogEntryReplyProto.newBuilder();
    if (t != null) {
      builder.setException(toLogException(t));
    } else if (ids != null) {
      for (long id: ids) {
        builder.addRecordId(id);
      }
    }
    return builder.build();
  }

  public static SyncLogReplyProto toSyncLogReplyProto(long index, Throwable t) {
    SyncLogReplyProto.Builder builder = SyncLogReplyProto.newBuilder();
    if (t != null) {
      builder.setException(toLogException(t));
    } else {
      builder.setLastRecordId(index);
    }
    return builder.build();
  }

  public static ArchiveLogRequestProto toExportInfoProto(ArchivalInfo info) {
    return ArchiveLogRequestProto.newBuilder().setIsExport(true)
        .setLastArchivedRaftIndex(info.getLastArchivedIndex())
        .setLocation(info.getArchiveLocation()).setLogName(
            LogServiceProtos.LogNameProto.newBuilder().setName(info.getArchiveLogName().getName())
                .build()).setStatus(ArchivalStatus.valueOf(info.getStatus().name())).build();
  }

  public static ArchivalInfo toExportInfo(ArchiveLogRequestProto proto){
    return new ArchivalInfo(proto.getLocation()).updateArchivalInfo(proto);

  }

  public GetLogLengthReplyProto toGetLogLengthReplyProto(long length) {
    GetLogLengthReplyProto.Builder builder = GetLogLengthReplyProto.newBuilder();
    builder.setLength(length);
    return builder.build();
  }

  public static LogServiceException toLogException(Throwable t) {
    LogServiceException.Builder builder = LogServiceException.newBuilder();
    builder.setExceptionClassName(t.getClass().getName());
    builder.setErrorMsg(t.getMessage());
    StackTraceElement[] trace = t.getStackTrace();
    StringBuffer buf = new StringBuffer();
    for (StackTraceElement el: trace) {
      buf.append(el.toString()).append("\n");
    }
    String strace = buf.toString();
    builder.setStacktrace(ByteString.copyFrom(strace, Charset.defaultCharset()));
    return builder.build();
  }

  public static LogServiceRequestProto toExportInfoRequestProto(LogName logName){
    LogServiceProtos.LogNameProto logNameProto =
        LogServiceProtos.LogNameProto.newBuilder().setName(logName.getName()).build();
    GetExportInfoRequestProto exportInfoRequestProto =
        GetExportInfoRequestProto.newBuilder().setLogName(logNameProto).build();
    return LogServiceRequestProto.newBuilder().setExportInfo(exportInfoRequestProto).build();
  }

  public static LogServiceRequestProto toArchiveLogRequestProto(LogName logName, String location,
      long raftIndex, boolean isArchival, ArchivalInfo.ArchivalStatus status) {
    LogServiceProtos.LogNameProto logNameProto =
        LogServiceProtos.LogNameProto.newBuilder().setName(logName.getName()).build();
    ArchiveLogRequestProto.Builder builder =
        ArchiveLogRequestProto.newBuilder().setLogName(logNameProto)
            .setLastArchivedRaftIndex(raftIndex).setStatus(ArchivalStatus.valueOf(status.name()));
    builder.setIsExport(!isArchival);
    if (location != null) {
      builder.setLocation(location);
    }
    ArchiveLogRequestProto archiveLog = builder.build();
    return LogServiceRequestProto.newBuilder().setArchiveLog(archiveLog).build();
  }
}
