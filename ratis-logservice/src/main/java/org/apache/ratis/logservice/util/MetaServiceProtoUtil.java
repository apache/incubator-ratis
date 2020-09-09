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


import org.apache.ratis.logservice.api.LogInfo;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.proto.LogServiceProtos;
import org.apache.ratis.logservice.proto.MetaServiceProtos;
import org.apache.ratis.logservice.proto.MetaServiceProtos.CreateLogReplyProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.CreateLogRequestProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.DeleteLogReplyProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.DeleteLogRequestProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.GetLogReplyProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.GetLogRequestProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.ListLogsReplyProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.ListLogsRequestProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.LogInfoProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.MetaSMRequestProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.MetaServiceExceptionProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.MetaServiceRequestProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.RaftGroupProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.RaftPeerProto;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.ReflectionUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.ratis.logservice.util.LogServiceProtoUtil.toLogNameProto;

public final class MetaServiceProtoUtil {
    private MetaServiceProtoUtil() {
    }

    public static RaftPeerProto toRaftPeerProto(RaftPeer peer) {
        RaftPeerProto.Builder builder = RaftPeerProto.newBuilder()
                .setId(peer.getId().toByteString());
        if (peer.getAddress() != null) {
            builder.setAddress(peer.getAddress());
        }
        return builder.build();
    }

    public static RaftPeer toRaftPeer(RaftPeerProto p) {
        return new RaftPeer(RaftPeerId.valueOf(p.getId()), p.getAddress());
    }

    public static RaftGroup toRaftGroup(RaftGroupProto proto) {
        return RaftGroup.valueOf(RaftGroupId.valueOf(proto.getId()), toRaftPeerArray(proto.getPeersList()));
    }

    public static RaftGroupProto toRaftGroupProto(RaftGroup group) {
        return RaftGroupProto.newBuilder()
                .setId(group.getGroupId().toByteString())
                .addAllPeers(toRaftPeerProtos(group.getPeers())).build();
    }

    public static LogInfo toLogInfo(LogInfoProto logProto) {
        return new LogInfo(LogServiceProtoUtil.toLogName(logProto.getLogname()), toRaftGroup(logProto.getRaftGroup()));
    }

    public static LogInfoProto toLogInfoProto (LogInfo logInfo) {
        return LogInfoProto.newBuilder()
                .setLogname(toLogNameProto(logInfo.getLogName()))
                .setRaftGroup(toRaftGroupProto(logInfo.getRaftGroup()))
                .build();
    }

    public static MetaSMRequestProto toPingRequestProto(RaftPeer peer) {
        return MetaServiceProtos.MetaSMRequestProto
                .newBuilder()
                .setPingRequest(
                        MetaServiceProtos.LogServicePingRequestProto
                                .newBuilder()
                                .setPeer(MetaServiceProtoUtil.toRaftPeerProto(peer)).build()).build();
    }

    public static MetaSMRequestProto toHeartbeatRequestProto(RaftPeer peer) {
        return MetaServiceProtos.MetaSMRequestProto
                .newBuilder()
                .setHeartbeatRequest(
                        MetaServiceProtos.LogServiceHeartbeatRequestProto
                                .newBuilder()
                                .setPeer(MetaServiceProtoUtil.toRaftPeerProto(peer)).build()).build();
    }

    public static MetaServiceRequestProto toCreateLogRequestProto(LogName logName) {
        LogServiceProtos.LogNameProto logNameProto = LogServiceProtos.LogNameProto.newBuilder()
                .setName(logName.getName())
                .build();
        CreateLogRequestProto createLog =
                CreateLogRequestProto.newBuilder().setLogName(logNameProto).build();
        return MetaServiceRequestProto.newBuilder().setCreateLog(createLog).build();
    }

    public static MetaServiceRequestProto toListLogRequestProto() {
        ListLogsRequestProto listLogs = ListLogsRequestProto.newBuilder().build();
        return MetaServiceRequestProto.newBuilder().setListLogs(listLogs).build();
    }

    public static MetaServiceRequestProto toGetLogRequestProto(LogName name) {
        GetLogRequestProto getLog =
                GetLogRequestProto.newBuilder().setLogName(toLogNameProto(name)).build();
        return MetaServiceRequestProto.newBuilder().setGetLog(getLog).build();
    }

    public static MetaServiceRequestProto toDeleteLogRequestProto(LogName logName) {
        LogServiceProtos.LogNameProto logNameProto = LogServiceProtos.LogNameProto.newBuilder()
                .setName(logName.getName())
                .build();
        DeleteLogRequestProto deleteLog =
                DeleteLogRequestProto.newBuilder().setLogName(logNameProto).build();
        return MetaServiceRequestProto.newBuilder().setDeleteLog(deleteLog).build();
    }

    public static CreateLogReplyProto.Builder toCreateLogReplyProto(LogInfo logInfo) {
        return CreateLogReplyProto.newBuilder().setLog(toLogInfoProto(logInfo));
    }

    public static ListLogsReplyProto toListLogLogsReplyProto(List<LogInfo> logInfos) {
        return ListLogsReplyProto.newBuilder().addAllLogs(
                logInfos.stream()
                        .map(log -> toLogInfoProto(log))
                        .collect(Collectors.toList())).build();
    }

    public static GetLogReplyProto toGetLogReplyProto(LogInfo logInfo) {
        return GetLogReplyProto.newBuilder().setLog(toLogInfoProto(logInfo)).build();
    }

    public static MetaServiceExceptionProto toMetaServiceExceptionProto(Exception exception) {
        final Throwable t = exception.getCause() != null ? exception.getCause() : exception;
        return MetaServiceExceptionProto.newBuilder()
                .setExceptionClassName(t.getClass().getName())
                .setErrorMsg(t.getMessage())
                .setStacktrace(ProtoUtils.writeObject2ByteString(t.getStackTrace()))
                .build();
    }

    public static CreateLogReplyProto.Builder toCreateLogExceptionReplyProto(Exception e) {
        return CreateLogReplyProto.newBuilder().setException(toMetaServiceExceptionProto(e));
    }

    public static GetLogReplyProto.Builder toGetLogExceptionReplyProto(Exception e) {
        return GetLogReplyProto.newBuilder().setException(toMetaServiceExceptionProto(e));
    }

    public static DeleteLogReplyProto.Builder toDeleteLogExceptionReplyProto(Exception e) {
        return DeleteLogReplyProto.newBuilder().setException(toMetaServiceExceptionProto(e));
    }

    public static IOException toMetaServiceException(MetaServiceExceptionProto exceptionProto) {
        try {
            IOException result = null;
            Class<?> clazz = Class.forName(exceptionProto.getExceptionClassName());
            Exception e = ReflectionUtils.instantiateException(
                    clazz.asSubclass(Exception.class), exceptionProto.getErrorMsg(), null);
            if(e instanceof IOException) {
                result = (IOException)e;
            } else {
                result = new IOException(e);
            }
            StackTraceElement[] stacktrace =
                    (StackTraceElement[]) ProtoUtils.toObject(exceptionProto.getStacktrace());
            result.setStackTrace(stacktrace);

            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Internal methods
    static RaftPeer[] toRaftPeerArray(List<RaftPeerProto> protos) {
        final RaftPeer[] peers = new RaftPeer[protos.size()];
        for (int i = 0; i < peers.length; i++) {
            peers[i] = toRaftPeer(protos.get(i));
        }
        return peers;
    }

    static Iterable<RaftPeerProto> toRaftPeerProtos(
            final Collection<RaftPeer> peers) {
        return () -> new Iterator<RaftPeerProto>() {
            private final Iterator<RaftPeer> i = peers.iterator();

            @Override
            public boolean hasNext() {
                return i.hasNext();
            }

            @Override
            public RaftPeerProto next() {
                return toRaftPeerProto(i.next());
            }
        };
    }

    public static DeleteLogReplyProto toDeleteLogReplyProto() {
        return DeleteLogReplyProto.newBuilder().build();
    }
}
