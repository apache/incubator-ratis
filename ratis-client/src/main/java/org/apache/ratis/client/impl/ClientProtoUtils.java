/*
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
package org.apache.ratis.client.impl;

import java.util.Optional;
import org.apache.ratis.proto.RaftProtos.AlreadyClosedExceptionProto;
import org.apache.ratis.proto.RaftProtos.ClientMessageEntryProto;
import org.apache.ratis.proto.RaftProtos.GroupAddRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupInfoReplyProto;
import org.apache.ratis.proto.RaftProtos.GroupInfoRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupListReplyProto;
import org.apache.ratis.proto.RaftProtos.GroupListRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupManagementRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupRemoveRequestProto;
import org.apache.ratis.proto.RaftProtos.LeaderNotReadyExceptionProto;
import org.apache.ratis.proto.RaftProtos.NotLeaderExceptionProto;
import org.apache.ratis.proto.RaftProtos.NotReplicatedExceptionProto;
import org.apache.ratis.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcReplyProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.proto.RaftProtos.SlidingWindowEntry;
import org.apache.ratis.proto.RaftProtos.StateMachineExceptionProto;
import org.apache.ratis.proto.RaftProtos.WriteRequestTypeProto;
import org.apache.ratis.protocol.AlreadyClosedException;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.GroupListReply;
import org.apache.ratis.protocol.GroupListRequest;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.protocol.LeaderNotReadyException;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.NotLeaderException;
import org.apache.ratis.protocol.NotReplicatedException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftException;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.ReflectionUtils;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.ALREADYCLOSEDEXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.LEADERNOTREADYEXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.NOTLEADEREXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.NOTREPLICATEDEXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.STATEMACHINEEXCEPTION;

public interface ClientProtoUtils {

  static RaftRpcReplyProto.Builder toRaftRpcReplyProtoBuilder(
      ByteString requestorId, ByteString replyId, RaftGroupId groupId,
      long callId, boolean success) {
    return RaftRpcReplyProto.newBuilder()
        .setRequestorId(requestorId)
        .setReplyId(replyId)
        .setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(groupId))
        .setCallId(callId)
        .setSuccess(success);
  }

  static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      ByteString requesterId, ByteString replyId, RaftGroupId groupId, long callId,
      SlidingWindowEntry slidingWindowEntry) {
    if (slidingWindowEntry == null) {
      slidingWindowEntry = SlidingWindowEntry.getDefaultInstance();
    }
    return RaftRpcRequestProto.newBuilder()
        .setRequestorId(requesterId)
        .setReplyId(replyId)
        .setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(groupId))
        .setCallId(callId)
        .setSlidingWindowEntry(slidingWindowEntry);
  }

  static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      ClientId requesterId, RaftPeerId replyId, RaftGroupId groupId, long callId,
      SlidingWindowEntry slidingWindowEntry) {
    return toRaftRpcRequestProtoBuilder(
        requesterId.toByteString(), replyId.toByteString(), groupId, callId, slidingWindowEntry);
  }

  static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      RaftClientRequest request) {
    return toRaftRpcRequestProtoBuilder(
        request.getClientId(),
        request.getServerId(),
        request.getRaftGroupId(),
        request.getCallId(),
        request.getSlidingWindowEntry());
  }

  static RaftClientRequest.Type toRaftClientRequestType(RaftClientRequestProto p) {
    switch (p.getTypeCase()) {
      case WRITE:
        return RaftClientRequest.Type.valueOf(p.getWrite());
      case STREAM:
        return RaftClientRequest.Type.valueOf(p.getStream());
      case READ:
        return RaftClientRequest.Type.valueOf(p.getRead());
      case STALEREAD:
        return RaftClientRequest.Type.valueOf(p.getStaleRead());
      case WATCH:
        return RaftClientRequest.Type.valueOf(p.getWatch());
      default:
        throw new IllegalArgumentException("Unexpected request type: " + p.getTypeCase()
            + " in request proto " + p);
    }
  }

  static RaftClientRequest toRaftClientRequest(RaftClientRequestProto p) {
    final RaftClientRequest.Type type = toRaftClientRequestType(p);
    final RaftRpcRequestProto request = p.getRpcRequest();
    return new RaftClientRequest(
        ClientId.valueOf(request.getRequestorId()),
        RaftPeerId.valueOf(request.getReplyId()),
        ProtoUtils.toRaftGroupId(request.getRaftGroupId()),
        request.getCallId(),
        toMessage(p.getMessage()),
        type,
        request.getSlidingWindowEntry());
  }

  static RaftClientRequestProto toRaftClientRequestProto(
      RaftClientRequest request) {
    final RaftClientRequestProto.Builder b = RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request));
    if (request.getMessage() != null) {
      b.setMessage(toClientMessageEntryProtoBuilder(request.getMessage()));
    }

    final RaftClientRequest.Type type = request.getType();
    switch (type.getTypeCase()) {
      case WRITE:
        b.setWrite(type.getWrite());
        break;
      case STREAM:
        b.setStream(type.getStream());
        break;
      case READ:
        b.setRead(type.getRead());
        break;
      case STALEREAD:
        b.setStaleRead(type.getStaleRead());
        break;
      case WATCH:
        b.setWatch(type.getWatch());
        break;
      default:
        throw new IllegalArgumentException("Unexpected request type: " + request.getType()
            + " in request " + request);
    }

    return b.build();
  }

  static RaftClientRequestProto toRaftClientRequestProto(
      ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId,
      long seqNum, ByteString content) {
    return RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(
            clientId, serverId, groupId, callId, ProtoUtils.toSlidingWindowEntry(seqNum, false)))
        .setWrite(WriteRequestTypeProto.getDefaultInstance())
        .setMessage(toClientMessageEntryProtoBuilder(content))
        .build();
  }

  static RaftClientReplyProto toRaftClientReplyProto(RaftClientReply reply) {
    final RaftClientReplyProto.Builder b = RaftClientReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(reply.getClientId().toByteString(),
          reply.getServerId().toByteString(), reply.getRaftGroupId(),
          reply.getCallId(), reply.isSuccess()));
      b.setLogIndex(reply.getLogIndex());
      if (reply.getMessage() != null) {
        b.setMessage(toClientMessageEntryProtoBuilder(reply.getMessage()));
      }
      ProtoUtils.addCommitInfos(reply.getCommitInfos(), b::addCommitInfos);

      final NotLeaderException nle = reply.getNotLeaderException();
      final StateMachineException sme;
      if (nle != null) {
        NotLeaderExceptionProto.Builder nleBuilder =
            NotLeaderExceptionProto.newBuilder();
        final RaftPeer suggestedLeader = nle.getSuggestedLeader();
        if (suggestedLeader != null) {
          nleBuilder.setSuggestedLeader(suggestedLeader.getRaftPeerProto());
        }
        nleBuilder.addAllPeersInConf(ProtoUtils.toRaftPeerProtos(nle.getPeers()));
        b.setNotLeaderException(nleBuilder.build());
      } else if ((sme = reply.getStateMachineException()) != null) {
        StateMachineExceptionProto.Builder smeBuilder =
            StateMachineExceptionProto.newBuilder();
        final Throwable t = sme.getCause() != null ? sme.getCause() : sme;
        smeBuilder.setExceptionClassName(t.getClass().getName())
            .setErrorMsg(t.getMessage())
            .setStacktrace(ProtoUtils.writeObject2ByteString(t.getStackTrace()));
        b.setStateMachineException(smeBuilder.build());
      }

      final NotReplicatedException nre = reply.getNotReplicatedException();
      if (nre != null) {
        final NotReplicatedExceptionProto.Builder nreBuilder = NotReplicatedExceptionProto.newBuilder()
            .setCallId(nre.getCallId())
            .setReplication(nre.getRequiredReplication())
            .setLogIndex(nre.getLogIndex());
        b.setNotReplicatedException(nreBuilder);
      }

      final LeaderNotReadyException lnre = reply.getLeaderNotReadyException();
      if (lnre != null) {
        LeaderNotReadyExceptionProto.Builder lnreBuilder = LeaderNotReadyExceptionProto.newBuilder()
            .setServerId(ProtoUtils.toRaftGroupMemberIdProtoBuilder(lnre.getServerId()));
        b.setLeaderNotReadyException(lnreBuilder);
      }

      final AlreadyClosedException ace = reply.getAlreadyClosedException();
      if (ace != null) {
        final Throwable t = ace.getCause() != null ? ace.getCause() : ace;
        AlreadyClosedExceptionProto.Builder aceBuilder = AlreadyClosedExceptionProto.newBuilder()
            .setExceptionClassName(t.getClass().getName())
            .setErrorMsg(ace.getMessage())
            .setStacktrace(ProtoUtils.writeObject2ByteString(ace.getStackTrace()));
        b.setAlreadyClosedException(aceBuilder);
      }

      final RaftClientReplyProto serialized = b.build();
      final RaftException e = reply.getException();
      if (e != null) {
        final RaftClientReply deserialized = toRaftClientReply(serialized);
        if (!Optional.ofNullable(deserialized.getException())
            .map(Object::getClass).filter(e.getClass()::equals).isPresent()) {
          throw new AssertionError("Corruption while serializing reply= " + reply
              + " but serialized=" + serialized + " and deserialized=" + deserialized, e);
        }
      }
      return serialized;
    }
    return b.build();
  }

  static GroupListReplyProto toGroupListReplyProto(
      GroupListReply reply) {
    final GroupListReplyProto.Builder b =
        GroupListReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(reply.getClientId().toByteString(),
          reply.getServerId().toByteString(), reply.getRaftGroupId(),
          reply.getCallId(), reply.isSuccess()));
      if (reply.getGroupIds() != null) {
        reply.getGroupIds().forEach(groupId -> b.addGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(groupId)));
      }
    }
    return b.build();
  }

  static GroupInfoReplyProto toGroupInfoReplyProto(GroupInfoReply reply) {
    final GroupInfoReplyProto.Builder b =
        GroupInfoReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(reply.getClientId().toByteString(),
          reply.getServerId().toByteString(), reply.getRaftGroupId(),
          reply.getCallId(), reply.isSuccess()));
      if (reply.getRaftGroupId() != null) {
        b.setGroup(ProtoUtils.toRaftGroupProtoBuilder(reply.getGroup()));
        b.setIsRaftStorageHealthy(reply.isRaftStorageHealthy());
        b.setRole(reply.getRoleInfoProto());
        ProtoUtils.addCommitInfos(reply.getCommitInfos(), b::addCommitInfos);
      }
    }
    return b.build();
  }

  static RaftClientReply toRaftClientReply(RaftClientReplyProto replyProto) {
    final RaftRpcReplyProto rp = replyProto.getRpcReply();
    final RaftGroupMemberId serverMemberId = ProtoUtils.toRaftGroupMemberId(rp.getReplyId(), rp.getRaftGroupId());

    final RaftException e;
    if (replyProto.getExceptionDetailsCase().equals(NOTLEADEREXCEPTION)) {
      NotLeaderExceptionProto nleProto = replyProto.getNotLeaderException();
      final RaftPeer suggestedLeader = nleProto.hasSuggestedLeader() ?
          ProtoUtils.toRaftPeer(nleProto.getSuggestedLeader()) : null;
      final List<RaftPeer> peers = ProtoUtils.toRaftPeers(nleProto.getPeersInConfList());
      e = new NotLeaderException(serverMemberId, suggestedLeader, peers);
    } else if (replyProto.getExceptionDetailsCase() == NOTREPLICATEDEXCEPTION) {
      final NotReplicatedExceptionProto nre = replyProto.getNotReplicatedException();
      e = new NotReplicatedException(nre.getCallId(), nre.getReplication(), nre.getLogIndex());
    } else if (replyProto.getExceptionDetailsCase().equals(STATEMACHINEEXCEPTION)) {
      StateMachineExceptionProto smeProto = replyProto.getStateMachineException();
      e = wrapStateMachineException(serverMemberId,
          smeProto.getExceptionClassName(), smeProto.getErrorMsg(), smeProto.getStacktrace());
    } else if (replyProto.getExceptionDetailsCase().equals(LEADERNOTREADYEXCEPTION)) {
      LeaderNotReadyExceptionProto lnreProto = replyProto.getLeaderNotReadyException();
      e = new LeaderNotReadyException(ProtoUtils.toRaftGroupMemberId(lnreProto.getServerId()));
    } else if (replyProto.getExceptionDetailsCase().equals(ALREADYCLOSEDEXCEPTION)) {
      AlreadyClosedExceptionProto aceProto = replyProto.getAlreadyClosedException();
      e = wrapAlreadyClosedException(aceProto.getExceptionClassName(),
          aceProto.getErrorMsg(), aceProto.getStacktrace());
    } else {
      e = null;
    }
    ClientId clientId = ClientId.valueOf(rp.getRequestorId());
    return new RaftClientReply(clientId, serverMemberId, rp.getCallId(), rp.getSuccess(),
        toMessage(replyProto.getMessage()), e,
        replyProto.getLogIndex(), replyProto.getCommitInfosList());
  }

  static GroupListReply toGroupListReply(
      GroupListReplyProto replyProto) {
    final RaftRpcReplyProto rp = replyProto.getRpcReply();
    ClientId clientId = ClientId.valueOf(rp.getRequestorId());
    final RaftGroupId groupId = ProtoUtils.toRaftGroupId(rp.getRaftGroupId());
    final List<RaftGroupId> groupInfos = replyProto.getGroupIdList().stream()
        .map(ProtoUtils::toRaftGroupId)
        .collect(Collectors.toList());
    return new GroupListReply(clientId, RaftPeerId.valueOf(rp.getReplyId()),
        groupId, rp.getCallId(), rp.getSuccess(), groupInfos);
  }

  static GroupInfoReply toGroupInfoReply(
      GroupInfoReplyProto replyProto) {
    final RaftRpcReplyProto rp = replyProto.getRpcReply();
    ClientId clientId = ClientId.valueOf(rp.getRequestorId());
    final RaftGroupId groupId = ProtoUtils.toRaftGroupId(rp.getRaftGroupId());
    final RaftGroup raftGroup = ProtoUtils.toRaftGroup(replyProto.getGroup());
    RoleInfoProto role = replyProto.getRole();
    boolean isRaftStorageHealthy = replyProto.getIsRaftStorageHealthy();
    return new GroupInfoReply(clientId, RaftPeerId.valueOf(rp.getReplyId()),
        groupId, rp.getCallId(), rp.getSuccess(), role, isRaftStorageHealthy,
        replyProto.getCommitInfosList(), raftGroup);
  }

  static StateMachineException wrapStateMachineException(
      RaftGroupMemberId memberId, String className, String errorMsg, ByteString stackTraceBytes) {
    StateMachineException sme;
    if (className == null) {
      sme = new StateMachineException(errorMsg);
    } else {
      try {
        Class<?> clazz = Class.forName(className);
        final Exception e = ReflectionUtils.instantiateException(
            clazz.asSubclass(Exception.class), errorMsg, null);
        sme = new StateMachineException(memberId, e);
      } catch (Exception e) {
        sme = new StateMachineException(className + ": " + errorMsg);
      }
    }
    StackTraceElement[] stacktrace =
        (StackTraceElement[]) ProtoUtils.toObject(stackTraceBytes);
    sme.setStackTrace(stacktrace);
    return sme;
  }

  static AlreadyClosedException wrapAlreadyClosedException(
      String className, String errorMsg, ByteString stackTraceBytes) {
    AlreadyClosedException ace;
    if (className == null) {
      ace = new AlreadyClosedException(errorMsg);
    } else {
      try {
        Class<?> clazz = Class.forName(className);
        final Exception e = ReflectionUtils.instantiateException(
            clazz.asSubclass(Exception.class), errorMsg, null);
        ace = new AlreadyClosedException(errorMsg, e);
      } catch (Exception e) {
        ace = new AlreadyClosedException(className + ": " + errorMsg);
      }
    }
    StackTraceElement[] stacktrace =
        (StackTraceElement[]) ProtoUtils.toObject(stackTraceBytes);
    ace.setStackTrace(stacktrace);
    return ace;
  }

  static Message toMessage(final ClientMessageEntryProto p) {
    return Message.valueOf(p.getContent());
  }

  static ClientMessageEntryProto.Builder toClientMessageEntryProtoBuilder(ByteString message) {
    return ClientMessageEntryProto.newBuilder().setContent(message);
  }

  static ClientMessageEntryProto.Builder toClientMessageEntryProtoBuilder(Message message) {
    return toClientMessageEntryProtoBuilder(message.getContent());
  }

  static SetConfigurationRequest toSetConfigurationRequest(
      SetConfigurationRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    final List<RaftPeer> peers = ProtoUtils.toRaftPeers(p.getPeersList());
    return new SetConfigurationRequest(
        ClientId.valueOf(m.getRequestorId()),
        RaftPeerId.valueOf(m.getReplyId()),
        ProtoUtils.toRaftGroupId(m.getRaftGroupId()),
        p.getRpcRequest().getCallId(), peers);
  }

  static SetConfigurationRequestProto toSetConfigurationRequestProto(
      SetConfigurationRequest request) {
    return SetConfigurationRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .addAllPeers(ProtoUtils.toRaftPeerProtos(request.getPeersInNewConf()))
        .build();
  }

  static GroupManagementRequest toGroupManagementRequest(GroupManagementRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    final ClientId clientId = ClientId.valueOf(m.getRequestorId());
    final RaftPeerId serverId = RaftPeerId.valueOf(m.getReplyId());
    switch(p.getOpCase()) {
      case GROUPADD:
        return GroupManagementRequest.newAdd(clientId, serverId, m.getCallId(),
            ProtoUtils.toRaftGroup(p.getGroupAdd().getGroup()));
      case GROUPREMOVE:
        final GroupRemoveRequestProto remove = p.getGroupRemove();
        return GroupManagementRequest.newRemove(clientId, serverId, m.getCallId(),
            ProtoUtils.toRaftGroupId(remove.getGroupId()),
            remove.getDeleteDirectory(), remove.getRenameDirectory());
      default:
        throw new IllegalArgumentException("Unexpected op " + p.getOpCase() + " in " + p);
    }
  }

  static GroupInfoRequest toGroupInfoRequest(
      GroupInfoRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    return new GroupInfoRequest(
        ClientId.valueOf(m.getRequestorId()),
        RaftPeerId.valueOf(m.getReplyId()),
        ProtoUtils.toRaftGroupId(m.getRaftGroupId()),
        m.getCallId());
  }

  static GroupListRequest toGroupListRequest(
      GroupListRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    return new GroupListRequest(
        ClientId.valueOf(m.getRequestorId()),
        RaftPeerId.valueOf(m.getReplyId()),
        ProtoUtils.toRaftGroupId(m.getRaftGroupId()),
        m.getCallId());
  }


  static GroupManagementRequestProto toGroupManagementRequestProto(GroupManagementRequest request) {
    final GroupManagementRequestProto.Builder b = GroupManagementRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request));
    final GroupManagementRequest.Add add = request.getAdd();
    if (add != null) {
      b.setGroupAdd(GroupAddRequestProto.newBuilder().setGroup(
          ProtoUtils.toRaftGroupProtoBuilder(add.getGroup())).build());
    }
    final GroupManagementRequest.Remove remove = request.getRemove();
    if (remove != null) {
      b.setGroupRemove(GroupRemoveRequestProto.newBuilder()
          .setGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(remove.getGroupId()))
          .setDeleteDirectory(remove.isDeleteDirectory())
          .setRenameDirectory(remove.isRenameDirectory())
          .build());
    }
    return b.build();
  }

  static GroupInfoRequestProto toGroupInfoRequestProto(
      GroupInfoRequest request) {
    return GroupInfoRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .build();
  }

  static GroupListRequestProto toGroupListRequestProto(
      GroupListRequest request) {
    return GroupListRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .build();
  }

  static String toString(RaftClientRequestProto proto) {
    final RaftRpcRequestProto rpc = proto.getRpcRequest();
    return ClientId.valueOf(rpc.getRequestorId()) + "->" + rpc.getReplyId().toStringUtf8()
        + "#" + rpc.getCallId() + "-" + ProtoUtils.toString(rpc.getSlidingWindowEntry());
  }

  static String toString(RaftClientReplyProto proto) {
    final RaftRpcReplyProto rpc = proto.getRpcReply();
    return ClientId.valueOf(rpc.getRequestorId()) + "<-" + rpc.getReplyId().toStringUtf8()
        + "#" + rpc.getCallId();
  }
}
