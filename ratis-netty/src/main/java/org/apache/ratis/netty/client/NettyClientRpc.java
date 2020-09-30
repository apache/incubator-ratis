/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.ratis.netty.client;

import java.util.concurrent.CompletableFuture;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.client.impl.RaftClientRpcWithProxy;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyRpcProxy;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupManagementRequestProto;
import org.apache.ratis.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerRequestProto;

import java.io.IOException;
import org.apache.ratis.util.JavaUtils;

public class NettyClientRpc extends RaftClientRpcWithProxy<NettyRpcProxy> {
  public NettyClientRpc(ClientId clientId, RaftProperties properties) {
    super(new NettyRpcProxy.PeerMap(clientId.toString(), properties));
  }

  @Override
  public CompletableFuture<RaftClientReply> sendRequestAsync(RaftClientRequest request) {
    final RaftPeerId serverId = request.getServerId();
    try {
      final NettyRpcProxy proxy = getProxies().getProxy(serverId);
      final RaftNettyServerRequestProto serverRequestProto = buildRequestProto(request);
      return proxy.sendAsync(serverRequestProto)
          .thenApply(replyProto -> {
            if (request instanceof GroupListRequest) {
              return ClientProtoUtils.toGroupListReply(replyProto.getGroupListReply());
            } else if (request instanceof GroupInfoRequest) {
              return ClientProtoUtils.toGroupInfoReply(replyProto.getGroupInfoReply());
            } else {
              return ClientProtoUtils.toRaftClientReply(replyProto.getRaftClientReply());
            }
          });
    } catch (Throwable e) {
      return JavaUtils.completeExceptionally(e);
    }
  }

  @Override
  public RaftClientReply sendRequest(RaftClientRequest request) throws IOException {
    final RaftPeerId serverId = request.getServerId();
    final NettyRpcProxy proxy = getProxies().getProxy(serverId);

    final RaftNettyServerRequestProto serverRequestProto = buildRequestProto(request);
    final RaftRpcRequestProto rpcRequest = getRpcRequestProto(serverRequestProto);
    if (request instanceof GroupListRequest) {
      return ClientProtoUtils.toGroupListReply(
          proxy.send(rpcRequest, serverRequestProto).getGroupListReply());
    } else if (request instanceof GroupInfoRequest) {
      return ClientProtoUtils.toGroupInfoReply(
          proxy.send(rpcRequest, serverRequestProto).getGroupInfoReply());
    } else {
      return ClientProtoUtils.toRaftClientReply(
          proxy.send(rpcRequest, serverRequestProto).getRaftClientReply());
    }
  }

  private RaftNettyServerRequestProto buildRequestProto(RaftClientRequest request) {
    final RaftNettyServerRequestProto.Builder b = RaftNettyServerRequestProto.newBuilder();
    if (request instanceof GroupManagementRequest) {
      final GroupManagementRequestProto proto = ClientProtoUtils.toGroupManagementRequestProto(
          (GroupManagementRequest)request);
      b.setGroupManagementRequest(proto);
    } else if (request instanceof SetConfigurationRequest) {
      final SetConfigurationRequestProto proto = ClientProtoUtils.toSetConfigurationRequestProto(
          (SetConfigurationRequest)request);
      b.setSetConfigurationRequest(proto);
    } else if (request instanceof GroupListRequest) {
      final RaftProtos.GroupListRequestProto proto = ClientProtoUtils.toGroupListRequestProto(
          (GroupListRequest)request);
      b.setGroupListRequest(proto);
    } else if (request instanceof GroupInfoRequest) {
      final RaftProtos.GroupInfoRequestProto proto = ClientProtoUtils.toGroupInfoRequestProto(
          (GroupInfoRequest)request);
      b.setGroupInfoRequest(proto);
    } else {
      final RaftClientRequestProto proto = ClientProtoUtils.toRaftClientRequestProto(request);
      b.setRaftClientRequest(proto);
    }
    return b.build();
  }

  private RaftRpcRequestProto getRpcRequestProto(RaftNettyServerRequestProto serverRequestProto) {
    if (serverRequestProto.hasGroupManagementRequest()) {
      return serverRequestProto.getGroupManagementRequest().getRpcRequest();
    } else if (serverRequestProto.hasSetConfigurationRequest()) {
      return serverRequestProto.getSetConfigurationRequest().getRpcRequest();
    } else if (serverRequestProto.hasGroupListRequest()) {
      return serverRequestProto.getGroupListRequest().getRpcRequest();
    } else if (serverRequestProto.hasGroupInfoRequest()) {
      return serverRequestProto.getGroupInfoRequest().getRpcRequest();
    } else {
      return serverRequestProto.getRaftClientRequest().getRpcRequest();
    }
  }
}
