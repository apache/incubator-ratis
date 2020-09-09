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
package org.apache.ratis.hadooprpc.client;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.proto.hadoop.HadoopCompatibilityProtos.ClientReplyProto;
import org.apache.ratis.proto.hadoop.HadoopCompatibilityProtos.ClientRequestProto;
import org.apache.ratis.proto.hadoop.HadoopCompatibilityProtos.ClientOps;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.GroupListReply;
import org.apache.ratis.protocol.GroupListRequest;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.server.RaftServer;
import com.google.protobuf.RpcController;
import org.apache.ratis.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupManagementRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupListRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupListReplyProto;
import org.apache.ratis.proto.RaftProtos.GroupInfoRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupInfoReplyProto;
import org.apache.ratis.thirdparty.com.google.protobuf.GeneratedMessageV3;


@InterfaceAudience.Private
public class CombinedClientProtocolServerSideTranslatorPB
    implements CombinedClientProtocolPB {
  private final RaftServer impl;

  public CombinedClientProtocolServerSideTranslatorPB(RaftServer impl) {
    this.impl = impl;
  }

  @Override
  public ClientReplyProto sendClient(RpcController unused, ClientRequestProto req) throws ServiceException {
    ByteBuffer buf = req.getRequest().asReadOnlyByteBuffer();
    GeneratedMessageV3 response = null;
    ClientOps type = req.getType();
    try {
      switch (type) {
      case groupInfo:
        response = groupInfo(GroupInfoRequestProto.parseFrom(buf));
        break;
      case groupList:
        response = groupList(GroupListRequestProto.parseFrom(buf));
        break;
      case groupManagement:
        response = groupManagement(GroupManagementRequestProto.parseFrom(buf));
        break;
      case setConfiguration:
        response = setConfiguration(SetConfigurationRequestProto.parseFrom(buf));
        break;
      case submitClientRequest:
        response = submitClientRequest(RaftClientRequestProto.parseFrom(buf));
        break;
      default:
      }
    } catch(IOException ioe) {
      throw new ServiceException(ioe);
    }
    return ClientReplyProto.newBuilder()
        .setType(type)
        .setResponse(ByteString.copyFrom(response.toByteArray()))
        .build();
  }

  public RaftClientReplyProto submitClientRequest(RaftClientRequestProto proto)
      throws IOException {
    final RaftClientRequest request = ClientProtoUtils.toRaftClientRequest(proto);
    final RaftClientReply reply = impl.submitClientRequest(request);
    return ClientProtoUtils.toRaftClientReplyProto(reply);
  }

  public RaftClientReplyProto setConfiguration(SetConfigurationRequestProto proto)
      throws IOException {
    final SetConfigurationRequest request = ClientProtoUtils.toSetConfigurationRequest(proto);
    final RaftClientReply reply = impl.setConfiguration(request);
    return ClientProtoUtils.toRaftClientReplyProto(reply);
  }

  public RaftClientReplyProto groupManagement(GroupManagementRequestProto proto)
      throws IOException {
    final GroupManagementRequest request = ClientProtoUtils.toGroupManagementRequest(proto);
    final RaftClientReply reply = impl.groupManagement(request);
    return ClientProtoUtils.toRaftClientReplyProto(reply);
  }

  public GroupListReplyProto groupList(GroupListRequestProto proto)
    throws IOException {
    final GroupListRequest request = ClientProtoUtils.toGroupListRequest(proto);
    final GroupListReply reply = impl.getGroupList(request);
    return ClientProtoUtils.toGroupListReplyProto(reply);
  }

  public GroupInfoReplyProto groupInfo(GroupInfoRequestProto proto)
      throws IOException {
    final GroupInfoRequest request = ClientProtoUtils.toGroupInfoRequest(proto);
    final GroupInfoReply reply = impl.getGroupInfo(request);
    return ClientProtoUtils.toGroupInfoReplyProto(reply);
  }
}
