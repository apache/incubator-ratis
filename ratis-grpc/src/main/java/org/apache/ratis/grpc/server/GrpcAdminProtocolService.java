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
package org.apache.ratis.grpc.server;

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.proto.RaftProtos.GroupInfoReplyProto;
import org.apache.ratis.proto.RaftProtos.GroupInfoRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupListReplyProto;
import org.apache.ratis.proto.RaftProtos.GroupListRequestProto;
import org.apache.ratis.protocol.AdminAsynchronousProtocol;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.GroupListRequest;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.proto.RaftProtos.GroupManagementRequestProto;
import org.apache.ratis.proto.grpc.AdminProtocolServiceGrpc.AdminProtocolServiceImplBase;

public class GrpcAdminProtocolService extends AdminProtocolServiceImplBase {
  private final AdminAsynchronousProtocol protocol;

  public GrpcAdminProtocolService(AdminAsynchronousProtocol protocol) {
    this.protocol = protocol;
  }

  @Override
  public void groupManagement(GroupManagementRequestProto proto,
        StreamObserver<RaftClientReplyProto> responseObserver) {
    final GroupManagementRequest request = ClientProtoUtils.toGroupManagementRequest(proto);
    GrpcUtil.asyncCall(responseObserver, () -> protocol.groupManagementAsync(request),
        ClientProtoUtils::toRaftClientReplyProto);
  }

  @Override
  public void groupList(GroupListRequestProto proto,
        StreamObserver<GroupListReplyProto> responseObserver) {
    final GroupListRequest request = ClientProtoUtils.toGroupListRequest(proto);
    GrpcUtil.asyncCall(responseObserver, () -> protocol.getGroupListAsync(request),
        ClientProtoUtils::toGroupListReplyProto);
  }

  @Override
  public void groupInfo(GroupInfoRequestProto proto, StreamObserver<GroupInfoReplyProto> responseObserver) {
    final GroupInfoRequest request = ClientProtoUtils.toGroupInfoRequest(proto);
    GrpcUtil.asyncCall(responseObserver, () -> protocol.getGroupInfoAsync(request),
        ClientProtoUtils::toGroupInfoReplyProto);
  }
}
