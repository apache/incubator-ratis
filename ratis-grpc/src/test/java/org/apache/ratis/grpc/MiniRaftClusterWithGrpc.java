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
package org.apache.ratis.grpc;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.server.GrpcService;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.impl.DelayLocalExecutionInjection;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.util.NetUtils;

import java.util.Optional;

/**
 * A {@link MiniRaftCluster} with {{@link SupportedRpcType#GRPC}} and data stream disabled.
 */
public class MiniRaftClusterWithGrpc extends MiniRaftCluster.RpcBase {
  public static final Factory<MiniRaftClusterWithGrpc> FACTORY
      = new Factory<MiniRaftClusterWithGrpc>() {
    @Override
    public MiniRaftClusterWithGrpc newCluster(String[] ids, String[] idOfListeners, RaftProperties prop) {
      RaftConfigKeys.Rpc.setType(prop, SupportedRpcType.GRPC);
      return new MiniRaftClusterWithGrpc(ids, idOfListeners, prop);
    }
  };

  public interface FactoryGet extends Factory.Get<MiniRaftClusterWithGrpc> {
    @Override
    default Factory<MiniRaftClusterWithGrpc> getFactory() {
      return FACTORY;
    }
  }

  public static final DelayLocalExecutionInjection sendServerRequestInjection =
      new DelayLocalExecutionInjection(GrpcService.GRPC_SEND_SERVER_REQUEST);

  protected MiniRaftClusterWithGrpc(String[] ids, String[] idOfLearners, RaftProperties properties) {
    super(ids, idOfLearners, properties, null);
  }

  protected MiniRaftClusterWithGrpc(String[] ids, RaftProperties properties) {
    super(ids, properties, null);
  }

  @Override
  protected Parameters setPropertiesAndInitParameters(RaftPeerId id, RaftGroup group, RaftProperties properties) {
    GrpcConfigKeys.Server.setPort(properties, getPort(id, group));
    Optional.ofNullable(getAddress(id, group, RaftPeer::getClientAddress)).ifPresent(address ->
        GrpcConfigKeys.Client.setPort(properties, NetUtils.createSocketAddr(address).getPort()));
    Optional.ofNullable(getAddress(id, group, RaftPeer::getAdminAddress)).ifPresent(address ->
        GrpcConfigKeys.Admin.setPort(properties, NetUtils.createSocketAddr(address).getPort()));
    return null;
  }

  @Override
  protected void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {
    RaftTestUtil.blockQueueAndSetDelay(getServers(), sendServerRequestInjection,
        leaderId, delayMs, getTimeoutMax());
  }
}
