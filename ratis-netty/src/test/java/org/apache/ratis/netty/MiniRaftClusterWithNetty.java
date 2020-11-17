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
package org.apache.ratis.netty;

import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.netty.server.NettyRpcService;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.impl.*;
import org.apache.ratis.statemachine.StateMachine;

import java.io.IOException;

/**
 * A {@link MiniRaftCluster} with {{@link SupportedRpcType#NETTY}} and {@link SupportedDataStreamType#NETTY}.
 */
public class MiniRaftClusterWithNetty extends MiniRaftCluster.RpcBase {
  public static final Factory<MiniRaftClusterWithNetty> FACTORY
      = new Factory<MiniRaftClusterWithNetty>() {
    @Override
    public MiniRaftClusterWithNetty newCluster(String[] ids, RaftProperties prop) {
      RaftConfigKeys.Rpc.setType(prop, SupportedRpcType.NETTY);
      RaftConfigKeys.DataStream.setType(prop, SupportedDataStreamType.NETTY);
      return new MiniRaftClusterWithNetty(ids, prop);
    }
  };

  public interface FactoryGet extends Factory.Get<MiniRaftClusterWithNetty> {
    @Override
    default Factory<MiniRaftClusterWithNetty> getFactory() {
      return FACTORY;
    }
  }

  public static final DelayLocalExecutionInjection sendServerRequest
      = new DelayLocalExecutionInjection(NettyRpcService.SEND_SERVER_REQUEST);

  private MiniRaftClusterWithNetty(String[] ids, RaftProperties properties) {
    super(ids, properties, null);
  }

  @Override
  protected RaftServerProxy newRaftServer(
      RaftPeerId id, StateMachine.Registry stateMachineRegistry , RaftGroup group,
      RaftProperties properties) throws IOException {
    NettyConfigKeys.Server.setPort(properties, getPort(id, group));
    NettyConfigKeys.DataStream.setPort(properties, getDataStreamPort(id, group));
    return ServerImplUtils.newRaftServer(id, group, stateMachineRegistry, properties, null);
  }

  @Override
  protected void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {
    RaftTestUtil.blockQueueAndSetDelay(getServers(), sendServerRequest,
        leaderId, delayMs, getTimeoutMax());
  }
}
