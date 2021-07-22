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

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.server.NettyRpcService;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.impl.DelayLocalExecutionInjection;
import org.apache.ratis.server.impl.MiniRaftCluster;

/**
 * A {@link MiniRaftCluster} with {{@link SupportedRpcType#NETTY}} and data stream disabled.
 */
public class MiniRaftClusterWithNetty extends MiniRaftCluster.RpcBase {
  public static final Factory<MiniRaftClusterWithNetty> FACTORY
      = new Factory<MiniRaftClusterWithNetty>() {
    @Override
    public MiniRaftClusterWithNetty newCluster(String[] ids, String[] idOfListeners, RaftProperties prop) {
      RaftConfigKeys.Rpc.setType(prop, SupportedRpcType.NETTY);
      return new MiniRaftClusterWithNetty(ids, idOfListeners, prop);
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

  protected MiniRaftClusterWithNetty(String[] ids, String[] idOfLearners, RaftProperties properties) {
    super(ids, idOfLearners, properties, null);
  }

  protected MiniRaftClusterWithNetty(String[] ids, RaftProperties properties) {
    super(ids, properties, null);
  }

  @Override
  protected Parameters setPropertiesAndInitParameters(RaftPeerId id, RaftGroup group, RaftProperties properties) {
    NettyConfigKeys.Server.setPort(properties, getPort(id, group));
    return null;
  }

  @Override
  protected void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {
    RaftTestUtil.blockQueueAndSetDelay(getServers(), sendServerRequest,
        leaderId, delayMs, getTimeoutMax());
  }
}
