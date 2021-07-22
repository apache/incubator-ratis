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
package org.apache.ratis.datastream;

import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;

/**
 * A {@link MiniRaftCluster} with {{@link SupportedRpcType#GRPC}} and {@link SupportedDataStreamType#NETTY}.
 */
public class MiniRaftClusterWithRpcTypeGrpcAndDataStreamTypeNetty extends MiniRaftClusterWithGrpc {
  public static final Factory<MiniRaftClusterWithRpcTypeGrpcAndDataStreamTypeNetty> FACTORY
      = new Factory<MiniRaftClusterWithRpcTypeGrpcAndDataStreamTypeNetty>() {
    @Override
    public MiniRaftClusterWithRpcTypeGrpcAndDataStreamTypeNetty newCluster(String[] ids, String[] idOfListeners, RaftProperties prop) {
      RaftConfigKeys.Rpc.setType(prop, SupportedRpcType.GRPC);
      RaftConfigKeys.DataStream.setType(prop, SupportedDataStreamType.NETTY);
      return new MiniRaftClusterWithRpcTypeGrpcAndDataStreamTypeNetty(ids, prop);
    }
  };

  public interface FactoryGet extends Factory.Get<MiniRaftClusterWithRpcTypeGrpcAndDataStreamTypeNetty> {
    @Override
    default Factory<MiniRaftClusterWithRpcTypeGrpcAndDataStreamTypeNetty> getFactory() {
      return FACTORY;
    }
  }

  private MiniRaftClusterWithRpcTypeGrpcAndDataStreamTypeNetty(String[] ids, RaftProperties properties) {
    super(ids, properties);
  }

  @Override
  protected Parameters setPropertiesAndInitParameters(RaftPeerId id, RaftGroup group, RaftProperties properties) {
    NettyConfigKeys.DataStream.setPort(properties, getDataStreamPort(id, group));
    return super.setPropertiesAndInitParameters(id, group, properties);
  }
}
