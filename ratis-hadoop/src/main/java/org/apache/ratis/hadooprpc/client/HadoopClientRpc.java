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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.ratis.client.impl.RaftClientRpcWithProxy;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.GroupListRequest;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.protocol.GroupMismatchException;
import org.apache.ratis.protocol.LeaderNotReadyException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftException;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.ReconfigurationInProgressException;
import org.apache.ratis.protocol.ReconfigurationTimeoutException;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.util.PeerProxyMap;

import java.io.IOException;

public class HadoopClientRpc extends RaftClientRpcWithProxy<CombinedClientProtocolClientSideTranslatorPB> {
  public HadoopClientRpc(ClientId clientId, Configuration conf) {
    super(new PeerProxyMap<>(clientId.toString(),
        p -> new CombinedClientProtocolClientSideTranslatorPB(p.getAddress(), conf)));
  }

  @Override
  public RaftClientReply sendRequest(RaftClientRequest request)
      throws IOException {
    final RaftPeerId serverId = request.getServerId();
    final CombinedClientProtocolClientSideTranslatorPB proxy =
        getProxies().getProxy(serverId);
    try {
      if (request instanceof GroupManagementRequest) {
        return proxy.groupManagement((GroupManagementRequest) request);
      } else if (request instanceof SetConfigurationRequest) {
        return proxy.setConfiguration((SetConfigurationRequest) request);
      } else if (request instanceof GroupListRequest) {
        return proxy.getGroupList((GroupListRequest) request);
      } else if (request instanceof GroupInfoRequest) {
        return proxy.getGroupInfo((GroupInfoRequest) request);
      } else {
        return proxy.submitClientRequest(request);
      }
    } catch (RemoteException e) {
      throw e.unwrapRemoteException(
          StateMachineException.class,
          ReconfigurationTimeoutException.class,
          ReconfigurationInProgressException.class,
          RaftException.class,
          LeaderNotReadyException.class,
          GroupMismatchException.class);
    }
  }
}
