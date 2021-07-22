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
package org.apache.ratis.server.simulation;

import java.util.concurrent.TimeUnit;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.StreamSupport;

import static org.apache.ratis.conf.ConfUtils.requireMin;

public class MiniRaftClusterWithSimulatedRpc extends MiniRaftCluster {
  static final Logger LOG = LoggerFactory.getLogger(MiniRaftClusterWithSimulatedRpc.class);

  public static final Factory<MiniRaftClusterWithSimulatedRpc> FACTORY
      = new Factory<MiniRaftClusterWithSimulatedRpc>() {
    @Override
    public MiniRaftClusterWithSimulatedRpc newCluster(
        String[] ids, String[] idOfListeners, RaftProperties prop) {
      RaftConfigKeys.Rpc.setType(prop, SimulatedRpc.INSTANCE);
      if (ThreadLocalRandom.current().nextBoolean()) {
        // turn off simulate latency half of the times.
        prop.setInt(SimulatedRequestReply.SIMULATE_LATENCY_KEY, 0);
      }
      final int simulateLatencyMs = ConfUtils.getInt(prop::getInt,
          SimulatedRequestReply.SIMULATE_LATENCY_KEY,
          SimulatedRequestReply.SIMULATE_LATENCY_DEFAULT, LOG::info, requireMin(0));
      final SimulatedRequestReply<RaftServerRequest, RaftServerReply> serverRequestReply
          = new SimulatedRequestReply<>(simulateLatencyMs);
      final SimulatedClientRpc client2serverRequestReply
          = new SimulatedClientRpc(simulateLatencyMs);
      return new MiniRaftClusterWithSimulatedRpc(ids, idOfListeners, prop,
          serverRequestReply, client2serverRequestReply);
    }
  };

  public interface FactoryGet extends Factory.Get<MiniRaftClusterWithSimulatedRpc> {
    @Override
    default Factory<MiniRaftClusterWithSimulatedRpc> getFactory() {
      return FACTORY;
    }
  }

  private final SimulatedRequestReply<RaftServerRequest, RaftServerReply> serverRequestReply;
  private final SimulatedClientRpc client2serverRequestReply;

  private MiniRaftClusterWithSimulatedRpc(
      String[] ids, String[] idOfListeners, RaftProperties properties,
      SimulatedRequestReply<RaftServerRequest, RaftServerReply> serverRequestReply,
      SimulatedClientRpc client2serverRequestReply) {
    super(ids, idOfListeners, properties,
        SimulatedRpc.Factory.newRaftParameters(serverRequestReply, client2serverRequestReply));
    this.serverRequestReply = serverRequestReply;
    this.client2serverRequestReply = client2serverRequestReply;
  }

  @Override
  public void restart(boolean format) throws IOException {
    serverRequestReply.clear();
    client2serverRequestReply.clear();
    super.restart(format);
  }

  @Override
  protected Parameters setPropertiesAndInitParameters(RaftPeerId id, RaftGroup group, RaftProperties properties) {
    serverRequestReply.addPeer(id);
    client2serverRequestReply.addPeer(id);
    return parameters;
  }

  @Override
  public void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {
    // block leader sendRequest if delayMs > 0
    final boolean block = delayMs > 0;
    LOG.debug("{} leader queue {} and set {}ms delay for the other queues",
        block? "Block": "Unblock", leaderId, delayMs);
    serverRequestReply.getQueue(leaderId).blockSendRequestTo.set(block);

    // set delay takeRequest for the other queues
    StreamSupport.stream(getServers().spliterator(), false)
        .filter(s -> !s.getId().toString().equals(leaderId))
        .map(s -> serverRequestReply.getQueue(s.getId().toString()))
        .forEach(q -> q.delayTakeRequestTo.set(delayMs));

    final long sleepMs = 3 * getTimeoutMax().toLong(TimeUnit.MILLISECONDS) / 2;
    Thread.sleep(sleepMs);
  }

  @Override
  public void setBlockRequestsFrom(String src, boolean block) {
    serverRequestReply.getQueue(src).blockTakeRequestFrom.set(block);
  }
}
