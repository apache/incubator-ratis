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
package org.apache.ratis.hadooprpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.hadooprpc.server.HadoopRpcService;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.impl.DelayLocalExecutionInjection;
import org.apache.ratis.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniRaftClusterWithHadoopRpc extends MiniRaftCluster.RpcBase {
  static final Logger LOG = LoggerFactory.getLogger(MiniRaftClusterWithHadoopRpc.class);

  public static class Factory extends MiniRaftCluster.Factory<MiniRaftClusterWithHadoopRpc> {
    public interface Get extends MiniRaftCluster.Factory.Get<MiniRaftClusterWithHadoopRpc> {
      @Override
      default MiniRaftCluster.Factory<MiniRaftClusterWithHadoopRpc> getFactory() {
        return FACTORY;
      }
    }

    @Override
    public MiniRaftClusterWithHadoopRpc newCluster(String[] ids, String[] idOfListeners, RaftProperties prop) {
      final Configuration conf = new Configuration();
      return newCluster(ids, prop, conf);
    }

    public MiniRaftClusterWithHadoopRpc newCluster(
        int numServers, RaftProperties properties, Configuration conf) {
      return newCluster(generateIds(numServers, 0), properties, conf);
    }

    public MiniRaftClusterWithHadoopRpc newCluster(
        String[] ids, String[] idOfLearners, RaftProperties prop, Configuration conf) {
      RaftConfigKeys.Rpc.setType(prop, SupportedRpcType.HADOOP);
      HadoopConfigKeys.Ipc.setAddress(conf, "0.0.0.0:0");
      return new MiniRaftClusterWithHadoopRpc(ids, idOfLearners, prop, conf);
    }

    public MiniRaftClusterWithHadoopRpc newCluster(
        String[] ids, RaftProperties prop, Configuration conf) {
      return newCluster(ids, new String[]{}, prop, conf);
    }
  }

  public static final Factory FACTORY = new Factory();

  public static final DelayLocalExecutionInjection sendServerRequest =
      new DelayLocalExecutionInjection(HadoopRpcService.SEND_SERVER_REQUEST);

  private final Configuration hadoopConf;

  private MiniRaftClusterWithHadoopRpc(String[] ids, String[] idOfLearners, RaftProperties properties,
      Configuration hadoopConf) {
    super(ids, idOfLearners, properties, HadoopFactory.newRaftParameters(hadoopConf));
    this.hadoopConf = hadoopConf;
  }

  @Override
  protected Parameters setPropertiesAndInitParameters(RaftPeerId id, RaftGroup group, RaftProperties properties) {
    final Configuration hconf = new Configuration(hadoopConf);
    final String address = "0.0.0.0:" + getPort(id, group);
    HadoopConfigKeys.Ipc.setAddress(hconf, address);
    return HadoopFactory.newRaftParameters(hconf);
  }

  @Override
  public void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {
    RaftTestUtil.blockQueueAndSetDelay(getServers(), sendServerRequest,
        leaderId, delayMs, getTimeoutMax());
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + ": sendServerRequest=" + sendServerRequest;
  }
}
