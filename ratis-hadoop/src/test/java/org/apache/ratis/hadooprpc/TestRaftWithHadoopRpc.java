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
package org.apache.ratis.hadooprpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.log4j.Level;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftBasicTests;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.util.Log4jUtils;
import org.junit.Test;

public class TestRaftWithHadoopRpc
    extends RaftBasicTests<MiniRaftClusterWithHadoopRpc> {
  static {
    Log4jUtils.setLogLevel(MiniRaftClusterWithHadoopRpc.LOG, Level.DEBUG);
  }

  static final Configuration CONF = new Configuration();
  static {
    HadoopConfigKeys.Ipc.setHandlers(CONF, 20);
    CONF.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    CONF.setInt(CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY, 1000);
    CONF.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 1000);
  }
  static final MiniRaftCluster.Factory<MiniRaftClusterWithHadoopRpc> FACTORY
      = new MiniRaftClusterWithHadoopRpc.Factory() {
    @Override
    public MiniRaftClusterWithHadoopRpc newCluster(String[] ids, String[] idOfListeners, RaftProperties prop) {
      return newCluster(ids, idOfListeners, prop, CONF);
    }
  };

  @Override
  @Test
  public void testWithLoad() throws Exception {
    super.testWithLoad();
    BlockRequestHandlingInjection.getInstance().unblockAll();
  }

  @Override
  public MiniRaftCluster.Factory<MiniRaftClusterWithHadoopRpc> getFactory() {
    return FACTORY;
  }
}
