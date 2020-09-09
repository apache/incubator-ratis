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

package org.apache.ratis.logservice.server;

import com.beust.jcommander.JCommander;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.logservice.api.LogServiceConfiguration;
import org.apache.ratis.logservice.common.Constants;
import org.apache.ratis.logservice.util.LogServiceUtils;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.apache.ratis.logservice.common.Constants.META_GROUP_ID;
import static org.apache.ratis.logservice.util.LogServiceUtils.getPeersFromQuorum;

/**
 * Master quorum is responsible for tracking all available quorum members
 */
public class MetadataServer extends BaseServer {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataServer.class);

    // RaftServer internal server. Has meta raft group and MetaStateMachine
    private  RaftServer server;

    private String id;

    private StateMachine metaStateMachine;

    private LifeCycle lifeCycle;

    public StateMachine getMetaStateMachine() {
      return metaStateMachine;
    }

    public MetadataServer(ServerOpts opts) {
      super(opts);
      LOG.debug("Metadata Server options: {}", opts);
      this.id = opts.getHost() + "_" + opts.getPort();
      this.lifeCycle = new LifeCycle(this.id);
    }

    public void start() throws IOException  {
        final ServerOpts opts = getServerOpts();
        if (opts.getHost() == null) {
            opts.setHost(LogServiceUtils.getHostName());
        }
        this.lifeCycle = new LifeCycle(this.id);
        RaftProperties properties = new RaftProperties();
        if(opts.getWorkingDir() != null) {
            RaftServerConfigKeys.setStorageDir(properties,
              Collections.singletonList(new File(opts.getWorkingDir())));
        } else {
          String path = getConfig().get(Constants.META_SERVER_WORKDIR_KEY);
          if (path != null) {
            RaftServerConfigKeys.setStorageDir(properties,
              Collections.singletonList(new File(path)));
          }
        }

        // Set properties common to all log service state machines
        setRaftProperties(properties);
        long failureDetectionPeriod = getConfig().
                getLong(Constants.LOG_SERVICE_PEER_FAILURE_DETECTION_PERIOD_KEY,
                Constants.DEFAULT_PEER_FAILURE_DETECTION_PERIOD);
        Set<RaftPeer> peers = getPeersFromQuorum(opts.getMetaQuorum());
        RaftGroupId raftMetaGroupId = RaftGroupId.valueOf(opts.getMetaGroupId());
        RaftGroup metaGroup = RaftGroup.valueOf(raftMetaGroupId, peers);
        metaStateMachine = new MetaStateMachine(raftMetaGroupId, RaftGroupId.valueOf(opts.getLogServerGroupId()),
                failureDetectionPeriod);

        // Make sure that we aren't setting any invalid/harmful properties
        validateRaftProperties(properties);

        server = RaftServer.newBuilder()
                .setGroup(metaGroup)
                .setServerId(RaftPeerId.valueOf(id))
                .setStateMachineRegistry(raftGroupId -> {
                    if(raftGroupId.equals(META_GROUP_ID)) {
                        return metaStateMachine;
                    }
                    return null;
                })
                .setProperties(properties).build();
        lifeCycle.startAndTransition(() -> {
            server.start();
        }, IOException.class);
    }

    public static void main(String[] args) throws IOException {
        ServerOpts opts = new ServerOpts();
        JCommander.newBuilder()
                .addObject(opts)
                .build()
                .parse(args);
        // Add config from log service configuration file
        LogServiceConfiguration config = LogServiceConfiguration.create();
        opts = config.addMetaServerOpts(opts);

        try (MetadataServer master = new MetadataServer(opts)) {
          master.start();
          while (true) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
          }
        }
    }

    public static MetadataServer.Builder newBuilder() {
        return new MetadataServer.Builder();
    }

    @Override
    public void close() throws IOException {
        server.close();
    }

    public String getId() {
        return id;
    }

    public String getAddress() {
        return getServerOpts().getHost() + ":" + getServerOpts().getPort();
    }

    public void cleanUp() throws IOException {
        FileUtils.deleteFully(new File(getServerOpts().getWorkingDir()));
    }

    public static class Builder extends BaseServer.Builder<MetadataServer> {
        /**
         * @return a {@link MetadataServer} object.
         */
        public MetadataServer build()  {
            validate();
            return new MetadataServer(getOpts());
        }
    }

    public RaftServer getServer() {
        return server;
    }
}
