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
package org.apache.ratis.server.leader;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotResult;
import org.apache.ratis.rpc.CallId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.server.util.ServerStringUtils;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.Timestamp;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.UUID;

/**
 * The default implementation of {@link LogAppender}
 * using {@link org.apache.ratis.server.protocol.RaftServerProtocol}.
 */
class LogAppenderDefault extends LogAppenderBase {
  LogAppenderDefault(RaftServer.Division server, LeaderState leaderState, FollowerInfo f) {
    super(server, leaderState, f);
  }

  /** Send an appendEntries RPC; retry indefinitely. */
  @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NULL_VALUE")
  private AppendEntriesReplyProto sendAppendEntriesWithRetries()
      throws InterruptedException, InterruptedIOException, RaftLogIOException {
    int retry = 0;
    AppendEntriesRequestProto request = null;
    while (isRunning()) { // keep retrying for IOException
      try {
        if (request == null || request.getEntriesCount() == 0) {
          request = newAppendEntriesRequest(CallId.getAndIncrement(), false);
        }

        if (request == null) {
          LOG.trace("{} no entries to send now, wait ...", this);
          return null;
        } else if (!isRunning()) {
          LOG.info("{} is stopped. Skip appendEntries.", this);
          return null;
        }

        if (request.getEntriesCount() == 0) {
          getFollower().updateLastHeartBeatSendTime();
        }
        Timestamp sendTime = Timestamp.currentTime();
        final AppendEntriesReplyProto r = getServerRpc().appendEntries(request);
        getFollower().updateLastRpcResponseTime(sendTime);

        getLeaderState().onFollowerCommitIndex(getFollower(), r.getFollowerCommit());
        return r;
      } catch (InterruptedIOException | RaftLogIOException e) {
        throw e;
      } catch (IOException ioe) {
        // TODO should have more detailed retry policy here.
        if (retry++ % 10 == 0) { // to reduce the number of messages
          LOG.warn("{}: Failed to appendEntries (retry={}): {}", this, retry++, ioe);
        }
        handleException(ioe);
      }
      if (isRunning()) {
        getServer().properties().rpcSleepTime().sleep();
      }
    }
    return null;
  }

  private InstallSnapshotReplyProto installSnapshot(SnapshotInfo snapshot) throws InterruptedIOException {
    String requestId = UUID.randomUUID().toString();
    InstallSnapshotReplyProto reply = null;
    try {
      for (InstallSnapshotRequestProto request : newInstallSnapshotRequests(requestId, snapshot)) {
        Timestamp sendTime = Timestamp.currentTime();
        reply = getServerRpc().installSnapshot(request);
        getFollower().updateLastRpcResponseTime(sendTime);

        if (!reply.getServerReply().getSuccess()) {
          return reply;
        }
      }
    } catch (InterruptedIOException iioe) {
      throw iioe;
    } catch (Exception ioe) {
      LOG.warn("{}: Failed to installSnapshot {}: {}", this, snapshot, ioe);
      handleException(ioe);
      return null;
    }

    if (reply != null) {
      getFollower().setSnapshotIndex(snapshot.getTermIndex().getIndex());
      LOG.info("{}: installSnapshot {} successfully", this, snapshot);
      getServer().getRaftServerMetrics().onSnapshotInstalled();
    }
    return reply;
  }

  @Override
  public void run() throws InterruptedException, IOException {
    while (isRunning()) {
      if (shouldSendAppendEntries()) {
        SnapshotInfo snapshot = shouldInstallSnapshot();
        if (snapshot != null) {
          LOG.info("{}: followerNextIndex = {} but logStartIndex = {}, send snapshot {} to follower",
              this, getFollower().getNextIndex(), getRaftLog().getStartIndex(), snapshot);

          final InstallSnapshotReplyProto r = installSnapshot(snapshot);
          if (r != null && r.getResult() == InstallSnapshotResult.NOT_LEADER) {
            onFollowerTerm(r.getTerm());
          } // otherwise if r is null, retry the snapshot installation
        } else {
          final AppendEntriesReplyProto r = sendAppendEntriesWithRetries();
          if (r != null) {
            handleReply(r);
          }
        }
      }
      if (isRunning() && !hasAppendEntries()) {
        final long waitTime = getHeartbeatRemainingTimeMs();
        if (waitTime > 0) {
          synchronized (this) {
            wait(waitTime);
          }
        }
      }
      getLeaderState().checkHealth(getFollower());
    }
  }

  private void handleReply(AppendEntriesReplyProto reply) throws IllegalArgumentException {
    if (reply != null) {
      switch (reply.getResult()) {
        case SUCCESS:
          final long oldNextIndex = getFollower().getNextIndex();
          final long nextIndex = reply.getNextIndex();
          if (nextIndex < oldNextIndex) {
            throw new IllegalStateException("nextIndex=" + nextIndex
                + " < oldNextIndex=" + oldNextIndex
                + ", reply=" + ServerStringUtils.toAppendEntriesReplyString(reply));
          }

          if (nextIndex > oldNextIndex) {
            getFollower().updateMatchIndex(nextIndex - 1);
            getFollower().increaseNextIndex(nextIndex);
            getLeaderState().onFollowerSuccessAppendEntries(getFollower());
          }
          break;
        case NOT_LEADER:
          // check if should step down
          onFollowerTerm(reply.getTerm());
          break;
        case INCONSISTENCY:
          getFollower().decreaseNextIndex(reply.getNextIndex());
          break;
        case UNRECOGNIZED:
          LOG.warn("{}: received {}", this, reply.getResult());
          break;
        default: throw new IllegalArgumentException("Unable to process result " + reply.getResult());
      }
    }
  }

  private void handleException(Exception e) {
    LOG.trace("TRACE", e);
    getServerRpc().handleException(getFollowerId(), e, false);
  }
}
