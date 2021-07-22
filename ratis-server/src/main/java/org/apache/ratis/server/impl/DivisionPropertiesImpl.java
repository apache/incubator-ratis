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
package org.apache.ratis.server.impl;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.DivisionProperties;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;

class DivisionPropertiesImpl implements DivisionProperties {
  private final TimeDuration rpcTimeoutMin;
  private final TimeDuration rpcTimeoutMax;
  private final TimeDuration leaderLeaseTimeout;
  private final TimeDuration rpcSleepTime;
  private final TimeDuration rpcSlownessTimeout;

  DivisionPropertiesImpl(RaftProperties properties) {
    this.rpcTimeoutMin = RaftServerConfigKeys.Rpc.timeoutMin(properties);
    this.rpcTimeoutMax = RaftServerConfigKeys.Rpc.timeoutMax(properties);
    Preconditions.assertTrue(rpcTimeoutMax.compareTo(rpcTimeoutMin) >= 0,
        "rpcTimeoutMax = %s < rpcTimeoutMin = %s", rpcTimeoutMax, rpcTimeoutMin);

    double ratio = RaftServerConfigKeys.Rpc.getLeaderLeaseTimeoutRatio(properties) * 1.0 / 100;
    this.leaderLeaseTimeout = this.rpcTimeoutMin.multiply(ratio);
    Preconditions.assertTrue(rpcTimeoutMin.compareTo(leaderLeaseTimeout) >= 0,
        "rpcTimeoutMin = %s < leaderLeaseTimeout = %s", rpcTimeoutMin, leaderLeaseTimeout);

    this.rpcSleepTime = RaftServerConfigKeys.Rpc.sleepTime(properties);
    this.rpcSlownessTimeout = RaftServerConfigKeys.Rpc.slownessTimeout(properties);
  }

  @Override
  public TimeDuration minRpcTimeout() {
    return rpcTimeoutMin;
  }

  @Override
  public TimeDuration leaderLeaseTimeout() {
    return leaderLeaseTimeout;
  }

  @Override
  public TimeDuration maxRpcTimeout() {
    return rpcTimeoutMax;
  }

  @Override
  public TimeDuration rpcSleepTime() {
    return rpcSleepTime;
  }

  @Override
  public TimeDuration rpcSlownessTimeout() {
    return rpcSlownessTimeout;
  }
}