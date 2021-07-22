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
package org.apache.ratis.server;

import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * The properties set for a server division.
 *
 * @see RaftServerConfigKeys
 */
public interface DivisionProperties {
  Logger LOG = LoggerFactory.getLogger(DivisionProperties.class);

  /** @return the minimum rpc timeout. */
  TimeDuration minRpcTimeout();

  /** @return the minimum rpc timeout in milliseconds. */
  default int minRpcTimeoutMs() {
    return minRpcTimeout().toIntExact(TimeUnit.MILLISECONDS);
  }

  TimeDuration leaderLeaseTimeout();

  default int leaderLeaseTimeoutMs() {
    return leaderLeaseTimeout().toIntExact(TimeUnit.MILLISECONDS);
  }

  /** @return the maximum rpc timeout. */
  TimeDuration maxRpcTimeout();

  /** @return the maximum rpc timeout in milliseconds. */
  default int maxRpcTimeoutMs() {
    return maxRpcTimeout().toIntExact(TimeUnit.MILLISECONDS);
  }

  /** @return the rpc sleep time period. */
  TimeDuration rpcSleepTime();

  /** @return the rpc slowness timeout. */
  TimeDuration rpcSlownessTimeout();
}
