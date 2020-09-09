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
package org.apache.ratis.server.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.StringUtils;


/**
 * Maintain the mappings between log index and corresponding raft configuration.
 * Initialized when starting the raft peer. The mappings are loaded from the
 * raft log, and updated while appending/truncating configuration related log
 * entries.
 */
public class ConfigurationManager {
  private final RaftConfiguration initialConf;
  private final NavigableMap<Long, RaftConfiguration> configurations = new TreeMap<>();
  /**
   * The current raft configuration. If configurations is not empty, should be
   * the last entry of the map. Otherwise is initialConf.
   */
  private RaftConfiguration currentConf;

  ConfigurationManager(RaftConfiguration initialConf) {
    this.initialConf = initialConf;
    this.currentConf = initialConf;
  }

  synchronized void addConfiguration(long logIndex, RaftConfiguration conf) {
    final RaftConfiguration found = configurations.get(logIndex);
    if (found != null) {
      Preconditions.assertTrue(found.equals(conf));
      return;
    }
    configurations.put(logIndex, conf);
    if (logIndex == configurations.lastEntry().getKey()) {
      currentConf = conf;
    }
  }

  synchronized RaftConfiguration getCurrent() {
    return currentConf;
  }

  /**
   * Remove all the configurations whose log index is >= the given index.
   * @param index The given index. All the configurations whose log index is >=
   *              this value will be removed.
   * @return The configuration with largest log index < the given index.
   */
  synchronized RaftConfiguration removeConfigurations(long index) {
    SortedMap<Long, RaftConfiguration> toRemove = configurations.tailMap(index);
    for (Iterator<Map.Entry<Long, RaftConfiguration>> iter =
         toRemove.entrySet().iterator(); iter.hasNext();) {
      iter.next();
      iter.remove();
    }
    currentConf = configurations.isEmpty() ? initialConf :
        configurations.lastEntry().getValue();
    return currentConf;
  }

  synchronized int numOfConf() {
    return 1 + configurations.size();
  }

  @Override
  public synchronized String toString() {
    return getClass().getSimpleName() + ", init=" + initialConf + ", confs=" + StringUtils.map2String(configurations);
  }

  // TODO: remove Configuration entries after they are committed
}
