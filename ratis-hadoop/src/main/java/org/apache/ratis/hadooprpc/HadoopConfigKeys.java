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

import static org.apache.ratis.conf.ConfUtils.getInetSocketAddress;
import static org.apache.ratis.conf.ConfUtils.getInt;
import static org.apache.ratis.conf.ConfUtils.printAll;
import static org.apache.ratis.conf.ConfUtils.requireMin;
import static org.apache.ratis.conf.ConfUtils.set;

import org.apache.hadoop.conf.Configuration;
import org.apache.ratis.conf.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.function.Consumer;


/** Hadoop Rpc specific configuration properties. */
public interface HadoopConfigKeys {
  String PREFIX = "raft.hadoop";

  String CONF_PARAMETER = PREFIX + ".conf";
  Class<Configuration> CONF_CLASS = Configuration.class;

  static Configuration getConf(Parameters parameters) {
    return parameters.get(CONF_PARAMETER, CONF_CLASS);
  }

  static void setConf(Parameters parameters, Configuration conf) {
    parameters.put(CONF_PARAMETER, conf, Configuration.class);
  }

  /** IPC server configurations */
  interface Ipc {
    Logger LOG = LoggerFactory.getLogger(Ipc.class);
    static Consumer<String> getDefaultLog() {
      return LOG::info;
    }

    String PREFIX = HadoopConfigKeys.PREFIX + ".ipc";

    String ADDRESS_KEY = PREFIX + ".address";
    int PORT_DEFAULT = 10718;
    String ADDRESS_DEFAULT = "0.0.0.0:" + PORT_DEFAULT;

    String HANDLERS_KEY = PREFIX + ".handlers";
    int HANDLERS_DEFAULT = 10;

    static int handlers(Configuration conf) {
      return getInt(conf::getInt, HANDLERS_KEY, HANDLERS_DEFAULT, getDefaultLog(), requireMin(1));
    }

    static void setHandlers(Configuration conf, int handers) {
      set(conf::setInt, HANDLERS_KEY, handers);
    }

    static InetSocketAddress address(Configuration conf) {
      return getInetSocketAddress(conf::getTrimmed, ADDRESS_KEY, ADDRESS_DEFAULT, getDefaultLog());
    }

    static void setAddress(Configuration conf, String address) {
      set(conf::set, ADDRESS_KEY, address);
    }
  }

  static void main(String[] args) {
    printAll(HadoopConfigKeys.class);
  }
}
