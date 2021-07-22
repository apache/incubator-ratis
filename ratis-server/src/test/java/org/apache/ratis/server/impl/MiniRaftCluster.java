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

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.CallId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.ServerFactory;
import org.apache.ratis.server.raftlog.memory.MemoryRaftLog;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ReflectionUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class MiniRaftCluster implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(MiniRaftCluster.class);

  public static final String CLASS_NAME = JavaUtils.getClassSimpleName(MiniRaftCluster.class);
  public static final String STATEMACHINE_CLASS_KEY = CLASS_NAME + ".statemachine.class";
  private static final StateMachine.Registry STATEMACHINE_REGISTRY_DEFAULT = gid -> new BaseStateMachine();
  private static final TimeDuration RETRY_INTERVAL_DEFAULT =
      TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);

  public static abstract class Factory<CLUSTER extends MiniRaftCluster> {
    public interface Get<CLUSTER extends MiniRaftCluster> {
      Supplier<RaftProperties> properties = JavaUtils.memoize(RaftProperties::new);

      Factory<CLUSTER> getFactory();

      default RaftProperties getProperties() {
        return properties.get();
      }

      default RaftProperties setStateMachine(Class<? extends StateMachine> stateMachineClass) {
        final RaftProperties p = getProperties();
        p.setClass(STATEMACHINE_CLASS_KEY, stateMachineClass, StateMachine.class);
        return p;
      }

      default CLUSTER newCluster(int numPeers) {
        return getFactory().newCluster(numPeers, getProperties());
      }

      default CLUSTER newCluster(int numPeers, int numListeners) {
        return getFactory().newCluster(numPeers, numListeners, getProperties());
      }

      default void runWithNewCluster(int numServers, CheckedConsumer<CLUSTER, Exception> testCase) throws Exception {
        runWithNewCluster(numServers, true, testCase);
      }

      default void runWithNewCluster(int numServers, int numListeners, CheckedConsumer<CLUSTER, Exception> testCase) throws Exception {
        runWithNewCluster(numServers, numListeners,true, testCase);
      }

      default void runWithNewCluster(int numServers, boolean startCluster, CheckedConsumer<CLUSTER, Exception> testCase)
          throws Exception {
        runWithNewCluster(numServers, 0, startCluster, testCase);
      }

      default void runWithNewCluster(int numServers, int numListeners, boolean startCluster, CheckedConsumer<CLUSTER, Exception> testCase)
          throws Exception {
        final StackTraceElement caller = JavaUtils.getCallerStackTraceElement();
        LOG.info("Running " + caller.getMethodName());
        final CLUSTER cluster = newCluster(numServers, numListeners);
        try {
          if (startCluster) {
            cluster.start();
          }
          testCase.accept(cluster);
        } catch(Exception t) {
          LOG.info(cluster.printServers());
          LOG.error("Failed " + caller, t);
          throw t;
        } finally {
          cluster.shutdown();
        }
      }

      default void runWithSameCluster(int numServers, CheckedConsumer<CLUSTER, Exception> testCase) throws Exception {
        final StackTraceElement caller = JavaUtils.getCallerStackTraceElement();
        LOG.info("Running " + caller.getMethodName());
        CLUSTER cluster = null;
        try {
          cluster = getFactory().reuseCluster(numServers, getProperties());
          testCase.accept(cluster);
        } catch(Exception t) {
          if (cluster != null) {
            LOG.info(cluster.printServers());
          }
          LOG.error("Failed " + caller, t);
          throw t;
        }
      }
    }

    private final AtomicReference<CLUSTER> reusableCluster = new AtomicReference<>();

    private CLUSTER reuseCluster(int numServers, RaftProperties prop) throws IOException {
      for(;;) {
        final CLUSTER cluster = reusableCluster.get();
        if (cluster != null) {
          return cluster;
        }

        final CLUSTER newCluster = newCluster(numServers, prop);
        if (reusableCluster.compareAndSet(null, newCluster)) {
          newCluster.start();
          Runtime.getRuntime().addShutdownHook(new Thread(newCluster::shutdown));
          return newCluster;
        }
      }
    }

    public abstract CLUSTER newCluster(
        String[] ids, String[] idOfListeners, RaftProperties prop);

    public CLUSTER newCluster(
        String[] ids, RaftProperties prop) {
      return newCluster(ids, null, prop);
    }

    public CLUSTER newCluster(int numServer, RaftProperties prop) {
      return newCluster(generateIds(numServer, 0), new String[0], prop);
    }

    public CLUSTER newCluster(int numServer, int numListener, RaftProperties prop) {
      return newCluster(generateIds(numServer, 0), generateIds(numListener, numServer), prop);
    }
  }

  public static abstract class RpcBase extends MiniRaftCluster {
    public RpcBase(String[] ids, String[] idOfListeners, RaftProperties properties, Parameters parameters) {
      super(ids, idOfListeners, properties, parameters);
    }

    public RpcBase(String[] ids, RaftProperties properties, Parameters parameters) {
      super(ids, null, properties, parameters);
    }

    @Override
    public void setBlockRequestsFrom(String src, boolean block) {
      if (block) {
        BlockRequestHandlingInjection.getInstance().blockRequestor(src);
      } else {
        BlockRequestHandlingInjection.getInstance().unblockRequestor(src);
      }
    }

    protected int getPort(RaftPeerId id, RaftGroup g) {
      return getPort(getAddress(id, g, RaftPeer::getAddress));
    }

    protected String getAddress(RaftPeerId id, RaftGroup g, Function<RaftPeer, String> getAddress) {
      final RaftPeer p = g != null? g.getPeer(id): peers.get(id);
      return p == null? null : getAddress.apply(p);
    }

    protected int getDataStreamPort(RaftPeerId id, RaftGroup g) {
      final RaftPeer p = g != null? g.getPeer(id): peers.get(id);
      final String address = p == null? null : p.getDataStreamAddress();
      return getPort(address);
    }

    private int getPort(String address) {
      final InetSocketAddress inetAddress = address != null?
          NetUtils.createSocketAddr(address): NetUtils.createLocalServerAddress();
      return inetAddress.getPort();
    }
  }

  public static class PeerChanges {
    public final RaftPeer[] allPeersInNewConf;
    public final RaftPeer[] newPeers;
    public final RaftPeer[] removedPeers;

    public PeerChanges(RaftPeer[] all, RaftPeer[] newPeers, RaftPeer[] removed) {
      this.allPeersInNewConf = all;
      this.newPeers = newPeers;
      this.removedPeers = removed;
    }
  }

  public static RaftGroup initRaftGroup(Collection<String> ids, Collection<String> idOfListeners) {
    int addressCount = 4 * ids.size() + (idOfListeners != null ? idOfListeners.size() * 4 : 0);
    Iterator<InetSocketAddress> addresses = NetUtils.createLocalServerAddress(addressCount).iterator();
    final List<RaftPeer> peers = ids.stream()
        .map(RaftPeerId::valueOf)
        .map(id -> RaftPeer.newBuilder().setId(id)
            .setAddress(addresses.next())
            .setAdminAddress(addresses.next())
            .setClientAddress(addresses.next())
            .setDataStreamAddress(addresses.next())
            .build())
        .collect(Collectors.toList());

    final List<RaftPeer> listeners = idOfListeners.stream()
        .map(RaftPeerId::valueOf)
        .map(id -> RaftPeer.newBuilder().setId(id)
            .setAddress(addresses.next())
            .setAdminAddress(addresses.next())
            .setClientAddress(addresses.next())
            .setDataStreamAddress(addresses.next())
            .build())
        .collect(Collectors.toList());
    return RaftGroup.valueOf(RaftGroupId.randomId(), peers, listeners);
  }

  private final Supplier<File> rootTestDir = JavaUtils.memoize(
      () -> new File(BaseTest.getRootTestDir(),
          JavaUtils.getClassSimpleName(getClass()) + Integer.toHexString(ThreadLocalRandom.current().nextInt())));

  public File getStorageDir(RaftPeerId id) {
    return new File(rootTestDir.get(), id.toString());
  }

  public static String[] generateIds(int numServers, int base) {
    String[] ids = new String[numServers];
    for (int i = 0; i < numServers; i++) {
      ids[i] = "s" + (i + base);
    }
    return ids;
  }

  public static int getIdIndex(String id) {
    return Integer.parseInt(id.substring(1));
  }

  protected RaftGroup group;
  protected final RaftProperties properties;
  protected final Parameters parameters;
  protected final Map<RaftPeerId, RaftServerProxy> servers = new ConcurrentHashMap<>();
  protected final Map<RaftPeerId, RaftPeer> peers = new ConcurrentHashMap<>();
  protected final Map<RaftPeerId, RaftPeer> listeners = new ConcurrentHashMap<>();

  private volatile StateMachine.Registry stateMachineRegistry = null;

  private final AtomicReference<Timer> timer = new AtomicReference<>();

  protected MiniRaftCluster(String[] ids, RaftProperties properties, Parameters parameters) {
    this(ids, new String[]{}, properties, parameters);
  }

  protected MiniRaftCluster(String[] ids, String[] idOfListeners, RaftProperties properties, Parameters parameters) {
    this.group = initRaftGroup(Arrays.asList(ids),
        idOfListeners != null ? Arrays.asList(idOfListeners) : Collections.emptyList());
    LOG.info("new {} with {}", JavaUtils.getClassSimpleName(getClass()), group);
    this.properties = new RaftProperties(properties);
    this.parameters = parameters;

    ExitUtils.disableSystemExit();
  }

  public RaftProperties getProperties() {
    return properties;
  }

  public MiniRaftCluster initServers() {
    LOG.info("servers = " + servers);
    if (servers.isEmpty()) {
      putNewServers(CollectionUtils.as(group.getPeers(), RaftPeer::getId),
          true, false);
    }
    return this;
  }

  public RaftServerProxy putNewServer(RaftPeerId id, RaftGroup group, boolean format) {
    final RaftServerProxy s = newRaftServer(id, group, format);
    Preconditions.assertTrue(servers.put(id, s) == null);
    peers.put(id, s.getPeer());
    return s;
  }

  public RaftServerProxy putNewListener(RaftPeerId id, RaftGroup group, boolean format) {
    final RaftServerProxy s = newRaftServer(id, group, format);
    Preconditions.assertTrue(servers.put(id, s) == null);
    listeners.put(id, s.getPeer());
    return s;
  }

  private Collection<RaftServer> putNewServers(
      Iterable<RaftPeerId> peers, boolean format, boolean emptyPeer) {
    if (emptyPeer) {
      RaftGroup raftGroup = RaftGroup.valueOf(group.getGroupId(),
          Collections.EMPTY_LIST);
      return StreamSupport.stream(peers.spliterator(), false)
          .map(id -> putNewServer(id, raftGroup, format))
          .collect(Collectors.toList());
    } else {
      return StreamSupport.stream(peers.spliterator(), false)
          .map(id -> putNewServer(id, group, format))
          .collect(Collectors.toList());
    }
  }

  private Collection<RaftServer> putNewListeners(
      Iterable<RaftPeerId> listeners, boolean format) {
    return StreamSupport.stream(listeners.spliterator(), false)
        .map(id -> putNewServer(id, group, format))
        .collect(Collectors.toList());
  }

  public Collection<RaftPeer> getListeners() {
    return listeners.values();
  }

  public void start() throws IOException {
    LOG.info(".............................................................. ");
    LOG.info("... ");
    LOG.info("...     Starting " + JavaUtils.getClassSimpleName(getClass()));
    LOG.info("... ");
    LOG.info(".............................................................. ");

    initServers();
    startServers(servers.values());

    this.timer.updateAndGet(t -> t != null? t
        : JavaUtils.runRepeatedly(() -> LOG.info("TIMED-PRINT: " + printServers()), 10, 10, TimeUnit.SECONDS));
  }

  /**
   * start a stopped server again.
   */
  public RaftServer.Division restartServer(RaftPeerId serverId, boolean format) throws IOException {
    return restartServer(serverId, group, format);
  }

  public RaftServer.Division restartServer(RaftPeerId serverId, RaftGroup group, boolean format) throws IOException {
    killServer(serverId);
    servers.remove(serverId);

    final RaftServer proxy = putNewServer(serverId, group, format);
    proxy.start();
    return group == null? null: proxy.getDivision(group.getGroupId());
  }

  public void restart(boolean format) throws IOException {
    shutdown();

    List<RaftPeerId> idList = new ArrayList<>(servers.keySet());
    servers.clear();
    putNewServers(idList, format, false);
    start();
  }

  public TimeDuration getTimeoutMax() {
    return RaftServerConfigKeys.Rpc.timeoutMax(properties);
  }

  private RaftServerProxy newRaftServer(RaftPeerId id, RaftGroup group, boolean format) {
    LOG.info("newRaftServer: {}, {}, format? {}", id, group, format);
    try {
      final File dir = getStorageDir(id);
      if (format) {
        FileUtils.deleteFully(dir);
        LOG.info("Formatted directory {}", dir);
      }
      final RaftProperties prop = new RaftProperties(properties);
      RaftServerConfigKeys.setStorageDir(prop, Collections.singletonList(dir));
      return ServerImplUtils.newRaftServer(id, group, getStateMachineRegistry(prop), prop,
          setPropertiesAndInitParameters(id, group, prop));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract Parameters setPropertiesAndInitParameters(
      RaftPeerId id, RaftGroup group, RaftProperties properties);

  public void setStateMachineRegistry(StateMachine.Registry stateMachineRegistry) {
    this.stateMachineRegistry = stateMachineRegistry;
  }

  StateMachine.Registry getStateMachineRegistry(RaftProperties properties) {
    if (stateMachineRegistry != null) {
      return stateMachineRegistry;
    }

    final Class<? extends StateMachine> smClass = properties.getClass(
        STATEMACHINE_CLASS_KEY, null, StateMachine.class);
    if (smClass == null) {
      return STATEMACHINE_REGISTRY_DEFAULT;
    }

    return gid -> {
      final RuntimeException exception;
      try {
        return ReflectionUtils.newInstance(smClass);
      } catch(RuntimeException e) {
        exception = e;
      }

      try {
        final Class<?>[] argClasses = {RaftProperties.class};
        return ReflectionUtils.newInstance(smClass, argClasses, properties);
      } catch(RuntimeException e) {
        exception.addSuppressed(e);
      }
      throw exception;
    };
  }

  private static List<RaftPeer> toRaftPeers(Iterable<RaftServer> servers) {
    return StreamSupport.stream(servers.spliterator(), false)
        .map(RaftServer::getPeer)
        .collect(Collectors.toList());
  }

  public PeerChanges addNewPeers(int number, boolean startNewPeer)
      throws IOException {
    return addNewPeers(generateIds(number, servers.size()), startNewPeer, false);
  }

  public PeerChanges addNewPeers(int number, boolean startNewPeer,
      boolean emptyPeer) throws IOException {
    return addNewPeers(generateIds(number, servers.size()), startNewPeer, emptyPeer);
  }

  public PeerChanges addNewPeers(String[] ids, boolean startNewPeer,
      boolean emptyPeer) throws IOException {
    LOG.info("Add new peers {}", Arrays.asList(ids));

    // create and add new RaftServers
    final Collection<RaftServer> newServers = putNewServers(
        CollectionUtils.as(Arrays.asList(ids), RaftPeerId::valueOf), true, emptyPeer);

    startServers(newServers);
    if (!startNewPeer) {
      // start and then close, in order to bind the port
      for(RaftServer s : newServers) {
        s.close();
      }
    }

    final Collection<RaftPeer> newPeers = toRaftPeers(newServers);
    final RaftPeer[] np = newPeers.toArray(RaftPeer.emptyArray());
    newPeers.addAll(group.getPeers());
    RaftPeer[] p = newPeers.toArray(RaftPeer.emptyArray());
    group = RaftGroup.valueOf(group.getGroupId(), p);
    return new PeerChanges(p, np, RaftPeer.emptyArray());
  }

  static void startServers(Iterable<? extends RaftServer> servers) throws IOException {
    for(RaftServer s : servers) {
      s.start();
    }
  }

  /**
   * prepare the peer list when removing some peers from the conf
   */
  public PeerChanges removePeers(int number, boolean removeLeader,
      Collection<RaftPeer> excluded) throws InterruptedException {
    Collection<RaftPeer> peers = new ArrayList<>(group.getPeers());
    List<RaftPeer> removedPeers = new ArrayList<>(number);
    if (removeLeader) {
      final RaftPeer leader = RaftTestUtil.waitForLeader(this).getPeer();
      Preconditions.assertTrue(!excluded.contains(leader));
      peers.remove(leader);
      removedPeers.add(leader);
    }
    final List<RaftServer.Division> followers = getFollowers();
    for (int i = 0, removed = 0; i < followers.size() &&
        removed < (removeLeader ? number - 1 : number); i++) {
      RaftPeer toRemove = followers.get(i).getPeer();
      if (!excluded.contains(toRemove)) {
        peers.remove(toRemove);
        removedPeers.add(toRemove);
        removed++;
      }
    }
    final RaftPeer[] p = peers.toArray(RaftPeer.emptyArray());
    group = RaftGroup.valueOf(group.getGroupId(), p);
    return new PeerChanges(p, RaftPeer.emptyArray(), removedPeers.toArray(RaftPeer.emptyArray()));
  }

  public void killServer(RaftPeerId id) {
    LOG.info("killServer " + id);
    servers.get(id).close();
  }

  public String printServers() {
    return printServers(null);
  }

  public String printServers(RaftGroupId groupId) {
    final StringBuilder b = new StringBuilder("printing ");
    if (groupId != null) {
      b.append(groupId);
    } else {
      b.append("ALL groups");
    }
    getRaftServerProxyStream(groupId).forEach(s -> b.append("\n  ").append(s));
    return b.toString();
  }

  public String printAllLogs() {
    StringBuilder b = new StringBuilder("\n#servers = " + servers.size() + "\n");
    for (RaftServer.Division s : iterateDivisions()) {
      b.append("  ");
      b.append(s).append("\n");

      final RaftLog log = s.getRaftLog();
      if (log instanceof MemoryRaftLog) {
        b.append("    ");
        b.append(((MemoryRaftLog) log).getEntryString());
      }
    }
    return b.toString();
  }

  public RaftServer.Division getLeaderAndSendFirstMessage(boolean ignoreException) throws IOException {
    final RaftServer.Division leader = getLeader();
    try(RaftClient client = createClient(leader.getId())) {
      client.io().send(new RaftTestUtil.SimpleMessage("first msg to make leader ready"));
    } catch (IOException e) {
      if (!ignoreException) {
        throw e;
      }
    }
    return leader;
  }

  public IllegalStateException newIllegalStateExceptionForNoLeaders(RaftGroupId groupId) {
    final String g = groupId == null? "": " for " + groupId;
    return new IllegalStateException("No leader yet " + g + ": " + printServers(groupId));
  }

  public IllegalStateException newIllegalStateExceptionForMultipleLeaders(RaftGroupId groupId,
      List<RaftServer.Division> leaders) {
    final String g = groupId == null? "": " for " + groupId;
    return new IllegalStateException("Found multiple leaders" + g
        + " at the same term (=" + leaders.get(0).getInfo().getCurrentTerm()
        + "), leaders.size() = " + leaders.size() + " > 1, leaders = " + leaders
        + ": " + printServers(groupId));
  }

  /**
   * Get leader for the single group case.
   * Do not use this method if this cluster has multiple groups.
   *
   * @return the unique leader with the highest term. Or, return null if there is no leader.
   * @throws IllegalStateException if there are multiple leaders with the same highest term.
   */
  public RaftServer.Division getLeader() {
    return getLeader(getLeaders(null), null, leaders -> {
      throw newIllegalStateExceptionForMultipleLeaders(null, leaders);
    });
  }

  public RaftServer.Division getLeader(RaftGroupId groupId, Runnable handleNoLeaders,
      Consumer<List<RaftServer.Division>> handleMultipleLeaders) {
    return getLeader(getLeaders(groupId), handleNoLeaders, handleMultipleLeaders);
  }

  static RaftServer.Division getLeader(List<RaftServer.Division> leaders, Runnable handleNoLeaders,
      Consumer<List<RaftServer.Division>> handleMultipleLeaders) {
    if (leaders.isEmpty()) {
      if (handleNoLeaders != null) {
        handleNoLeaders.run();
      }
      return null;
    } else if (leaders.size() > 1) {
      if (handleMultipleLeaders != null) {
        handleMultipleLeaders.accept(leaders);
      }
      return null;
    } else {
      return leaders.get(0);
    }
  }

  /**
   * @return the list of leaders with the highest term (i.e. leaders with a lower term are not included).
   *         from the given group.
   */
  private List<RaftServer.Division> getLeaders(RaftGroupId groupId) {
    final Stream<RaftServer.Division> serverAliveStream = getServerAliveStream(groupId);
    final List<RaftServer.Division> leaders = new ArrayList<>();
    serverAliveStream.filter(server -> server.getInfo().isLeader()).forEach(s -> {
      if (leaders.isEmpty()) {
        leaders.add(s);
      } else {
        final long leaderTerm = leaders.get(0).getInfo().getCurrentTerm();
        final long term = s.getInfo().getCurrentTerm();
        if (term >= leaderTerm) {
          if (term > leaderTerm) {
            leaders.clear();
          }
          leaders.add(s);
        }
      }
    });
    return leaders;
  }

  boolean isLeader(String leaderId) {
    final RaftServer.Division leader = getLeader();
    return leader != null && leader.getId().toString().equals(leaderId);
  }

  public List<RaftServer.Division> getFollowers() {
    return getServerAliveStream()
        .filter(server -> server.getInfo().isFollower())
        .collect(Collectors.toList());
  }

  public int getNumServers() {
    return servers.size();
  }

  public Iterable<RaftServer> getServers() {
    return CollectionUtils.as(servers.values(), s -> s);
  }

  private Stream<RaftServerProxy> getRaftServerProxyStream(RaftGroupId groupId) {
    return servers.values().stream()
        .filter(s -> groupId == null || s.getGroupIds().contains(groupId));
  }

  public Iterable<RaftServer.Division> iterateDivisions() {
    return CollectionUtils.as(getServers(), this::getDivision);
  }

  private Stream<RaftServer.Division> getServerStream(RaftGroupId groupId) {
    final Stream<RaftServerProxy> stream = getRaftServerProxyStream(groupId);
    return groupId != null?
        stream.map(s -> JavaUtils.callAsUnchecked(() -> s.getDivision(groupId)))
        : stream.flatMap(s -> JavaUtils.callAsUnchecked(s::getImpls).stream());
  }

  public Stream<RaftServer.Division> getServerAliveStream() {
    return getServerAliveStream(getGroupId());
  }

  private Stream<RaftServer.Division> getServerAliveStream(RaftGroupId groupId) {
    return getServerStream(groupId).filter(server -> server.getInfo().isAlive());
  }

  private RetryPolicy getDefaultRetryPolicy() {
    return RetryPolicies.retryForeverWithSleep(RETRY_INTERVAL_DEFAULT);
  }

  public RaftServerProxy getServer(RaftPeerId id) {
    return servers.get(id);
  }

  public ServerFactory getServerFactory(RaftPeerId id) {
    return servers.get(id).getFactory();
  }

  public RaftServer.Division getDivision(RaftPeerId id) {
    return getDivision(servers.get(id));
  }

  public RaftServer.Division getDivision(RaftPeerId id, RaftGroupId groupId) {
    return RaftServerTestUtil.getDivision(servers.get(id), groupId);
  }

  public RaftServer.Division getDivision(RaftServer server) {
    return RaftServerTestUtil.getDivision(server, getGroupId());
  }

  public List<RaftPeer> getPeers() {
    return toRaftPeers(getServers());
  }

  public RaftGroup getGroup() {
    return group;
  }

  public RaftClient createClient() {
    return createClient(null, group);
  }

  public RaftClient createClient(RaftGroup g) {
    return createClient(null, g);
  }

  public RaftClient createClient(RaftPeerId leaderId) {
    return createClient(leaderId, group);
  }

  public RaftClient createClient(RetryPolicy retryPolicy) {
    return createClient(null, group, retryPolicy);
  }

  public RaftClient createClient(RaftPeerId leaderId, RetryPolicy retryPolicy) {
    return createClient(leaderId, group, retryPolicy);
  }

  public RaftClient createClient(RaftPeerId leaderId, RaftGroup group) {
    return createClient(leaderId, group, getDefaultRetryPolicy());
  }

  public RaftClient createClient(RaftPeer primaryServer) {
    return createClient(null, group, getDefaultRetryPolicy(), primaryServer);
  }

  public RaftClient createClient(RaftPeerId leaderId, RaftGroup group, RetryPolicy retryPolicy) {
    return createClient(leaderId, group, retryPolicy, null);
  }

  public RaftClient createClient(RaftPeerId leaderId, RaftGroup group, RetryPolicy retryPolicy, RaftPeer primaryServer) {
    RaftClient.Builder builder = RaftClient.newBuilder()
        .setRaftGroup(group)
        .setLeaderId(leaderId)
        .setProperties(properties)
        .setParameters(parameters)
        .setPrimaryDataStreamServer(primaryServer)
        .setRetryPolicy(retryPolicy);
    return builder.build();
  }

  public RaftClientRequest newRaftClientRequest(
      ClientId clientId, RaftPeerId leaderId, Message message) {
    return newRaftClientRequest(clientId, leaderId, CallId.getDefault(), message);
  }

  public RaftClientRequest newRaftClientRequest(
      ClientId clientId, RaftPeerId leaderId, long callId, Message message) {
    return RaftClientRequest.newBuilder()
        .setClientId(clientId)
        .setServerId(leaderId)
        .setGroupId(getGroupId())
        .setCallId(callId)
        .setMessage(message)
        .setType(RaftClientRequest.writeRequestType())
        .build();
  }

  public SetConfigurationRequest newSetConfigurationRequest(
      ClientId clientId, RaftPeerId leaderId,
      RaftPeer... peers) {
    return new SetConfigurationRequest(clientId, leaderId, getGroupId(), CallId.getDefault(), Arrays.asList(peers));
  }

  public void setConfiguration(RaftPeer... peers) throws IOException {
    try(RaftClient client = createClient()) {
      LOG.info("Start changing the configuration: {}", Arrays.asList(peers));
      final RaftClientReply reply = client.admin().setConfiguration(peers);
      Preconditions.assertTrue(reply.isSuccess());
    }
  }

  @Override
  public void close() {
    shutdown();
  }

  public void shutdown() {
    LOG.info("************************************************************** ");
    LOG.info("*** ");
    LOG.info("***     Stopping " + JavaUtils.getClassSimpleName(getClass()));
    LOG.info("*** ");
    LOG.info("************************************************************** ");
    LOG.info(printServers());

    // TODO: classes like RaftLog may throw uncaught exception during shutdown (e.g. write after close)
    ExitUtils.setTerminateOnUncaughtException(false);

    final ExecutorService executor = Executors.newFixedThreadPool(servers.size(), Daemon::new);
    getServers().forEach(proxy -> executor.submit(() -> JavaUtils.runAsUnchecked(proxy::close)));
    try {
      executor.shutdown();
      // just wait for a few seconds
      executor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("shutdown interrupted", e);
      Thread.currentThread().interrupt();
    }

    Optional.ofNullable(timer.get()).ifPresent(Timer::cancel);
    ExitUtils.assertNotTerminated();
    LOG.info("{} shutdown completed", JavaUtils.getClassSimpleName(getClass()));
  }

  /**
   * Block all the incoming requests for the peer with leaderId. Also delay
   * outgoing or incoming msg for all other peers.
   */
  protected abstract void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException;

  /**
   * Try to enforce the leader of the cluster.
   * @param leaderId ID of the targeted leader server.
   * @return true if server has been successfully enforced to the leader, false
   *         otherwise.
   */
  public boolean tryEnforceLeader(String leaderId) throws InterruptedException {
    // do nothing and see if the given id is already a leader.
    if (isLeader(leaderId)) {
      return true;
    }

    // Blocking all other server's RPC read process to make sure a read takes at
    // least ELECTION_TIMEOUT_MIN. In this way when the target leader request a
    // vote, all non-leader servers can grant the vote.
    // Disable the target leader server RPC so that it can request a vote.
    blockQueueAndSetDelay(leaderId,
        RaftServerConfigKeys.Rpc.TIMEOUT_MIN_DEFAULT.toIntExact(TimeUnit.MILLISECONDS));

    // Reopen queues so that the vote can make progress.
    blockQueueAndSetDelay(leaderId, 0);

    return isLeader(leaderId);
  }

  /** Block/unblock the requests sent from the given source. */
  public abstract void setBlockRequestsFrom(String src, boolean block);

  public RaftGroupId getGroupId() {
    return group.getGroupId();
  }
}
