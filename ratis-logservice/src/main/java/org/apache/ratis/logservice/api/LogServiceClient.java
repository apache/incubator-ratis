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
package org.apache.ratis.logservice.api;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.logservice.api.LogStream.State;
import org.apache.ratis.logservice.common.Constants;
import org.apache.ratis.logservice.impl.ArchivedLogStreamImpl;
import org.apache.ratis.logservice.impl.ExportedLogStreamImpl;
import org.apache.ratis.logservice.impl.LogStreamImpl;
import org.apache.ratis.logservice.proto.LogServiceProtos;
import org.apache.ratis.logservice.proto.MetaServiceProtos.CreateLogReplyProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.DeleteLogReplyProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.GetLogReplyProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.ListLogsReplyProto;
import org.apache.ratis.logservice.proto.MetaServiceProtos.LogInfoProto;
import org.apache.ratis.logservice.server.ArchivalInfo;
import org.apache.ratis.logservice.util.LogServiceProtoUtil;
import org.apache.ratis.logservice.util.MetaServiceProtoUtil;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;

import static org.apache.ratis.logservice.util.LogServiceUtils.getPeersFromQuorum;


/**
 * LogServiceClient is responsible for all meta service communications such as create/get/list logs.
 * Initialized by the metaQuorum string that has list of masters as "server:port' separated by a comma.
 * An example: 'server1.example.com:9999,server2.example.com:9999,server3.example.com:9999
 */

public class LogServiceClient implements AutoCloseable {


    // the raft client for meta quorum. All DML operations are going using this client.
    private final RaftClient client;
    private final LogServiceConfiguration config;

    /**
     * Constuctor. Build raft client for meta quorum
     * @param metaQuorum
     */
    public LogServiceClient(String metaQuorum) {
        this(metaQuorum, LogServiceConfiguration.create(), new RaftProperties());
    }

    /**
     * Constuctor. Build raft client for meta quorum
     * @param metaQuorum
     * @param properties
     */
    public LogServiceClient(String metaQuorum, RaftProperties properties) {
        this(metaQuorum, LogServiceConfiguration.create(), properties);
    }

    /**
     * Constuctor (with configuration). Build raft client for meta quorum
     * @param metaQuorum
     * @param config log service configuration
     * @param properties
     */
    public LogServiceClient(String metaQuorum, LogServiceConfiguration config, RaftProperties properties) {
        Set<RaftPeer> peers = getPeersFromQuorum(metaQuorum);
        RaftGroup meta = RaftGroup.valueOf(Constants.META_GROUP_ID, peers);
        client = RaftClient.newBuilder()
                .setRaftGroup(meta)
                .setClientId(ClientId.randomId())
                .setProperties(properties)
                .build();
        this.config = config;
    }

    /**
     * Create a new Log request.
     * @param logName the name of the log to create
     * @return
     * @throws IOException
     */
    public LogStream createLog(LogName logName) throws IOException {
        RaftClientReply reply = client.sendReadOnly(
            () -> MetaServiceProtoUtil.toCreateLogRequestProto(logName).toByteString());
        CreateLogReplyProto message =
            CreateLogReplyProto.parseFrom(reply.getMessage().getContent());
        if (message.hasException()) {
            throw MetaServiceProtoUtil.toMetaServiceException(message.getException());
        }
        LogInfo info = MetaServiceProtoUtil.toLogInfo(message.getLog());
        return new LogStreamImpl(logName, getRaftClient(info), config);
    }

    /**
     * Get log request.
     * @param logName the name of the log to get
     * @return
     * @throws IOException
     */
    public LogStream getLog(LogName logName) throws IOException {
        return new LogStreamImpl(logName, getRaftClient(getLogInfo(logName)), config);
    }

    /**
     * Get Archive log .
     * @param logName the name of the log to get
     * @return
     * @throws IOException
     */
    public LogStream getArchivedLog(LogName logName) throws IOException {
        return new ArchivedLogStreamImpl(logName, config);
    }

    /**
     * Get exported log .
     * @param logName the name of the log to get
     * @param location location of the exported log
     * @return
     * @throws IOException
     */

    public LogStream getExportLog(LogName logName, String location) throws IOException {
        return new ExportedLogStreamImpl(logName, location);
    }

    public List<ArchivalInfo> getExportStatus(LogName logName) throws IOException {
        try (RaftClient client = getRaftClient(getLogInfo(logName))) {
            RaftClientReply exportInfoReply = client.sendReadOnly(
                () -> LogServiceProtoUtil.toExportInfoRequestProto(logName).toByteString());
            LogServiceProtos.GetExportInfoReplyProto message =
                LogServiceProtos.GetExportInfoReplyProto
                    .parseFrom(exportInfoReply.getMessage().getContent());
            if (message.hasException()) {
                throw new IOException(message.getException().getErrorMsg());
            }
            return message.getInfoList().stream()
                .map(infoProto -> LogServiceProtoUtil.toExportInfo(infoProto))
                .collect(Collectors.toList());
        }
    }

    public void deleteLog(LogName logName) throws IOException {
        RaftClientReply reply = client.sendReadOnly
                (() -> MetaServiceProtoUtil.toDeleteLogRequestProto(logName).toByteString());
        DeleteLogReplyProto message = DeleteLogReplyProto.parseFrom(reply.getMessage().getContent());
        if(message.hasException()) {
            throw MetaServiceProtoUtil.toMetaServiceException(message.getException());
        }
    }

    /**
     * Return the list of available logs
     * @return
     * @throws IOException
     */
    public List<LogInfo> listLogs() throws IOException {
        RaftClientReply reply = client.sendReadOnly
                (() -> MetaServiceProtoUtil.toListLogRequestProto().toByteString());
        ListLogsReplyProto message = ListLogsReplyProto.parseFrom(reply.getMessage().getContent());
        List<LogInfoProto> infoProtos = message.getLogsList();
        List<LogInfo> infos = infoProtos.stream()
                .map(proto -> MetaServiceProtoUtil.toLogInfo(proto))
                .collect(Collectors.toList());
        return infos;
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    // Internal methods

    /**
     * Build a raft client for the particular log. Temporary here. TODO: Should be moved to LogService part
     * @param logInfo
     * @return
     */
    private RaftClient getRaftClient(LogInfo logInfo) throws IOException {

        RaftProperties properties = new RaftProperties();
        return RaftClient.newBuilder().setRaftGroup(logInfo.getRaftGroup()).setProperties(properties).build();

    }

    private LogInfo getLogInfo(LogName logName) throws IOException {
        RaftClientReply reply = client.sendReadOnly(
            () -> MetaServiceProtoUtil.toGetLogRequestProto(logName).toByteString());
        GetLogReplyProto message = GetLogReplyProto.parseFrom(reply.getMessage().getContent());
        if (message.hasException()) {
            throw MetaServiceProtoUtil.toMetaServiceException(message.getException());
        }
        return MetaServiceProtoUtil.toLogInfo(message.getLog());
    }

    /**
     * Archives the given log out of the state machine and into a configurable long-term storage.
     * A log must be in {@link State#CLOSED} to archive it.
     * Archiving of the log will happen asynchronously from the client,
     * The call will return immediately after adding a request for archiving log
     * to the respective quorum
     *
     * Client can check the status of Archiving by calling getState() Method
     *
     * @param logName The name of the log to archive.
     */
    public void archiveLog(LogName logName) throws IOException {
        exportLog(logName, null, 0);
    }

    /**
     * Export the given log out of the state machine and into a provided location on the configured storage
     * A log must be in {@link State#CLOSED} to export it.
     * exporting of the log will happen asynchronously from the client,
     * The call will return immediately after adding a request for archiving log
     * to the respective quorum
     *
     * Client can check the status of export by calling getState() Method
     *
     * @param logName The name of the log to archive.
     */
    public void exportLog(LogName logName, String location, long recordId) throws IOException {
        try (RaftClient client = getRaftClient(getLogInfo(logName))) {
            RaftClientReply archiveLogReply = client.sendReadOnly(() -> LogServiceProtoUtil
                .toArchiveLogRequestProto(logName, location, recordId,
                    location == null ? true : false, ArchivalInfo.ArchivalStatus.SUBMITTED)
                .toByteString());
            LogServiceProtos.ArchiveLogReplyProto archiveMessage =
                LogServiceProtos.ArchiveLogReplyProto
                    .parseFrom(archiveLogReply.getMessage().getContent());
            if (archiveMessage.hasException()) {
                throw new IOException(archiveMessage.getException().getErrorMsg());
            }
        }
    }

    /**
     * Moves the {@link LogStream} identified by the {@code name} from {@link State#OPEN} to {@link State#CLOSED}.
     * If the log is not {@link State#OPEN}, this method returns an error.
     *
     * @param name The name of the log to close
     */
    // TODO this name sucks, confusion WRT the Java Closeable interface.
    public void closeLog(LogName name) throws IOException {
        try (RaftClient client = getRaftClient(getLogInfo(name))) {
            RaftClientReply reply = client.send(
                () -> LogServiceProtoUtil.toChangeStateRequestProto(name, State.CLOSED)
                    .toByteString());
            LogServiceProtos.ChangeStateReplyProto message =
                LogServiceProtos.ChangeStateReplyProto.parseFrom(reply.getMessage().getContent());
        }
    }

    /**
     * Updates a log with the new configuration object, overriding
     * the previous configuration.
     *
     * @param conf The new configuration object
     */
    void updateConfiguration(LogName name, LogServiceConfiguration conf) {
      //TODO: write me
    }

}
