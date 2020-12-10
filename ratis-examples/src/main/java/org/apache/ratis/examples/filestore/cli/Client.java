/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.examples.filestore.cli;

import com.beust.jcommander.Parameter;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.examples.common.SubCommandBase;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Client to connect filestore example cluster.
 */
public abstract class Client extends SubCommandBase {

  @Parameter(names = {"--size"}, description = "Size of each file in bytes", required = true)
  private long fileSizeInBytes;

  @Parameter(names = {"--bufferSize"}, description = "Size of buffer in bytes, should less than 4MB, " +
      "i.e BUFFER_BYTE_LIMIT_DEFAULT", required = true)
  private int bufferSizeInBytes;

  @Parameter(names = {"--numFiles"}, description = "Number of files to be written", required = true)
  private int numFiles;

  @Parameter(names = {"--storage", "-s"}, description = "Storage dir, eg. --storage dir1 --storage dir2",
      required = true)
  private List<File> storageDir = new ArrayList<>();

  private static final int MAX_THREADS_NUM = 100;

  public int getNumThread() {
    return numFiles < MAX_THREADS_NUM ? numFiles : MAX_THREADS_NUM;
  }

  public long getFileSizeInBytes() {
    return fileSizeInBytes;
  }

  public int getBufferSizeInBytes() {
    return bufferSizeInBytes;
  }

  public int getNumFiles() {
    return numFiles;
  }

  @Override
  public void run() throws Exception {
    int raftSegmentPreallocatedSize = 1024 * 1024 * 1024;
    RaftProperties raftProperties = new RaftProperties();
    RaftConfigKeys.Rpc.setType(raftProperties, SupportedRpcType.GRPC);
    GrpcConfigKeys.setMessageSizeMax(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setWriteBufferSize(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setPreallocatedSize(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setSegmentSizeMax(raftProperties,
        SizeInBytes.valueOf(1 * 1024 * 1024 * 1024L));
    RaftConfigKeys.DataStream.setType(raftProperties, SupportedDataStreamType.NETTY);

    RaftServerConfigKeys.Log.setSegmentCacheNumMax(raftProperties, 2);

    RaftClientConfigKeys.Rpc.setRequestTimeout(raftProperties,
        TimeDuration.valueOf(50000, TimeUnit.MILLISECONDS));
    RaftClientConfigKeys.Async.setOutstandingRequestsMax(raftProperties, 1000);


    final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
            getPeers());

    RaftClient.Builder builder =
        RaftClient.newBuilder().setProperties(raftProperties);
    builder.setRaftGroup(raftGroup);
    builder.setClientRpc(new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), raftProperties));
    builder.setPrimaryDataStreamServer(getPrimary());
    RaftClient client = builder.build();

    for (File dir : storageDir) {
      FileUtils.createDirectories(dir);
    }

    operation(client);
  }


  protected void stop(RaftClient client) throws IOException {
    client.close();
    System.exit(0);
  }

  public String getPath(String fileName) {
    int hash = fileName.hashCode() % storageDir.size();
    return new File(storageDir.get(Math.abs(hash)), fileName).getAbsolutePath();
  }

  private CompletableFuture<Long> writeFileAsync(String path, ExecutorService executor) {
    final CompletableFuture<Long> future = new CompletableFuture<>();
    CompletableFuture.supplyAsync(() -> {
      try {
        future.complete(
            writeFile(path, fileSizeInBytes, bufferSizeInBytes, new Random().nextInt(127) + 1));
      } catch (IOException e) {
        future.completeExceptionally(e);
      }
      return future;
    }, executor);
    return future;
  }

  protected List<String> generateFiles(ExecutorService executor) {
    UUID uuid = UUID.randomUUID();
    List<String> paths = new ArrayList<>();
    List<CompletableFuture<Long>> futures = new ArrayList<>();
    for (int i = 0; i < numFiles; i ++) {
      String path = getPath("file-" + uuid + "-" + i);
      paths.add(path);
      futures.add(writeFileAsync(path, executor));
    }

    for (int i = 0; i < futures.size(); i ++) {
      long size = futures.get(i).join();
      if (size != fileSizeInBytes) {
        System.err.println("Error: path:" + paths.get(i) + " write:" + size +
            " mismatch expected size:" + fileSizeInBytes);
      }
    }

    return paths;
  }

  protected long writeFile(String path, long fileSize, long bufferSize, int random) throws IOException {
    RandomAccessFile raf = null;
    long offset = 0;
    try {
      raf = new RandomAccessFile(path, "rw");
      while (offset < fileSize) {
        final long remaining = fileSize - offset;
        final long chunkSize = Math.min(remaining, bufferSize);
        byte[] buffer = new byte[(int)chunkSize];
        for (int i = 0; i < chunkSize; i ++) {
          buffer[i]= (byte) (i % random);
        }
        raf.write(buffer);
        offset += chunkSize;
      }
    } finally {
      if (raf != null) {
        raf.close();
      }
    }
    return offset;
  }

  protected abstract void operation(RaftClient client) throws IOException, ExecutionException, InterruptedException;
}
