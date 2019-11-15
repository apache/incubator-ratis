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
package org.apache.ratis.statemachine.impl;

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.util.AtomicFileOutputStream;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.function.CheckedRunnable;
import net.jodah.failsafe.function.CheckedSupplier;

import org.apache.ratis.retry.IORetryPolicy;

/**
 * A StateMachineStorage that stores the snapshot in a single file.
 */
public class SimpleStateMachineStorage implements StateMachineStorage {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleStateMachineStorage.class);

  static final String SNAPSHOT_FILE_PREFIX = "snapshot";
  static final String CORRUPT_SNAPSHOT_FILE_SUFFIX = ".corrupt";
  /** snapshot.term_index */
  public static final Pattern SNAPSHOT_REGEX =
      Pattern.compile(SNAPSHOT_FILE_PREFIX + "\\.(\\d+)_(\\d+)");

  private RaftStorage raftStorage;
  private File smDir = null;

  private volatile SingleFileSnapshotInfo currentSnapshot = null;

  @Override
  public void init(RaftStorage raftStorage) throws IOException {
    this.raftStorage = raftStorage;
    this.smDir = raftStorage.getStorageDir().getStateMachineDir();
    loadLatestSnapshot();
  }

  @Override
  public void format() throws IOException {
    // TODO
  }

  @Override
  public void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy) throws IOException {
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{
      if (snapshotRetentionPolicy != null && snapshotRetentionPolicy.getNumSnapshotsRetained() > 0) {

        List<SingleFileSnapshotInfo> allSnapshotFiles = new ArrayList<>();
        try (DirectoryStream<Path> stream =
                 Files.newDirectoryStream(smDir.toPath())) {
          for (Path path : stream) {
            Matcher matcher = SNAPSHOT_REGEX.matcher(path.getFileName().toString());
            if (matcher.matches()) {
              final long endIndex = Long.parseLong(matcher.group(2));
              final long term = Long.parseLong(matcher.group(1));
              final FileInfo fileInfo = new FileInfo(path, null); //We don't need FileDigest here.
              allSnapshotFiles.add(new SingleFileSnapshotInfo(fileInfo, term, endIndex));
            }
          }
        }

        if (allSnapshotFiles.size() > snapshotRetentionPolicy.getNumSnapshotsRetained()) {
          allSnapshotFiles.sort(new SnapshotFileComparator());
          List<File> snapshotFilesToBeCleaned = allSnapshotFiles.subList(
              snapshotRetentionPolicy.getNumSnapshotsRetained(), allSnapshotFiles.size()).stream()
              .map(singleFileSnapshotInfo -> singleFileSnapshotInfo.getFile().getPath().toFile())
              .collect(Collectors.toList());
          for (File snapshotFile : snapshotFilesToBeCleaned) {
            LOG.info("Deleting old snapshot at {}", snapshotFile.getAbsolutePath());
            FileUtils.deleteFileQuietly(snapshotFile);
          }
        }
      }
    });
  }

  public static TermIndex getTermIndexFromSnapshotFile(File file) {
    final String name = file.getName();
    final Matcher m = SNAPSHOT_REGEX.matcher(name);
    if (!m.matches()) {
      throw new IllegalArgumentException("File \"" + file
          + "\" does not match snapshot file name pattern \""
          + SNAPSHOT_REGEX + "\"");
    }
    final long term = Long.parseLong(m.group(1));
    final long index = Long.parseLong(m.group(2));
    return TermIndex.newTermIndex(term, index);
  }

  protected static String getTmpSnapshotFileName(long term, long endIndex) {
    return getSnapshotFileName(term, endIndex) + AtomicFileOutputStream.TMP_EXTENSION;
  }

  protected static String getCorruptSnapshotFileName(long term, long endIndex) {
    return getSnapshotFileName(term, endIndex) + CORRUPT_SNAPSHOT_FILE_SUFFIX;
  }

  public File getSnapshotFile(long term, long endIndex) {
    return new File(smDir, getSnapshotFileName(term, endIndex));
  }

  protected File getTmpSnapshotFile(long term, long endIndex) {
    return new File(smDir, getTmpSnapshotFileName(term, endIndex));
  }

  protected File getCorruptSnapshotFile(long term, long endIndex) {
    return new File(smDir, getCorruptSnapshotFileName(term, endIndex));
  }

  public SingleFileSnapshotInfo findLatestSnapshot() throws IOException {
    return((Failsafe.with(IORetryPolicy.retryPolicy).get((CheckedSupplier<SingleFileSnapshotInfo>)()->{
      SingleFileSnapshotInfo latest = null;
      try (DirectoryStream<Path> stream =
               Files.newDirectoryStream(smDir.toPath())) {
        for (Path path : stream) {
          Matcher matcher = SNAPSHOT_REGEX.matcher(path.getFileName().toString());
          if (matcher.matches()) {
            final long endIndex = Long.parseLong(matcher.group(2));
            if (latest == null || endIndex > latest.getIndex()) {
              final long term = Long.parseLong(matcher.group(1));
              MD5Hash fileDigest = MD5FileUtil.readStoredMd5ForFile(path.toFile());
              final FileInfo fileInfo = new FileInfo(path, fileDigest);
              latest = new SingleFileSnapshotInfo(fileInfo, term, endIndex);
            }
          }
        }
      }
      return(latest);
    })));
  }

  public void loadLatestSnapshot() throws IOException {
    this.currentSnapshot = findLatestSnapshot();
  }

  public static String getSnapshotFileName(long term, long endIndex) {
    return SNAPSHOT_FILE_PREFIX + "." + term + "_" + endIndex;
  }

  @Override
  public SingleFileSnapshotInfo getLatestSnapshot() {
    return currentSnapshot;
  }

  @VisibleForTesting
  public File getSmDir() {
    return smDir;
  }
}

/**
 * Compare snapshot files based on transaction indexes.
 */
class SnapshotFileComparator implements Comparator<SingleFileSnapshotInfo> {
  @Override
  public int compare(SingleFileSnapshotInfo file1, SingleFileSnapshotInfo file2) {
    return (int) (file2.getIndex() - file1.getIndex());
  }
}
