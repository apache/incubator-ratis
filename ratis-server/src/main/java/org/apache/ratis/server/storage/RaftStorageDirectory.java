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
package org.apache.ratis.server.storage;

import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.util.AtomicFileOutputStream;
import org.apache.ratis.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.function.CheckedRunnable;
import net.jodah.failsafe.function.CheckedSupplier;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ratis.server.impl.RaftServerConstants.INVALID_LOG_INDEX;
import org.apache.ratis.retry.IORetryPolicy;

public class RaftStorageDirectory {
  static final Logger LOG = LoggerFactory.getLogger(RaftStorageDirectory.class);

  static final String STORAGE_DIR_CURRENT = "current";
  static final String STORAGE_FILE_LOCK = "in_use.lock";
  static final String META_FILE_NAME = "raft-meta";
  static final String LOG_FILE_INPROGRESS = "inprogress";
  static final String LOG_FILE_PREFIX = "log";
  static final String STATE_MACHINE = "sm"; // directory containing state machine snapshots
  static final String TEMP = "tmp";
  static final Pattern CLOSED_SEGMENT_REGEX = Pattern.compile("log_(\\d+)-(\\d+)");
  static final Pattern OPEN_SEGMENT_REGEX = Pattern.compile("log_inprogress_(\\d+)(?:\\..*)?");
  private static final String CONF_EXTENSION = ".conf";


  enum StorageState {
    NON_EXISTENT,
    NOT_FORMATTED,
    NORMAL
  }

  public static class LogPathAndIndex {
    private final Path path;
    public final long startIndex;
    public final long endIndex;

    LogPathAndIndex(Path path, long startIndex, long endIndex) {
      this.path = path;
      this.startIndex = startIndex;
      this.endIndex = endIndex;
    }

    public Path getPath() {
      return path;
    }

    public boolean isOpen() {
      return endIndex == RaftServerConstants.INVALID_LOG_INDEX;
    }

    @Override
    public String toString() {
      return path + "-" + startIndex + "-" + endIndex;
    }
  }

  private final File root; // root directory
  private FileLock lock;   // storage lock

  /**
   * Constructor
   * @param dir directory corresponding to the storage
   */
  RaftStorageDirectory(File dir) {
    this.root = dir;
    this.lock = null;
  }

  /**
   * Get root directory of this storage
   */
  //TODO
  public File getRoot() {
    return root;
  }

  /**
   * Clear and re-create storage directory.
   * <p>
   * Removes contents of the current directory and creates an empty directory.
   *
   * This does not fully format storage directory.
   * It cannot write the version file since it should be written last after
   * all other storage type dependent files are written.
   * Derived storage is responsible for setting specific storage values and
   * writing the version file to disk.
   */
  void clearDirectory() throws IOException {
    clearDirectory(getCurrentDir());
    clearDirectory(getStateMachineDir());
  }

  private static void clearDirectory(File dir) throws IOException {
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{
      if (dir.exists()) {
        LOG.info(dir + " already exists.  Deleting it ...");
        FileUtils.deleteFully(dir);
      }
      FileUtils.createDirectories(dir);
    });
  }

  /**
   * Directory {@code current} contains latest files defining
   * the file system meta-data.
   *
   * @return the directory path
   */
  File getCurrentDir() {
    return new File(root, STORAGE_DIR_CURRENT);
  }

  File getMetaFile() {
    return new File(getCurrentDir(), META_FILE_NAME);
  }

  File getMetaTmpFile() {
    return new File(getCurrentDir(), META_FILE_NAME
        + AtomicFileOutputStream.TMP_EXTENSION);
  }

  File getMetaConfFile() {
    return new File(getCurrentDir(), META_FILE_NAME + CONF_EXTENSION);
  }

  public File getOpenLogFile(long startIndex) {
    return new File(getCurrentDir(), getOpenLogFileName(startIndex));
  }

  static String getOpenLogFileName(long startIndex) {
    return LOG_FILE_PREFIX + "_" + LOG_FILE_INPROGRESS + "_" + startIndex;
  }

  public File getClosedLogFile(long startIndex, long endIndex) {
    return new File(getCurrentDir(), getClosedLogFileName(startIndex, endIndex));
  }

  static String getClosedLogFileName(long startIndex, long endIndex) {
    return LOG_FILE_PREFIX + "_" + startIndex + "-" + endIndex;
  }

  public File getStateMachineDir() {
    return new File(getRoot(), STATE_MACHINE);
  }

  /** Returns a uniquely named temporary directory under $rootdir/tmp/ */
  public File getNewTempDir() {
    return new File(new File(getRoot(), TEMP), UUID.randomUUID().toString());
  }

  public Path relativizeToRoot(Path p) {
    if (p.isAbsolute()) {
      return getRoot().toPath().relativize(p);
    }
    return p;
  }

  /**
   * @return log segment files sorted based on their index.
   */
  public List<LogPathAndIndex> getLogSegmentFiles() throws IOException {
    List<LogPathAndIndex> files = Failsafe.with(IORetryPolicy.retryPolicy).get((CheckedSupplier<List<LogPathAndIndex>>)()->{
      List<LogPathAndIndex> list = new ArrayList<>();
      try (DirectoryStream<Path> stream =
               Files.newDirectoryStream(getCurrentDir().toPath())) {
        for (Path path : stream) {
          for (Pattern pattern : Arrays.asList(CLOSED_SEGMENT_REGEX, OPEN_SEGMENT_REGEX)) {
            Matcher matcher = pattern.matcher(path.getFileName().toString());
            if (matcher.matches()) {
              if (pattern == OPEN_SEGMENT_REGEX && Files.size(path) == 0L) {
                Files.delete(path);
                LOG.info("Delete zero size file " + path);
                break;
              }
              final long startIndex = Long.parseLong(matcher.group(1));
              final long endIndex = matcher.groupCount() == 2 ?
                  Long.parseLong(matcher.group(2)) : INVALID_LOG_INDEX;
              list.add(new LogPathAndIndex(path, startIndex, endIndex));
              break;
            }
          }
        }
      }
      return(list);
    });
    files.sort(Comparator.comparingLong(o -> o.startIndex));
    return files;
  }

  /**
   * Check to see if current/ directory is empty.
   */
  boolean isCurrentEmpty() throws IOException {
    return(Failsafe.with(IORetryPolicy.retryPolicy).get((CheckedSupplier<Boolean>)()->{
      File currentDir = getCurrentDir();
      if(!currentDir.exists()) {
        // if current/ does not exist, it's safe to format it.
        return true;
      }
      try(DirectoryStream<Path> dirStream =
              newDirectoryStream(currentDir.toPath())) {
        if (dirStream.iterator().hasNext()) {
          return false;
        }
      }
      return true;
    }));
  }

  /**
   * Check consistency of the storage directory.
   *
   * @return state {@link StorageState} of the storage directory
   */
  StorageState analyzeStorage(boolean toLock) throws IOException {
    return(Failsafe.with(IORetryPolicy.retryPolicy).get((CheckedSupplier<StorageState>)()->{
      Objects.requireNonNull(root, "root directory is null");

      String rootPath = root.getCanonicalPath();
      try { // check that storage exists
        if (!root.exists()) {
          LOG.info("The storage directory " + rootPath + " does not exist. Creating ...");
          FileUtils.createDirectories(root);
        }
        // or is inaccessible
        if (!root.isDirectory()) {
          LOG.warn(rootPath + " is not a directory");
          return StorageState.NON_EXISTENT;
        }
        if (!Files.isWritable(root.toPath())) {
          LOG.warn("The storage directory " + rootPath + " is not writable.");
          return StorageState.NON_EXISTENT;
        }
      } catch(SecurityException ex) {
        LOG.warn("Cannot access storage directory " + rootPath, ex);
        return StorageState.NON_EXISTENT;
      }

      if (toLock) {
        this.lock(); // lock storage if it exists
      }

      // check whether current directory is valid
      if (hasMetaFile()) {
        return StorageState.NORMAL;
      } else {
        return StorageState.NOT_FORMATTED;
      }
    }));
  }

  public boolean hasMetaFile() {
    return getMetaFile().exists();
  }

  /**
   * Lock storage to provide exclusive access.
   *
   * <p> Locking is not supported by all file systems.
   * E.g., NFS does not consistently support exclusive locks.
   *
   * <p> If locking is supported we guarantee exclusive access to the
   * storage directory. Otherwise, no guarantee is given.
   *
   * @throws IOException if locking fails
   */
  public void lock() throws IOException {
    final FileLock newLock = tryLock();
    if (newLock == null) {
      String msg = "Cannot lock storage " + this.root
          + ". The directory is already locked";
      LOG.info(msg);
      throw new IOException(msg);
    }
    // Don't overwrite lock until success - this way if we accidentally
    // call lock twice, the internal state won't be cleared by the second
    // (failed) lock attempt
    lock = newLock;
  }

  /**
   * Attempts to acquire an exclusive lock on the storage.
   *
   * @return A lock object representing the newly-acquired lock or
   * <code>null</code> if storage is already locked.
   * @throws IOException if locking fails.
   */
  private FileLock tryLock() throws IOException {
    return(Failsafe.with(IORetryPolicy.retryPolicy).get((CheckedSupplier<FileLock>)()->{
      final File lockF = new File(root, STORAGE_FILE_LOCK);
      boolean deletionHookAdded = false;
      if (!lockF.exists()) {
        lockF.deleteOnExit();
        deletionHookAdded = true;
      }
      RandomAccessFile file = new RandomAccessFile(lockF, "rws");
      String jvmName = ManagementFactory.getRuntimeMXBean().getName();
      FileLock res;
      try {
        res = file.getChannel().tryLock();
        if (null == res) {
          LOG.error("Unable to acquire file lock on path " + lockF.toString());
          throw new OverlappingFileLockException();
        }

        file.write(jvmName.getBytes(StandardCharsets.UTF_8));
        LOG.info("Lock on " + lockF + " acquired by nodename " + jvmName);
      } catch (OverlappingFileLockException oe) {
        // Cannot read from the locked file on Windows.
        LOG.error("It appears that another process "
            + "has already locked the storage directory: " + root, oe);
        file.close();
        // XXX swallowing this exception; need to find something to raise up
        throw new IOException("Failed to lock storage " + this.root + ". The directory is already locked", oe);
      } catch(IOException e) {
        LOG.warn("Failed to acquire lock on " + lockF
            + ". If this storage directory is mounted via NFS, "
            + "ensure that the appropriate nfs lock services are running.", e);
        file.close();
        throw e;
      }
      if (!deletionHookAdded) {
        // If the file existed prior to our startup, we didn't
        // call deleteOnExit above. But since we successfully locked
        // the dir, we can take care of cleaning it up.
        lockF.deleteOnExit();
      }
      return res;
    }));
  }

  /**
   * Unlock storage.
   */
  public void unlock() throws IOException {
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{
      if (this.lock == null)
        return;
      this.lock.release();
      lock.channel().close();
      lock = null;
    });
  }

  @Override
  public String toString() {
    return "Storage Directory " + this.root;
  }
}
