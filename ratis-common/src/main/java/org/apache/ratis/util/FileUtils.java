/*
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
package org.apache.ratis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.function.CheckedRunnable;

import org.apache.ratis.retry.IORetryPolicy;

public interface FileUtils {
  Logger LOG = LoggerFactory.getLogger(FileUtils.class);

  static void truncateFile(File f, long target) throws IOException {
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{
      final long original = f.length();
      LogUtils.runAndLog(LOG,
          () -> {
            try (FileOutputStream out = new FileOutputStream(f, true)) {
              out.getChannel().truncate(target);
            }},
          () -> "FileOutputStream.getChannel().truncate " + f + " length: " + original + " -> " + target);
    });
  }

  static OutputStream createNewFile(Path p) throws IOException {
    return LogUtils.supplyAndLog(LOG,
        () -> Files.newOutputStream(p, StandardOpenOption.CREATE_NEW),
        () -> "Files.newOutputStream " + StandardOpenOption.CREATE_NEW + " " + p);
  }

  static void createDirectories(File dir) throws IOException {
    createDirectories(dir.toPath());
  }

  static void createDirectories(Path dir) throws IOException {
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{
      LogUtils.runAndLog(LOG,
          () -> Files.createDirectories(dir),
          () -> "Files.createDirectories " + dir);
    });
  }

  static void move(File src, File dst) throws IOException {
    move(src.toPath(), dst.toPath());
  }

  static void move(Path src, Path dst) throws IOException {
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{
      LogUtils.runAndLog(LOG,
          () -> Files.move(src, dst),
          () -> "Files.move " + src + " to " + dst);
    });
  }

  /** The same as passing f.toPath() to {@link #delete(Path)}. */
  static void deleteFile(File f) throws IOException {
    delete(f.toPath());
  }

  /** The same as passing f.toPath() to {@link #delete(Path)}. */
  static void deleteFileQuietly(File f) {
    try {
      delete(f.toPath());
    } catch (Exception ex) {
      LOG.debug("File delete was not susccesful {}", f.getAbsoluteFile(), ex);
    }
  }

  /**
   * Use {@link Files#delete(Path)} to delete the given path.
   *
   * This method may print log messages using {@link #LOG}.
   */
  static void delete(Path p) throws IOException {
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{
      LogUtils.runAndLog(LOG,
          () -> Files.delete(p),
          () -> "Files.delete " + p);
    });
  }

  /** The same as passing f.toPath() to {@link #deleteFully(Path)}. */
  static void deleteFully(File f) throws IOException {
    LOG.trace("deleteFully {}", f);
    deleteFully(f.toPath());
  }

  /**
   * Delete fully the given path.
   *
   * (1) If it is a file, the file will be deleted.
   *
   * (2) If it is a directory, the directory and all its contents will be recursively deleted.
   *     If an exception is thrown, the directory may possibly be partially deleted.*
   *
   * (3) If it is a symlink, the symlink will be deleted but the symlink target will not be deleted.
   */
  static void deleteFully(Path p) throws IOException {
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{
      if (!Files.exists(p, LinkOption.NOFOLLOW_LINKS)) {
        LOG.trace("deleteFully: {} does not exist.", p);
        return;
      }
      Files.walkFileTree(p, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
          if (e != null) {
            // directory iteration failed
            throw e;
          }
          delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    });
  }
}
