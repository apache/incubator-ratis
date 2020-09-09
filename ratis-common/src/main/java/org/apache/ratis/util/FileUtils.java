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

import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.function.Supplier;

public interface FileUtils {
  Logger LOG = LoggerFactory.getLogger(FileUtils.class);

  int NUM_ATTEMPTS = 5;
  TimeDuration SLEEP_TIME = TimeDuration.ONE_SECOND;

  static <T> T attempt(CheckedSupplier<T, IOException> op, Supplier<?> name) throws IOException {
    try {
      return JavaUtils.attempt(op, NUM_ATTEMPTS, SLEEP_TIME, name, LOG);
    } catch (InterruptedException e) {
      throw IOUtils.toInterruptedIOException("Interrupted " + name.get(), e);
    }
  }

  static void truncateFile(File f, long target) throws IOException {
    final long original = f.length();
    LogUtils.runAndLog(LOG,
        () -> {
          try (FileOutputStream out = new FileOutputStream(f, true)) {
            out.getChannel().truncate(target);
          }
        },
        () -> "FileOutputStream.getChannel().truncate " + f + " length: " + original + " -> " + target);
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
    LogUtils.runAndLog(LOG,
        () -> Files.createDirectories(dir),
        () -> "Files.createDirectories " + dir);
  }

  static void move(File src, File dst) throws IOException {
    move(src.toPath(), dst.toPath());
  }

  static void move(Path src, Path dst) throws IOException {
    LogUtils.runAndLog(LOG,
        () -> Files.move(src, dst),
        () -> "Files.move " + src + " to " + dst);
  }

  /** The same as passing f.toPath() to {@link #delete(Path)}. */
  static void deleteFile(File f) throws IOException {
    delete(f.toPath());
  }

  /**
   * Moves the directory. If any file is locked, the exception is caught
   * and logged and continues to other files.
   * @param source
   * @param dest
   * @throws IOException
   */
  static void moveDirectory(Path source, Path dest) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("moveDirectory source: {} dest: {}", source, dest);
    }
    createDirectories(dest);
    Files.walkFileTree(source, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir,
          BasicFileAttributes attrs) throws IOException {
        Path targetPath = dest.resolve(source.relativize(dir));
        if (!Files.exists(targetPath)) {
          createDirectories(targetPath);
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
        try {
          move(file, dest.resolve(source.relativize(file)));
        } catch (IOException e) {
          LOG.info("Files.moveDirectory: could not delete {}",
              file.getFileName());
        }
        return FileVisitResult.CONTINUE;
      }
    });

    /* Delete the source since all the files has been moved. */
    deleteFully(source);
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
    LogUtils.runAndLog(LOG,
        () -> Files.delete(p),
        () -> "Files.delete " + p);
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
  }

  // Rename a file by appending .corrupt to file name. This function does not guarantee
  // that the rename operation is successful.
  static void renameFileToCorrupt(File tmpSnapshotFile) {
    File corruptedTempFile = new File(tmpSnapshotFile.getPath() + ".corrupt");
    tmpSnapshotFile.renameTo(corruptedTempFile);
  }
}
