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
package org.apache.ratis.server.storage;

import java.nio.file.Path;

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.retry.IORetryPolicy;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.function.CheckedSupplier;

/**
 * Metadata about a file.
 *
 * The objects of this class are immutable.
 */
public class FileInfo {
  private final Path path;
  private final MD5Hash fileDigest;
  private final long fileSize;

  public FileInfo(Path path, MD5Hash fileDigest) {
    this.path = path;
    this.fileDigest = fileDigest;
    this.fileSize = Failsafe.with(IORetryPolicy.retryPolicy).get((
        CheckedSupplier<Long>)()->{return(path.toFile().length());});
  }

  @Override
  public String toString() {
    return path.toString();
  }

  /** @return the path of the file. */
  public Path getPath() {
    return path;
  }

  /** @return the MD5 file digest of the file. */
  public MD5Hash getFileDigest() {
    return fileDigest;
  }

  /** @return the size of the file. */
  public long getFileSize() {
    return fileSize;
  }
}
