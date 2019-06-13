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
package org.apache.ratis.logservice.impl;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ratis.logservice.api.ArchiveLogReader;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Archive Log Reader implementation. This class is not thread-safe
 *
 */

public class ArchiveHdfsLogReader implements ArchiveLogReader {
  public static final Logger LOG = LoggerFactory.getLogger(ArchiveHdfsLogReader.class);
  private long fileLength;
  private List<FileStatus> files;
  private FileSystem hdfs;
  private FSDataInputStream is;
  private ByteBuffer currentRecord;
  private int fileCounter=0;
  private int currentRecordId = -1;

  public ArchiveHdfsLogReader(String archiveLocation) throws IOException {
    Configuration configuration = new Configuration();
    this.hdfs = FileSystem.get(configuration);
    Path archiveLocationPath=new Path(archiveLocation);
    if (!hdfs.exists(archiveLocationPath)) {
      throw new FileNotFoundException(archiveLocation);
    }
    files = Arrays.asList(hdfs.listStatus(archiveLocationPath));
    Collections.sort(files, new Comparator<FileStatus>() {
      @Override public int compare(FileStatus o1, FileStatus o2) {
        //ascending order
        return o1.getPath().getName().compareTo(o2.getPath().getName());
      }
    });
    openNextFilePath();
    loadNext();
  }

  public Path openNextFilePath() throws IOException {
    Path filePath = files.get(fileCounter).getPath();
    this.is = this.hdfs.open(filePath);
    this.fileLength = this.hdfs.getFileStatus(filePath).getLen();
    fileCounter++;
    return filePath;

  }

  @Override public void seek(long recordId) throws IOException {
    while (currentRecordId < recordId && hasNext()) {
      next();
    }
  }

  @Override public boolean hasNext() throws IOException {
    return currentRecord != null;
  }

  @Override public byte[] next() throws IOException {
    return readNext().array();
  }

  @Override public long getCurrentRaftIndex() {
    return 0;
  }

  @Override public ByteBuffer readNext() throws IOException {
    if (currentRecord == null) {
      throw new NoSuchElementException();
    }
    ByteBuffer current = currentRecord;
    currentRecord = null;
    loadNext();
    return current;
  }

  private int readLength() throws IOException {
    int length;
    try {
      length = is.readInt();
    }catch (EOFException e){
      if (files.size()==fileCounter) {
        throw e;
      }else {
        openNextFilePath();
        length = is.readInt();
      }
    }
    return length;
  }

  @Override public void readNext(ByteBuffer buffer) throws IOException {
    Preconditions.checkNotNull(buffer, "buffer is NULL");
    buffer.put(readNext().array());
  }

  @Override public List<ByteBuffer> readBulk(int numRecords) throws IOException {
    Preconditions.checkArgument(numRecords > 0, "number of records must be greater than 0");
    List<ByteBuffer> ret = new ArrayList<ByteBuffer>();
    try {

      for (int i = 0; i < numRecords; i++) {
        ByteBuffer buffer = readNext();
        ret.add(buffer);
      }

    } catch (EOFException eof) {
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      return ret;
    }
  }

  @Override public int readBulk(ByteBuffer[] buffers) throws IOException {
    Preconditions.checkNotNull(buffers, "list of buffers is NULL");
    Preconditions.checkArgument(buffers.length > 0, "list of buffers is empty");
    int count = 0;
    try {
      for (int i = 0; i < buffers.length; i++) {
        buffers[i] = readNext();
        count++;
      }
    } catch (EOFException eof) {

    }
    return count;
  }

  @Override public long getPosition() throws IOException {
    return currentRecordId;
  }

  @Override public void close() throws IOException {
    if (this.is != null) {
      this.is.close();
      this.is = null;
    }
  }

  private void loadNext() throws IOException {
    try {
      ByteBuffer buff = ByteBuffer.allocate(readLength());
      is.read(buff.array());
      currentRecordId++;
      currentRecord = buff;
    } catch (EOFException e) {
      currentRecord = null;
      return;
    }
  }

}
