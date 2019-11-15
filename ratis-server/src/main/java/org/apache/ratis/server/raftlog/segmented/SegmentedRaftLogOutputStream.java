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
package org.apache.ratis.server.raftlog.segmented;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.PureJavaCrc32C;
import org.apache.ratis.util.function.CheckedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.Checksum;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeException;
import net.jodah.failsafe.function.CheckedRunnable;

import org.apache.ratis.retry.IORetryPolicy;

public class SegmentedRaftLogOutputStream implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(SegmentedRaftLogOutputStream.class);

  private static final ByteBuffer fill;
  private static final int BUFFER_SIZE = 1024 * 1024; // 1 MB
  static {
    fill = ByteBuffer.allocateDirect(BUFFER_SIZE);
    fill.position(0);
    for (int i = 0; i < fill.capacity(); i++) {
      fill.put(SegmentedRaftLogFormat.getTerminator());
    }
  }

  private File file;
  private FileChannel fc; // channel of the file stream for sync
  private BufferedWriteChannel out; // buffered FileChannel for writing
  private final Checksum checksum;

  private final long segmentMaxSize;
  private final long preallocatedSize;
  private long preallocatedPos;

  public SegmentedRaftLogOutputStream(File file, boolean append, long segmentMaxSize,
      long preallocatedSize, int bufferSize)
      throws IOException {
    this.file = file;
    this.checksum = new PureJavaCrc32C();
    this.segmentMaxSize = segmentMaxSize;
    this.preallocatedSize = preallocatedSize;
    RandomAccessFile rp = new RandomAccessFile(file, "rw");
    try {
      Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{
        fc = rp.getChannel();
        fc.position(fc.size());
        preallocatedPos = fc.size();

        out = new BufferedWriteChannel(fc, bufferSize);
        if (!append) {
          create();
        }
      });
    } catch (FailsafeException ioe) {
      LOG.warn("Hit IOException while creating log segment " + file
          + ", delete the partial file.");
      // hit IOException, clean up the in-progress log file
      try {
        Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{
          FileUtils.deleteFully(file);
        });
      } catch (FailsafeException e) {
        LOG.warn("Failed to delete the file " + file, e);
      }
      throw new IOException(ioe);
    }
  }

  /**
   * Write the given entry to this output stream.
   *
   * Format:
   *   (1) The serialized size of the entry.
   *   (2) The entry.
   *   (3) 4-byte checksum of the entry.
   *
   * Size in bytes to be written:
   *   (size to encode n) + n + (checksum size),
   *   where n is the entry serialized size and the checksum size is 4.
   */
  public void write(LogEntryProto entry) throws IOException {
    final int serialized = entry.getSerializedSize();
    final int bufferSize = CodedOutputStream.computeUInt32SizeNoTag(serialized)
        + serialized;

    preallocateIfNecessary(bufferSize + 4);

    byte[] buf = new byte[bufferSize];
    CodedOutputStream cout = CodedOutputStream.newInstance(buf);
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{cout.writeUInt32NoTag(serialized);});
    ///XXX Protobuf writeTo() may not be idempotent!
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{
      entry.writeTo(cout);
    });

    checksum.reset();
    checksum.update(buf, 0, buf.length);
    final int sum = (int) checksum.getValue();

    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{out.write(buf);});
    writeInt(sum);
  }

  private void writeInt(int v) throws IOException {
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{out.write((v >>> 24) & 0xFF);});
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{out.write((v >>> 16) & 0xFF);});
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{out.write((v >>>  8) & 0xFF);});
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{out.write((v) & 0xFF);});
  }

  private void create() throws IOException {
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{fc.truncate(0);});
    fc.position(0);
    preallocatedPos = 0;
    preallocate(); // preallocate file

    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{
      SegmentedRaftLogFormat.applyHeaderTo(CheckedConsumer.asCheckedFunction(out::write));
    });
    flush();
  }

  @Override
  public void close() throws IOException {
    try {
      Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)(out::flush));
      if (fc != null && fc.isOpen()) {
        Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{
          fc.truncate(fc.position());
        });
      }
    } finally {
      IOUtils.cleanup(LOG, fc, out);
      fc = null;
      out = null;
    }
  }

  /**
   * Flush data to persistent store.
   * Collect sync metrics.
   */
  public void flush() throws IOException {
    if (out == null) {
      throw new IOException("Trying to use aborted output stream");
    }
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)(out::flush));
  }

  private void preallocate() throws IOException {
    fill.position(0);
    long targetSize = Math.min(segmentMaxSize - fc.size(), preallocatedSize);
    int allocated = 0;
    while (allocated < targetSize) {
      int size = (int) Math.min(BUFFER_SIZE, targetSize - allocated);
      ByteBuffer buffer = fill.slice();
      buffer.limit(size);
      Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{
        IOUtils.writeFully(fc, buffer, preallocatedPos);
      });
      preallocatedPos += size;
      allocated += size;
    }
    LOG.debug("Pre-allocated {} bytes for the log segment", allocated);
  }

  private void preallocateIfNecessary(int size) throws IOException {
    if (out.position() + size > preallocatedPos) {
      preallocate();
    }
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "(" + file + ")";
  }
}
