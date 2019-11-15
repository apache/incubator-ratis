/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.ratis.server.storage;

import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.AtomicFileOutputStream;
import org.apache.ratis.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.function.CheckedRunnable;
import net.jodah.failsafe.function.CheckedSupplier;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Vector;

import org.apache.ratis.retry.IORetryPolicy;

/**
 * Class that represents a file on disk which persistently stores
 * a single <code>long</code> value. The file is updated atomically
 * and durably (i.e fsynced).
 */
public class MetaFile {
  private static final Logger LOG = LoggerFactory.getLogger(MetaFile.class);
  private static final String TERM_KEY = "term";
  private static final String VOTEDFOR_KEY = "votedFor";
  static final long DEFAULT_TERM = 0;
  static final String EMPTY_VOTEFOR = "";

  private final File file;
  private boolean loaded = false;
  private long term;
  private String votedFor;

  MetaFile(File file) {
    this.file = file;
    term = DEFAULT_TERM;
    votedFor = EMPTY_VOTEFOR;
  }

  boolean exists() {
    return(Failsafe.with(IORetryPolicy.retryPolicy).get((CheckedSupplier<Boolean>)()->{
      return this.file.exists();
    }));
  }

  public long getTerm() throws IOException {
    if (!loaded) {
      try {
        readFile();
      } catch (IllegalStateException e) {
        throw new IOException(e);
      }
      loaded = true;
    }
    return term;
  }

  public String getVotedFor() throws IOException {
    if (!loaded) {
      try {
        readFile();
      } catch (IllegalStateException e) {
        throw new IOException(e);
      }
      loaded = true;
    }
    return votedFor;
  }

  public void set(long newTerm, String newVotedFor) throws IOException {
    newVotedFor = newVotedFor == null ? EMPTY_VOTEFOR : newVotedFor;
    if (!loaded || (newTerm != term || !newVotedFor.equals(votedFor))) {
      writeFile(newTerm, newVotedFor);
    }
    term = newTerm;
    votedFor = newVotedFor;
    loaded = true;
  }

  /**
   * Atomically write the given term and votedFor information to the given file,
   * including fsyncing.
   *
   * @throws IOException if the file cannot be written
   */
  void writeFile(long term, String votedFor) throws IOException {
    Failsafe.with(IORetryPolicy.retryPolicy).run((CheckedRunnable)()->{
      AtomicFileOutputStream fos = new AtomicFileOutputStream(file);
      Properties properties = new Properties();
      properties.setProperty(TERM_KEY, Long.toString(term));
      properties.setProperty(VOTEDFOR_KEY, votedFor);
      try {
        properties.store(
            new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8)), "");
        fos.close();
        fos = null;
      } finally {
        if (fos != null) {
          fos.abort();
        }
      }
    });
  }

  /*
   * Will set class fields @term and @votedFor if a successful read of @file
   * @throws @IOException if @file fails to read in @retryReadPolicy
   * @throws @IllegalStateException if @file is malformed
   */
  void readFile() throws IllegalStateException {
    term = DEFAULT_TERM;
    votedFor = EMPTY_VOTEFOR;
    Vector<Object> result = Failsafe.with(IORetryPolicy.retryPolicy).get((CheckedSupplier<Vector<Object>>)()->{
      Vector<Object> v = new Vector<Object>(2);
      if (file.exists()) {
        BufferedReader br = new BufferedReader(
            new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
        try {
          Properties properties = new Properties();
          properties.load(br);
          if (properties.containsKey(TERM_KEY) &&
              properties.containsKey(VOTEDFOR_KEY)) {
            // terrible hack to return Vector<Object> v:[Term, VotedFor]
            v.add(Long.parseLong((String) properties.get(TERM_KEY)));
            v.add((String) properties.get(VOTEDFOR_KEY));
          } else {
            throw new IllegalStateException("Corrupted term/votedFor properties: "
                + properties);
          }
        } finally {
          IOUtils.cleanup(LOG, br);
        }
      }
      return(v);
    });
    if (result.size() == 2) {
      term = (Long)result.get(0);
      votedFor = (String)result.get(1);
    } else {
      throw new IllegalStateException("Corrupted term/votedFor vector");
    }
  }
}
