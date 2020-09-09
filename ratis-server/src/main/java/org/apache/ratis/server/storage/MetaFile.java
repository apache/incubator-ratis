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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import org.apache.ratis.util.AtomicFileOutputStream;
import org.apache.ratis.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

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
    return this.file.exists();
  }

  public long getTerm() throws IOException {
    if (!loaded) {
      readFile();
      loaded = true;
    }
    return term;
  }

  public String getVotedFor() throws IOException {
    if (!loaded) {
      readFile();
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
  void writeFile(long givenTerm, String votedForInfo) throws IOException {
    AtomicFileOutputStream fos = new AtomicFileOutputStream(file);
    Properties properties = new Properties();
    properties.setProperty(TERM_KEY, Long.toString(givenTerm));
    properties.setProperty(VOTEDFOR_KEY, votedForInfo);
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
  }

  void readFile() throws IOException {
    term = DEFAULT_TERM;
    votedFor = EMPTY_VOTEFOR;
    if (file.exists()) {
      BufferedReader br = new BufferedReader(
          new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
      try {
        Properties properties = new Properties();
        properties.load(br);
        if (properties.containsKey(TERM_KEY) &&
            properties.containsKey(VOTEDFOR_KEY)) {
          term = Long.parseLong((String) properties.get(TERM_KEY));
          votedFor = (String) properties.get(VOTEDFOR_KEY);
        } else {
          throw new IOException("Corrupted term/votedFor properties: "
              + properties);
        }
      } catch(IOException e) {
        LOG.warn("Cannot load term/votedFor properties from {}", file, e);
        throw e;
      } finally {
        IOUtils.cleanup(LOG, br);
      }
    }
  }
}
