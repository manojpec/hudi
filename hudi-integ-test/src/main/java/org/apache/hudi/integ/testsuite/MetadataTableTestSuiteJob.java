/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.integ.testsuite;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.IdentitySplitter;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Test for reading records directly from HBase HFile.
 */
public class MetadataTableTestSuiteJob {
  private static volatile Logger LOG = LoggerFactory.getLogger(MetadataTableTestSuiteJob.class);

  private final transient JavaSparkContext jsc;
  private final transient SparkSession sparkSession;
  private final transient FileSystem fs;
  private final HoodieTestSuiteConfig cfg;
  private final Random random;
  TypedProperties props;

  public MetadataTableTestSuiteJob(HoodieTestSuiteConfig cfg, JavaSparkContext jsc) throws IOException {
    this.cfg = cfg;
    this.jsc = jsc;
    this.sparkSession = SparkSession.builder().config(jsc.getConf()).enableHiveSupport().getOrCreate();
    this.fs = FSUtils.getFs(cfg.hfilePath, jsc.hadoopConfiguration());
    this.props = UtilHelpers.readConfig(fs.getConf(), new Path(cfg.propsFilePath), cfg.configs).getProps();
    this.random = new Random();
    this.random.setSeed(System.currentTimeMillis());
  }

  public static void main(String[] args) throws Exception {
    final HoodieTestSuiteConfig cfg = new HoodieTestSuiteConfig();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    JavaSparkContext jssc = UtilHelpers.buildSparkContext("metadata-hfile-reader", cfg.sparkMaster);
    new MetadataTableTestSuiteJob(cfg, jssc).runTestSuite();
  }

  public void runTestSuite() {
    try {
      runTestSuiteImpl();
    } catch (Exception e) {
      throw new HoodieException("Failed to run Test Suite ", e);
    } finally {
      stopQuietly();
    }
  }

  private void runTestSuiteImpl() throws InterruptedException, IOException {
    LOG.error("XXX Cfg\nHFile: " + cfg.hfilePath
        + "\n Props: " + cfg.propsFilePath
        + "\n: SparkMaster: " + cfg.sparkMaster);
    scan();
  }

  private void scan() throws InterruptedException, IOException {
    LOG.info("Opening HFile reader for " + cfg.hfilePath);
    final Path hfilePath = new Path(cfg.hfilePath);
    final CacheConfig cacheConfig = new CacheConfig(jsc.hadoopConfiguration());

    HFile.Reader reader = HFile.createReader(
        FSUtils.getFs(hfilePath.toString(), jsc.hadoopConfiguration()),
        hfilePath, cacheConfig, jsc.hadoopConfiguration());
    HFileScanner scanner = null;

    LOG.error("XXX Cfg scanAll: " + cfg.scanAll
        + ", scanRandom: " + cfg.scanRandomKeys
        + ", scanCount: " + cfg.scanCount
        + ", reuseScanner: " + cfg.enableReuseScanner
        + ", cache: " + cfg.enableCacheBlocks
        + ", pread: " + cfg.enablePread);

    int i = 0;
    boolean scannerAvailable = false;
    while (i++ < cfg.scanCount) {
      if (cfg.scanAll) {
        LOG.info("Scanning all keys");
        if (!scannerAvailable || !cfg.enableReuseScanner) {
          LOG.info("Opening HFile scanner");
          scanner = reader.getScanner(cfg.enableCacheBlocks, cfg.enablePread);
          scannerAvailable = true;
        }
        scanAll(scanner);
      }

      if (cfg.scanRandomKeys) {
        if (!scannerAvailable || !cfg.enableReuseScanner) {
          LOG.info("Opening HFile scanner");
          scanner = reader.getScanner(cfg.enableCacheBlocks, cfg.enablePread);
          scannerAvailable = true;
        }
        scanRandom(scanner);
      }

      if (cfg.readDelayBetweenKeysRead > 0) {
        LOG.info("Scan delay " + cfg.readDelayBetweenKeysRead + "ms");
        Thread.sleep(cfg.readDelayBetweenKeysRead);
      }
    }
  }

  private void scanAll(HFileScanner scanner) throws IOException {
    if (scanner.seekTo()) {
      do {
        Cell c = scanner.getKeyValue();
        byte[] keyBytes = Arrays.copyOfRange(c.getRowArray(), c.getRowOffset(), c.getRowOffset() + c.getRowLength());
        LOG.info("Read key: " + new String(keyBytes));
      } while (scanner.next());
    } else {
      LOG.error("Failed to seekTo()");
      return;
    }
  }

  private void scanRandom(HFileScanner scanner) throws IOException {
    final String key = getRandomKey();
    KeyValue kv = new KeyValue(key.getBytes(), null, null, null);
    if (scanner.seekTo(kv) == 0) {
      Cell c = scanner.getKeyValue();
      Arrays.copyOfRange(c.getValueArray(), c.getValueOffset(), c.getValueOffset() + c.getValueLength());
      LOG.info("Read random key: " + key);
    } else {
      LOG.error("Failed to seekTo(vk): " + key);
    }
  }

  private String getRandomKey() {
    final int choice = random.nextInt(10);

    // 10%
    if (cfg.useNonExistingKeys && choice == 0) {
      return UUID.randomUUID().toString();
    }

    // 20%
    if (choice < 2) {
      return "__all_partitions__";
    }

    // rest all
    final int month = random.nextInt(2) + 1;
    StringBuilder key = new StringBuilder("1970/0");
    key.append(month);
    key.append("/");

    final int suffix = (month == 1 ? (random.nextInt(31) + 1) : (random.nextInt(19) + 1));
    key.append(String.format("%02d", suffix));
    return key.toString();
  }

  private void stopQuietly() {
    try {
      jsc.stop();
    } catch (Exception e) {
      LOG.error("Unable to stop spark session", e);
    }
  }

  public static class HoodieTestSuiteConfig {
    @Parameter(names = {"--hfile-path"}, description = "HFile path", required = true)
    public String hfilePath = "s3a://dl-scale-test/manoj/010RC2/integration-test-large-scale/sanity-10rounds/mor/output/"
        + ".hoodie/metadata/files/files-0000_0-644-4247_20211201022415024001.hfile";

    @Parameter(names = {"--scan-all"}, description = "Scan all keys")
    public Boolean scanAll = false;

    @Parameter(names = {"--scan-random-keys"}, description = "Seek to random keys")
    public Boolean scanRandomKeys = false;

    @Parameter(names = {"--scan-keys-count"}, description = "Total number of random keys read")
    public Integer scanCount = 1;

    @Parameter(names = {"--delay-between-keys-read"}, description = "Delay between read in millis")
    public Integer readDelayBetweenKeysRead = 0;

    @Parameter(names = {"--enable-reuse-scanner"}, description = "Reuse the existing scanner for all iterations")
    public Boolean enableReuseScanner = false;

    @Parameter(names = {"--enable-cache-blocks"}, description = "Reuse the existing scanner for all iterations")
    public Boolean enableCacheBlocks = false;

    @Parameter(names = {"--enable-pread"}, description = "Reuse the existing scanner for all iterations")
    public Boolean enablePread = false;

    @Parameter(names = {"--use-non-existing-keys"}, description = "Reuse the existing scanner for all iterations")
    public Boolean useNonExistingKeys = false;

    @Parameter(names = {"--props"}, description = "path to properties file")
    public String propsFilePath = "";

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--spark-master"}, description = "spark master to use.")
    public String sparkMaster = "local[2]";

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }
}
