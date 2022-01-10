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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.hash.FileIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.utilities.IdentitySplitter;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Benchmark test for loading bloom filters directly from parquet files vs
 * loading bloom filters from metadata table based bloom index.
 */
public class MetaIndexBenchmark implements Serializable {
  private static volatile Logger LOG = LoggerFactory.getLogger(MetaIndexBenchmark.class);

  private final transient HoodieTestSuiteConfig cfg;
  private final transient JavaSparkContext jsc;
  private transient SparkSession sparkSession;
  private transient FileSystem fs;
  private TypedProperties props;

  public MetaIndexBenchmark(HoodieTestSuiteConfig cfg, JavaSparkContext jsc) {
    this.cfg = cfg;
    this.jsc = jsc;
    this.sparkSession = SparkSession.builder().config(jsc.getConf()).enableHiveSupport().getOrCreate();
    if (!cfg.propsFilePath.isEmpty()) {
      this.fs = FSUtils.getFs(cfg.propsFilePath, jsc.hadoopConfiguration());
      this.props = UtilHelpers.readConfig(fs.getConf(), new Path(cfg.propsFilePath), cfg.configs).getProps();
    }
  }

  public static void main(String[] args) {
    final HoodieTestSuiteConfig cfg = new HoodieTestSuiteConfig();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    JavaSparkContext jssc = UtilHelpers.buildSparkContext("meta-index-benchmark", cfg.sparkMaster);
    new MetaIndexBenchmark(cfg, jssc).runBenchmark();
  }

  public void runBenchmark() {
    HoodieTimer timer = new HoodieTimer().startTimer();
    try {
      runTestSuiteImpl();
    } catch (Exception e) {
      throw new HoodieException("Failed to run Test Suite ", e);
    } finally {
      stopQuietly();
    }
    long timeTaken = timer.endTimer();
    LOG.info("Benchmark time taken: " + timeTaken + " ms  / " + (timeTaken / 1000.0)
        + " sec / " + (timeTaken / (1000.0 * 60)) + " min");
  }

  private void runTestSuiteImpl() throws IOException {
    LOG.error("XXX Cfg\nParquetPathList : " + cfg.parquetPathList
        + "\n NumPartitions: " + cfg.numPartitions
        + "\n TableBasePath: " + cfg.basePath
        + "\n Props: " + cfg.propsFilePath
        + "\n: SparkMaster: " + cfg.sparkMaster);
    if (!cfg.parquetPathList.isEmpty()) {
      loadParquetBloomFilters();
    }

    if (!cfg.basePath.isEmpty()) {
      loadMetaIndexBloomFilters();
    }
  }

  private void loadParquetBloomFilters() {
    JavaRDD<String> parquetPathListRDD;
    if (this.cfg.numPartitions > 0) {
      parquetPathListRDD = this.jsc.textFile(cfg.parquetPathList, this.cfg.numPartitions);
    } else {
      parquetPathListRDD = this.jsc.textFile(cfg.parquetPathList);
    }

    parquetPathListRDD.foreach(parquetPath -> {
      readBloomFilter(new Configuration(), parquetPath);
    });
  }

  private void readBloomFilter(Configuration config, final String parquetFilepath) {
    final Pair<String, Long> id = getExecutorAndTaskID();
    ParquetUtils.getInstance(HoodieFileFormat.PARQUET).readBloomFilterFromMetadata(config, new Path(parquetFilepath));
    LOG.info("Executor[" + id.getLeft() + ", " + id.getRight() + "] loaded bloom filter from: " + parquetFilepath);
  }

  private Pair<String, Long> getExecutorAndTaskID() {
    TaskContext tc = TaskContext.get();
    SparkEnv sparkEnv = SparkEnv.get();
    return Pair.of((sparkEnv != null ? sparkEnv.executorId() : "NA"),
        (tc != null ? tc.taskAttemptId() : 0));
  }

  private void loadMetaIndexBloomFilters() throws IOException {
    final int maxKeysForBloomIndexLookup = 2048;
    LOG.info("Opening hudi table: " + cfg.basePath + " with props: " + this.props);
    HoodieMetadataConfig config = HoodieMetadataConfig.newBuilder()
        .fromProperties(this.props)
        .build();
    HoodieBackedTableMetadata metadata = new HoodieBackedTableMetadata(
        new HoodieLocalEngineContext(new Configuration()),
        config, cfg.basePath, "/tmp");
    LOG.info("All partitions: " + metadata.getAllPartitionPaths());

    List<Pair<PartitionIndexID, FileIndexID>> partitionFileKeyList = new ArrayList<>();
    List<Pair<String, String>> partitionFileList = new ArrayList<>();
    metadata.getAllPartitionPaths().forEach(partition -> {
      try {
        Arrays.stream(metadata.getAllFilesInPartition(new Path(cfg.basePath, partition))).forEach(fileStatus -> {
          if (partitionFileKeyList.size() < maxKeysForBloomIndexLookup) {
            partitionFileList.add(Pair.of(partition, FSUtils.getFileId(fileStatus.getPath().getName())));
            partitionFileKeyList.add(Pair.of(new PartitionIndexID(partition),
                new FileIndexID(FSUtils.getFileId(fileStatus.getPath().getName()))));
          }
        });
      } catch (IOException e) {
        LOG.error("Error loading meta index bloom filters: ", e);
      }
    });

    LOG.info("All partitions and files: " + partitionFileList.subList(0, Math.min(partitionFileList.size(), 32)));
    HoodieTimer timer = new HoodieTimer().startTimer();
    Map<String, ByteBuffer> fileIDToBloomFilterByteBufferMap = metadata.getBloomFilters(partitionFileKeyList);
    LOG.info("Loaded bloom filters: " + fileIDToBloomFilterByteBufferMap.size());

    long timeTaken = timer.endTimer();
    LOG.info("Bloom filter loading time taken: " + timeTaken + " ms  / " + (timeTaken / 1000.0)
        + " sec / " + (timeTaken / (1000.0 * 60)) + " min");
  }

  private void stopQuietly() {
    try {
      jsc.stop();
    } catch (Exception e) {
      LOG.error("Unable to stop spark session", e);
    }
  }

  public static class HoodieTestSuiteConfig {
    @Parameter(names = {"--parquet-path-list"}, description = "Parquet path list")
    public String parquetPathList = "";

    @Parameter(names = {"--base-path"}, description = "Hudi table base path")
    public String basePath = "";

    @Parameter(names = {"--num-partitions"},
        description = "Total number of partitions to split the input parquet path list", required = false)
    public Integer numPartitions = 0;

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
