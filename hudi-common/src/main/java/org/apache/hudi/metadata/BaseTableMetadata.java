
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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieColumnStats;
import org.apache.hudi.avro.model.HoodieMetadataBloomFilter;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.hash.FileID;
import org.apache.hudi.exception.HoodieMetadataException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BaseTableMetadata implements HoodieTableMetadata {

  private static final Logger LOG = LogManager.getLogger(BaseTableMetadata.class);

  static final long MAX_MEMORY_SIZE_IN_BYTES = 1024 * 1024 * 1024;
  static final int BUFFER_SIZE = 10 * 1024 * 1024;

  protected final transient HoodieEngineContext engineContext;
  protected final SerializableConfiguration hadoopConf;
  protected final String dataBasePath;
  protected final HoodieTableMetaClient dataMetaClient;
  protected final Option<HoodieMetadataMetrics> metrics;
  protected final HoodieMetadataConfig metadataConfig;
  // Directory used for Spillable Map when merging records
  protected final String spillableMapDirectory;

  protected boolean enabled;
  protected boolean isBloomFilterMetadataEnabled;
  protected boolean isColumnStatsMetadataEnabled;

  protected BaseTableMetadata(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig,
                              String dataBasePath, String spillableMapDirectory) {
    this.engineContext = engineContext;
    this.hadoopConf = new SerializableConfiguration(engineContext.getHadoopConf());
    this.dataBasePath = dataBasePath;
    this.dataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf.get()).setBasePath(dataBasePath).build();
    this.spillableMapDirectory = spillableMapDirectory;
    this.metadataConfig = metadataConfig;

    this.enabled = metadataConfig.enabled();
    this.isBloomFilterMetadataEnabled = (this.enabled && metadataConfig.isBloomFiltersEnabled());
    this.isColumnStatsMetadataEnabled = (this.enabled && metadataConfig.isColumnStatsEnabled());
    if (metadataConfig.enableMetrics()) {
      this.metrics = Option.of(new HoodieMetadataMetrics(Registry.getRegistry("HoodieMetadata")));
    } else {
      this.metrics = Option.empty();
    }
  }

  /**
   * Return the list of partitions in the dataset.
   * <p>
   * If the Metadata Table is enabled, the listing is retrieved from the stored metadata. Otherwise, the list of
   * partitions is retrieved directly from the underlying {@code FileSystem}.
   * <p>
   * On any errors retrieving the listing from the metadata, defaults to using the file system listings.
   */
  @Override
  public List<String> getAllPartitionPaths() throws IOException {
    if (enabled) {
      try {
        return fetchAllPartitionPaths();
      } catch (Exception e) {
        throw new HoodieMetadataException("Failed to retrieve list of partition from metadata", e);
      }
    }
    return new FileSystemBackedTableMetadata(getEngineContext(), hadoopConf, dataBasePath,
        metadataConfig.shouldAssumeDatePartitioning()).getAllPartitionPaths();
  }

  /**
   * Return the list of files in a partition.
   * <p>
   * If the Metadata Table is enabled, the listing is retrieved from the stored metadata. Otherwise, the list of
   * partitions is retrieved directly from the underlying {@code FileSystem}.
   * <p>
   * On any errors retrieving the listing from the metadata, defaults to using the file system listings.
   *
   * @param partitionPath The absolute path of the partition to list
   */
  @Override
  public FileStatus[] getAllFilesInPartition(Path partitionPath)
      throws IOException {
    if (enabled) {
      try {
        return fetchAllFilesInPartition(partitionPath);
      } catch (Exception e) {
        throw new HoodieMetadataException("Failed to retrieve files in partition " + partitionPath
            + " from metadata", e);
      }
    }

    return new FileSystemBackedTableMetadata(getEngineContext(), hadoopConf, dataBasePath,
        metadataConfig.shouldAssumeDatePartitioning())
        .getAllFilesInPartition(partitionPath);
  }

  @Override
  public Map<String, FileStatus[]> getAllFilesInPartitions(List<String> partitions)
      throws IOException {
    if (enabled) {
      try {
        List<Path> partitionPaths = partitions.stream().map(entry -> new Path(entry)).collect(Collectors.toList());
        Map<String, FileStatus[]> partitionsFilesMap = fetchAllFilesInPartitionPaths(partitionPaths);
        return partitionsFilesMap;
      } catch (Exception e) {
        throw new HoodieMetadataException("Failed to retrieve files in partition from metadata", e);
      }
    }

    return new FileSystemBackedTableMetadata(getEngineContext(), hadoopConf, dataBasePath,
        metadataConfig.shouldAssumeDatePartitioning())
        .getAllFilesInPartitions(partitions);
  }

  @Override
  public Option<ByteBuffer> getBloomFilter(FileID fileID) throws HoodieMetadataException {
    if (!isBloomFilterMetadataEnabled) {
      throw new HoodieMetadataException("Metadata table or the bloom filter indexing is disabled! Cannot get bloom "
          + "filter for " + fileID);
    }

    HoodieTimer timer = new HoodieTimer().startTimer();
    Option<HoodieRecord<HoodieMetadataPayload>> hoodieRecord = getRecordByKey(fileID.asBase64EncodedString(),
        MetadataPartitionType.BLOOM_FILTERS.partitionPath());
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_BLOOM_FILTERS_METADATA_STR, timer.endTimer()));

    if (!hoodieRecord.isPresent()) {
      return Option.empty();
    }

    final Option<HoodieMetadataBloomFilter> optionalBloomFilterMetadata =
        hoodieRecord.get().getData().getBloomFilterMetadata();
    if (!optionalBloomFilterMetadata.isPresent()) {
      LOG.error("Bloom filter metadata for " + fileID + " is not available!");
      return Option.empty();
    }

    return Option.of(optionalBloomFilterMetadata.get().getBloomfilter());
  }

  @Override
  public Map<String, ByteBuffer> getBloomFilters(List<FileID> fileIDList) throws HoodieMetadataException {
    if (!isBloomFilterMetadataEnabled) {
      throw new HoodieMetadataException("Metadata table or the bloom filter indexing is disabled!");
    }

    HoodieTimer timer = new HoodieTimer().startTimer();
    List<String> fileIDStrings = new ArrayList<>();
    fileIDList.forEach(fileID -> fileIDStrings.add(fileID.asBase64EncodedString()));
    List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> hoodieRecordList =
        getRecordsByKeys(fileIDStrings, MetadataPartitionType.BLOOM_FILTERS.partitionPath());
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_BLOOM_FILTERS_METADATA_STR, timer.endTimer()));

    Map<String, ByteBuffer> fileIDBloomFilterMap = new HashMap<>();
    for (final Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>> entry : hoodieRecordList) {
      if (entry.getRight().isPresent()) {
        final Option<HoodieMetadataBloomFilter> optionalBloomFilterMetadata =
            entry.getRight().get().getData().getBloomFilterMetadata();
        if (optionalBloomFilterMetadata.isPresent()) {
          fileIDBloomFilterMap.put(entry.getLeft(), optionalBloomFilterMetadata.get().getBloomfilter());
        }
      }
    }
    return fileIDBloomFilterMap;
  }

  @Override
  public Map<String, HoodieColumnStats> getColumnStats(List<String> keySet) throws HoodieMetadataException {

    if (!isColumnStatsMetadataEnabled) {
      throw new HoodieMetadataException("Metadata table or column stats indexing is disabled!");
    }

    HoodieTimer timer = new HoodieTimer().startTimer();
    List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> hoodieRecordList =
        getRecordsByKeys(keySet, MetadataPartitionType.COLUMN_STATS.partitionPath());
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_COLUMN_STATS_METADATA_STR, timer.endTimer()));

    Map<String, HoodieColumnStats> columnKeyToStatMap = new HashMap<>();
    for (final Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>> entry : hoodieRecordList) {
      if (entry.getRight().isPresent()) {
        final Option<HoodieColumnStats> optionalColumnStatPayload =
            entry.getRight().get().getData().getColumnStatMetadata();
        if (optionalColumnStatPayload.isPresent()) {
          ValidationUtils.checkState(!columnKeyToStatMap.containsKey(entry.getLeft()));
          columnKeyToStatMap.put(entry.getLeft(), optionalColumnStatPayload.get());
        }
      }
    }
    return columnKeyToStatMap;
  }

  /**
   * Returns a list of all partitions.
   */
  protected List<String> fetchAllPartitionPaths() throws IOException {
    HoodieTimer timer = new HoodieTimer().startTimer();
    Option<HoodieRecord<HoodieMetadataPayload>> hoodieRecord = getRecordByKey(RECORDKEY_PARTITION_LIST,
        MetadataPartitionType.FILES.partitionPath());
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_PARTITIONS_STR, timer.endTimer()));

    List<String> partitions = Collections.emptyList();
    if (hoodieRecord.isPresent()) {
      if (!hoodieRecord.get().getData().getDeletions().isEmpty()) {
        throw new HoodieMetadataException("Metadata partition list record is inconsistent: "
            + hoodieRecord.get().getData());
      }

      partitions = hoodieRecord.get().getData().getFilenames();
      // Partition-less tables have a single empty partition
      if (partitions.contains(NON_PARTITIONED_NAME)) {
        partitions.remove(NON_PARTITIONED_NAME);
        partitions.add("");
      }
    }

    LOG.info("Listed partitions from metadata: #partitions=" + partitions.size());
    return partitions;
  }

  /**
   * Return all the files from the partition.
   *
   * @param partitionPath The absolute path of the partition
   */
  FileStatus[] fetchAllFilesInPartition(Path partitionPath) throws IOException {
    String partitionName = FSUtils.getRelativePartitionPath(new Path(dataBasePath), partitionPath);
    if (partitionName.isEmpty()) {
      partitionName = NON_PARTITIONED_NAME;
    }

    HoodieTimer timer = new HoodieTimer().startTimer();
    Option<HoodieRecord<HoodieMetadataPayload>> hoodieRecord = getRecordByKey(partitionName,
        MetadataPartitionType.FILES.partitionPath());
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_FILES_STR, timer.endTimer()));

    FileStatus[] statuses = {};
    if (hoodieRecord.isPresent()) {
      if (!hoodieRecord.get().getData().getDeletions().isEmpty()) {
        throw new HoodieMetadataException("Metadata record for partition " + partitionName + " is inconsistent: "
            + hoodieRecord.get().getData());
      }
      statuses = hoodieRecord.get().getData().getFileStatuses(hadoopConf.get(), partitionPath);
    }

    LOG.info("Listed file in partition from metadata: partition=" + partitionName + ", #files=" + statuses.length);
    return statuses;
  }

  Map<String, FileStatus[]> fetchAllFilesInPartitionPaths(List<Path> partitionPaths) throws IOException {
    Map<String, Path> partitionInfo = new HashMap<>();
    boolean foundNonPartitionedPath = false;
    for (Path partitionPath : partitionPaths) {
      String partitionName = FSUtils.getRelativePartitionPath(new Path(dataBasePath), partitionPath);
      if (partitionName.isEmpty()) {
        if (partitionInfo.size() > 1) {
          throw new HoodieMetadataException("Found mix of partitioned and non partitioned paths while fetching data "
              + "from metadata table");
        }
        partitionInfo.put(NON_PARTITIONED_NAME, partitionPath);
        foundNonPartitionedPath = true;
      } else {
        if (foundNonPartitionedPath) {
          throw new HoodieMetadataException("Found mix of partitioned and non partitioned paths while fetching data "
              + "from metadata table");
        }
        partitionInfo.put(partitionName, partitionPath);
      }
    }

    HoodieTimer timer = new HoodieTimer().startTimer();
    List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> partitionsFileStatus =
        getRecordsByKeys(new ArrayList<>(partitionInfo.keySet()), MetadataPartitionType.FILES.partitionPath());
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_FILES_STR, timer.endTimer()));
    Map<String, FileStatus[]> result = new HashMap<>();

    for (Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>> entry : partitionsFileStatus) {
      if (entry.getValue().isPresent()) {
        if (!entry.getValue().get().getData().getDeletions().isEmpty()) {
          throw new HoodieMetadataException("Metadata record for partition " + entry.getKey() + " is inconsistent: "
              + entry.getValue().get().getData());
        }
        result.put(partitionInfo.get(entry.getKey()).toString(),
            entry.getValue().get().getData().getFileStatuses(hadoopConf.get(), partitionInfo.get(entry.getKey())));
      }
    }

    LOG.info("Listed files in partitions from metadata: partition list =" + Arrays.toString(partitionPaths.toArray()));
    return result;
  }

  protected abstract Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKey(String key, String partitionName);

  protected abstract List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> getRecordsByKeys(List<String> key, String partitionName);

  protected HoodieEngineContext getEngineContext() {
    return engineContext != null ? engineContext : new HoodieLocalEngineContext(hadoopConf.get());
  }

  public HoodieMetadataConfig getMetadataConfig() {
    return metadataConfig;
  }

  protected String getLatestDataInstantTime() {
    return dataMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant()
        .map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);
  }
}
