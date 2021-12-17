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

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieList;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieColumnStatsMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.hash.FileIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.metadata.HoodieTableMetadata.EMPTY_PARTITION_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadata.NON_PARTITIONED_NAME;

/**
 * A utility to convert timeline information to metadata table records.
 */
public class HoodieTableMetadataUtil {

  private static final Logger LOG = LogManager.getLogger(HoodieTableMetadataUtil.class);

  protected static final String PARTITION_NAME_FILES = "files";
  protected static final String PARTITION_NAME_COLUMN_STATS = "column_stats";
  protected static final String PARTITION_NAME_BLOOM_FILTERS = "bloom_filters";

  /**
   * TODO: Comment.
   *
   * @param partitionPath
   * @return
   */
  public static Option<MetadataPartitionType> fromPartitionPath(final String partitionPath) {
    switch (partitionPath) {
      case PARTITION_NAME_FILES:
        return Option.of(MetadataPartitionType.FILES);
      case PARTITION_NAME_COLUMN_STATS:
        return Option.of(MetadataPartitionType.COLUMN_STATS);
      case PARTITION_NAME_BLOOM_FILTERS:
        return Option.of(MetadataPartitionType.BLOOM_FILTERS);
      default:
        LOG.error("Unexpected Partition path " + partitionPath);
        return Option.empty();
    }
  }

  /**
   * Delete the metadata table for the dataset. This will be invoked during upgrade/downgrade operation during which
   * no other
   * process should be running.
   *
   * @param basePath base path of the dataset
   * @param context  instance of {@link HoodieEngineContext}.
   */
  public static void deleteMetadataTable(String basePath, HoodieEngineContext context) {
    final String metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
    FileSystem fs = FSUtils.getFs(metadataTablePath, context.getHadoopConf().get());
    try {
      fs.delete(new Path(metadataTablePath), true);
    } catch (Exception e) {
      throw new HoodieMetadataException("Failed to remove metadata table from path " + metadataTablePath, e);
    }
  }

  /**
   * Convert commit action to metadata records for the enabled partition types.
   *
   * @param commitMetadata - Commit action metadata
   * @param dataMetaClient - Meta client for the data table
   * @param instantTime    - Action instant time
   * @return Map of partition to metadata records for the commit action
   */
  public static Map<MetadataPartitionType, HoodieData<HoodieRecord>> convertMetadataToRecords(
      HoodieEngineContext context, List<MetadataPartitionType> enabledPartitionTypes,
      HoodieCommitMetadata commitMetadata, HoodieTableMetaClient dataMetaClient, String instantTime) {
    final Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionToRecordsMap = new HashMap<>();
    final HoodieData<HoodieRecord> filesPartitionRecordsRDD = context.parallelize(
        convertMetadataToFilesPartitionRecords(commitMetadata, instantTime), 1);
    partitionToRecordsMap.put(MetadataPartitionType.FILES, filesPartitionRecordsRDD);

    if (enabledPartitionTypes.contains(MetadataPartitionType.BLOOM_FILTERS)) {
      final List<HoodieRecord> metadataBloomFilterRecords = convertMetadataToBloomFilterRecords(commitMetadata,
          dataMetaClient, instantTime);
      if (!metadataBloomFilterRecords.isEmpty()) {
        final HoodieData<HoodieRecord> metadataBloomFilterRecordsRDD = context.parallelize(metadataBloomFilterRecords, 1);
        partitionToRecordsMap.put(MetadataPartitionType.BLOOM_FILTERS, metadataBloomFilterRecordsRDD);
      }
    }

    if (enabledPartitionTypes.contains(MetadataPartitionType.COLUMN_STATS)) {
      final List<HoodieRecord> metadataColumnStats = convertMetadataToColumnStatsRecords(commitMetadata, context,
          dataMetaClient, instantTime);
      if (!metadataColumnStats.isEmpty()) {
        final HoodieData<HoodieRecord> metadataColumnStatsRDD = context.parallelize(metadataColumnStats, 1);
        partitionToRecordsMap.put(MetadataPartitionType.COLUMN_STATS, metadataColumnStatsRDD);
      }
    }

    return partitionToRecordsMap;
  }

  /**
   * Finds all new files/partitions created as part of commit and creates metadata table records for them.
   *
   * @param commitMetadata - Commit action metadata
   * @param instantTime    - Commit action instant time
   * @return List of metadata table records
   */
  public static List<HoodieRecord> convertMetadataToFilesPartitionRecords(HoodieCommitMetadata commitMetadata,
                                                                          String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    List<String> allPartitions = new LinkedList<>();
    commitMetadata.getPartitionToWriteStats().forEach((partitionStatName, writeStats) -> {
      final String partition = partitionStatName.equals(EMPTY_PARTITION_NAME) ? NON_PARTITIONED_NAME : partitionStatName;
      allPartitions.add(partition);

      Map<String, Long> newFiles = new HashMap<>(writeStats.size());
      writeStats.forEach(hoodieWriteStat -> {
        String pathWithPartition = hoodieWriteStat.getPath();
        if (pathWithPartition == null) {
          // Empty partition
          LOG.warn("Unable to find path in write stat to update metadata table " + hoodieWriteStat);
          return;
        }

        int offset = partition.equals(NON_PARTITIONED_NAME) ? (pathWithPartition.startsWith("/") ? 1 : 0) : partition.length() + 1;
        String filename = pathWithPartition.substring(offset);
        long totalWriteBytes = newFiles.containsKey(filename)
            ? newFiles.get(filename) + hoodieWriteStat.getTotalWriteBytes()
            : hoodieWriteStat.getTotalWriteBytes();
        newFiles.put(filename, totalWriteBytes);
      });
      // New files added to a partition
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(
          partition, Option.of(newFiles), Option.empty());
      records.add(record);
    });

    // New partitions created
    HoodieRecord record = HoodieMetadataPayload.createPartitionListRecord(new ArrayList<>(allPartitions));
    records.add(record);

    LOG.info("Updating at " + instantTime + " from Commit/" + commitMetadata.getOperationType()
        + ". #partitions_updated=" + records.size());
    return records;
  }

  /**
   * Convert commit action metadata to bloom filter records.
   *
   * @param commitMetadata - Commit action metadata
   * @param dataMetaClient - Meta client for the data table
   * @param instantTime    - Action instant time
   * @return List of metadata table records
   */
  public static List<HoodieRecord> convertMetadataToBloomFilterRecords(HoodieCommitMetadata commitMetadata,
                                                                       HoodieTableMetaClient dataMetaClient,
                                                                       String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    commitMetadata.getPartitionToWriteStats().forEach((partitionStatName, writeStats) -> {
      final String partition = partitionStatName.equals(EMPTY_PARTITION_NAME) ? NON_PARTITIONED_NAME : partitionStatName;
      Map<String, Long> newFiles = new HashMap<>(writeStats.size());
      writeStats.forEach(hoodieWriteStat -> {
        // No action for delta logs
        if (hoodieWriteStat instanceof HoodieDeltaWriteStat) {
          return;
        }

        String pathWithPartition = hoodieWriteStat.getPath();
        if (pathWithPartition == null) {
          // Empty partition
          LOG.error("Failed to find path in write stat to update metadata table " + hoodieWriteStat);
          return;
        }

        int offset = partition.equals(NON_PARTITIONED_NAME) ? (pathWithPartition.startsWith("/") ? 1 : 0) :
            partition.length() + 1;

        String filename = pathWithPartition.substring(offset);
        ValidationUtils.checkState(!newFiles.containsKey(filename), "Duplicate files in HoodieCommitMetadata");
        Path writeFilePath = new Path(dataMetaClient.getBasePath(), pathWithPartition);
        try {
          HoodieFileReader<IndexedRecord> fileReader =
              HoodieFileReaderFactory.getFileReader(dataMetaClient.getHadoopConf(), writeFilePath);
          final BloomFilter fileBloomFilter = fileReader.readBloomFilter();
          if (fileBloomFilter == null) {
            LOG.error("Failed to read bloom filter for " + writeFilePath);
            return;
          }
          ByteBuffer bloomByteBuffer = ByteBuffer.wrap(fileBloomFilter.serializeToString().getBytes());
          // TODO: Improve the schema to include the filter type also
          HoodieRecord record = HoodieMetadataPayload.createBloomFilterMetadataRecord(
              new PartitionIndexID(partition), new FileIndexID(filename), instantTime, bloomByteBuffer, true);
          records.add(record);
          fileReader.close();
        } catch (IOException e) {
          LOG.error("Failed to get bloom filter for file: " + writeFilePath + ", write stat: " + hoodieWriteStat);
        }
      });
    });

    return records;
  }

  /**
   * Convert the clean action to metadata records.
   */
  public static Map<MetadataPartitionType, HoodieData<HoodieRecord>> convertMetadataToRecords(
      HoodieEngineContext engineContext, List<MetadataPartitionType> enabledPartitionTypes,
      HoodieCleanMetadata cleanMetadata, String instantTime) {
    final Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionToRecordsMap = new HashMap<>();
    final HoodieData<HoodieRecord> filesPartitionRecordsRDD = engineContext.parallelize(
        convertMetadataToFilesPartitionRecords(cleanMetadata, enabledPartitionTypes, instantTime), 1);
    partitionToRecordsMap.put(MetadataPartitionType.FILES, filesPartitionRecordsRDD);
    return partitionToRecordsMap;
  }

  /**
   * Finds all files that were deleted as part of a clean and creates metadata table records for them.
   *
   * @param cleanMetadata
   * @param instantTime
   * @return a list of metadata table records
   */
  public static List<HoodieRecord> convertMetadataToFilesPartitionRecords(HoodieCleanMetadata cleanMetadata,
                                                                          List<MetadataPartitionType> enabledPartitionTypes,
                                                                          String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    int[] fileDeleteCount = {0};
    cleanMetadata.getPartitionMetadata().forEach((partitionName, partitionMetadata) -> {
      final String partition = partitionName.equals(EMPTY_PARTITION_NAME) ? NON_PARTITIONED_NAME : partitionName;
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.empty(),
          Option.of(new ArrayList<>(deletedFiles)));

      records.add(record);
      fileDeleteCount[0] += deletedFiles.size();
    });

    LOG.info("Updating at " + instantTime + " from Clean. #partitions_updated=" + records.size()
        + ", #files_deleted=" + fileDeleteCount[0]);
    return records;
  }

  /**
   * TODO: Comment.
   */
  public static Map<MetadataPartitionType, HoodieData<HoodieRecord>> convertMetadataToRecords(
      HoodieEngineContext engineContext, List<MetadataPartitionType> enabledPartitionTypes,
      HoodieActiveTimeline metadataTableTimeline, HoodieRestoreMetadata restoreMetadata, String instantTime,
      Option<String> lastSyncTs) {
    Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionToRecordsMap = new HashMap<>();
    partitionToRecordsMap.put(MetadataPartitionType.FILES, convertMetadataToRestoreRecords(
        engineContext, metadataTableTimeline, restoreMetadata, instantTime, lastSyncTs));
    return partitionToRecordsMap;
  }

  /**
   * Aggregates all files deleted and appended to from all rollbacks associated with a restore operation then
   * creates metadata table records for them.
   *
   * @param engineContext
   * @param restoreMetadata
   * @param instantTime
   * @return a list of metadata table records
   */
  public static HoodieData<HoodieRecord> convertMetadataToRestoreRecords(HoodieEngineContext engineContext, HoodieActiveTimeline metadataTableTimeline,
                                                                         HoodieRestoreMetadata restoreMetadata,
                                                                         String instantTime, Option<String> lastSyncTs) {
    Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    Map<String, List<String>> partitionToDeletedFiles = new HashMap<>();
    restoreMetadata.getHoodieRestoreMetadata().values().forEach(rms -> {
      rms.forEach(rm -> processRollbackMetadata(metadataTableTimeline, rm, partitionToDeletedFiles, partitionToAppendedFiles, lastSyncTs));
    });

    return engineContext.parallelize(convertFilesToRecords(partitionToDeletedFiles, partitionToAppendedFiles, instantTime, "Restore"), 1);
  }

  /**
   * TODO: Comment.
   */
  public static Map<MetadataPartitionType, HoodieData<HoodieRecord>> convertMetadataToRecords(
      HoodieEngineContext engineContext, List<MetadataPartitionType> enabledPartitionTypes,
      HoodieActiveTimeline metadataTableTimeline, HoodieRollbackMetadata rollbackMetadata, String instantTime,
      Option<String> lastSyncTs, boolean wasSynced) {
    final Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionToRecordsMap = new HashMap<>();
    final HoodieData<HoodieRecord> rollbackRecordsRDD = engineContext.parallelize(convertMetadataToRollbackRecords(
        metadataTableTimeline, rollbackMetadata, instantTime, lastSyncTs, wasSynced), 1);
    partitionToRecordsMap.put(MetadataPartitionType.FILES, rollbackRecordsRDD);
    return partitionToRecordsMap;
  }

  public static List<HoodieRecord> convertMetadataToRollbackRecords(HoodieActiveTimeline metadataTableTimeline,
                                                                    HoodieRollbackMetadata rollbackMetadata,
                                                                    String instantTime,
                                                                    Option<String> lastSyncTs, boolean wasSynced) {
    Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    Map<String, List<String>> partitionToDeletedFiles = new HashMap<>();
    processRollbackMetadata(metadataTableTimeline, rollbackMetadata, partitionToDeletedFiles,
        partitionToAppendedFiles, lastSyncTs);
    if (!wasSynced) {
      // Since the instant-being-rolled-back was never committed to the metadata table, the files added there
      // need not be deleted. For MOR Table, the rollback appends logBlocks so we need to keep the appended files.
      partitionToDeletedFiles.clear();
    }
    return convertFilesToRecords(partitionToDeletedFiles, partitionToAppendedFiles, instantTime, "Rollback");
  }

  /**
   * Extracts information about the deleted and append files from the {@code HoodieRollbackMetadata}.
   *
   * During a rollback files may be deleted (COW, MOR) or rollback blocks be appended (MOR only) to files. This
   * function will extract this change file for each partition.
   * @param metadataTableTimeline Current timeline of the Metdata Table
   * @param rollbackMetadata {@code HoodieRollbackMetadata}
   * @param partitionToDeletedFiles The {@code Map} to fill with files deleted per partition.
   * @param partitionToAppendedFiles The {@code Map} to fill with files appended per partition and their sizes.
   */
  private static void processRollbackMetadata(HoodieActiveTimeline metadataTableTimeline,
                                              HoodieRollbackMetadata rollbackMetadata,
                                              Map<String, List<String>> partitionToDeletedFiles,
                                              Map<String, Map<String, Long>> partitionToAppendedFiles,
                                              Option<String> lastSyncTs) {
    rollbackMetadata.getPartitionMetadata().values().forEach(pm -> {
      final String instantToRollback = rollbackMetadata.getCommitsRollback().get(0);
      // Has this rollback produced new files?
      boolean hasRollbackLogFiles = pm.getRollbackLogFiles() != null && !pm.getRollbackLogFiles().isEmpty();
      boolean hasNonZeroRollbackLogFiles = hasRollbackLogFiles && pm.getRollbackLogFiles().values().stream().mapToLong(Long::longValue).sum() > 0;

      // If instant-to-rollback has not been synced to metadata table yet then there is no need to update metadata
      // This can happen in two cases:
      //  Case 1: Metadata Table timeline is behind the instant-to-rollback.
      boolean shouldSkip = lastSyncTs.isPresent()
          && HoodieTimeline.compareTimestamps(instantToRollback, HoodieTimeline.GREATER_THAN, lastSyncTs.get());

      if (!hasNonZeroRollbackLogFiles && shouldSkip) {
        LOG.info(String.format("Skipping syncing of rollbackMetadata at %s, given metadata table is already synced upto to %s",
            instantToRollback, lastSyncTs.get()));
        return;
      }

      // Case 2: The instant-to-rollback was never committed to Metadata Table. This can happen if the instant-to-rollback
      // was a failed commit (never completed) as only completed instants are synced to Metadata Table.
      // But the required Metadata Table instants should not have been archived
      HoodieInstant syncedInstant = new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, instantToRollback);
      if (metadataTableTimeline.getCommitsTimeline().isBeforeTimelineStarts(syncedInstant.getTimestamp())) {
        throw new HoodieMetadataException(String.format("The instant %s required to sync rollback of %s has been archived",
            syncedInstant, instantToRollback));
      }

      shouldSkip = !metadataTableTimeline.containsInstant(syncedInstant);
      if (!hasNonZeroRollbackLogFiles && shouldSkip) {
        LOG.info(String.format("Skipping syncing of rollbackMetadata at %s, since this instant was never committed to Metadata Table",
            instantToRollback));
        return;
      }

      final String partition = pm.getPartitionPath();
      if ((!pm.getSuccessDeleteFiles().isEmpty() || !pm.getFailedDeleteFiles().isEmpty()) && !shouldSkip) {
        if (!partitionToDeletedFiles.containsKey(partition)) {
          partitionToDeletedFiles.put(partition, new ArrayList<>());
        }

        // Extract deleted file name from the absolute paths saved in getSuccessDeleteFiles()
        List<String> deletedFiles = pm.getSuccessDeleteFiles().stream().map(p -> new Path(p).getName())
            .collect(Collectors.toList());
        if (!pm.getFailedDeleteFiles().isEmpty()) {
          deletedFiles.addAll(pm.getFailedDeleteFiles().stream().map(p -> new Path(p).getName())
              .collect(Collectors.toList()));
        }
        partitionToDeletedFiles.get(partition).addAll(deletedFiles);
      }

      BiFunction<Long, Long, Long> fileMergeFn = (oldSize, newSizeCopy) -> {
        // if a file exists in both written log files and rollback log files, we want to pick the one that is higher
        // as rollback file could have been updated after written log files are computed.
        return oldSize > newSizeCopy ? oldSize : newSizeCopy;
      };

      if (hasRollbackLogFiles) {
        if (!partitionToAppendedFiles.containsKey(partition)) {
          partitionToAppendedFiles.put(partition, new HashMap<>());
        }

        // Extract appended file name from the absolute paths saved in getAppendFiles()
        pm.getRollbackLogFiles().forEach((path, size) -> {
          partitionToAppendedFiles.get(partition).merge(new Path(path).getName(), size, fileMergeFn);
        });
      }
    });
  }

  private static List<HoodieRecord> convertFilesToRecords(Map<String, List<String>> partitionToDeletedFiles,
                                                          Map<String, Map<String, Long>> partitionToAppendedFiles, String instantTime,
                                                          String operation) {
    List<HoodieRecord> records = new LinkedList<>();
    int[] fileChangeCount = {0, 0}; // deletes, appends

    partitionToDeletedFiles.forEach((partitionName, deletedFiles) -> {
      fileChangeCount[0] += deletedFiles.size();
      final String partition = partitionName.equals(EMPTY_PARTITION_NAME) ? NON_PARTITIONED_NAME : partitionName;

      Option<Map<String, Long>> filesAdded = Option.empty();
      if (partitionToAppendedFiles.containsKey(partitionName)) {
        filesAdded = Option.of(partitionToAppendedFiles.remove(partitionName));
      }

      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, filesAdded,
          Option.of(new ArrayList<>(deletedFiles)));
      records.add(record);
    });

    partitionToAppendedFiles.forEach((partitionName, appendedFileMap) -> {
      final String partition = partitionName.equals(EMPTY_PARTITION_NAME) ? NON_PARTITIONED_NAME : partitionName;
      fileChangeCount[1] += appendedFileMap.size();

      // Validate that no appended file has been deleted
      ValidationUtils.checkState(
          !appendedFileMap.keySet().removeAll(partitionToDeletedFiles.getOrDefault(partition, Collections.emptyList())),
          "Rollback file cannot both be appended and deleted");

      // New files added to a partition
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.of(appendedFileMap),
          Option.empty());
      records.add(record);
    });

    LOG.info("Found at " + instantTime + " from " + operation + ". #partitions_updated=" + records.size()
        + ", #files_deleted=" + fileChangeCount[0] + ", #files_appended=" + fileChangeCount[1]);

    return records;
  }

  /**
   * Map a record key to a file group in partition of interest.
   *
   * Note: For hashing, the algorithm is same as String.hashCode() but is being defined here as hashCode()
   * implementation is not guaranteed by the JVM to be consistent across JVM versions and implementations.
   *
   * @param recordKey record key for which the file group index is looked up for.
   * @return An integer hash of the given string
   */
  public static int mapRecordKeyToFileGroupIndex(String recordKey, int numFileGroups) {
    int h = 0;
    for (int i = 0; i < recordKey.length(); ++i) {
      h = 31 * h + recordKey.charAt(i);
    }

    return Math.abs(Math.abs(h) % numFileGroups);
  }

  /**
   * Get the latest file slices for a Metadata Table partition. If the file slice is
   * because of pending compaction instant, then merge the file slice with the one
   * just before the compaction instant time. The list of file slices returned is
   * sorted in the correct order of file group name.
   *
   * @param metaClient - Instance of {@link HoodieTableMetaClient}.
   * @param partition  - The name of the partition whose file groups are to be loaded.
   * @return List of latest file slices for all file groups in a given partition.
   */
  public static List<FileSlice> getPartitionLatestMergedFileSlices(HoodieTableMetaClient metaClient, String partition) {
    LOG.info("Loading latest merged file slices for metadata table partition " + partition);
    return getPartitionFileSlices(metaClient, partition, true);
  }

  /**
   * Get the latest file slices for a Metadata Table partition. The list of file slices
   * returned is sorted in the correct order of file group name.
   *
   * @param metaClient - Instance of {@link HoodieTableMetaClient}.
   * @param partition  - The name of the partition whose file groups are to be loaded.
   * @return List of latest file slices for all file groups in a given partition.
   */
  public static List<FileSlice> getPartitionLatestFileSlices(HoodieTableMetaClient metaClient, String partition) {
    LOG.info("Loading latest file slices for metadata table partition " + partition);
    return getPartitionFileSlices(metaClient, partition, false);
  }

  /**
   * Get the latest file slices for a given partition.
   *
   * @param metaClient      - Instance of {@link HoodieTableMetaClient}.
   * @param partition       - The name of the partition whose file groups are to be loaded.
   * @param mergeFileSlices - When enabled, will merge the latest file slices with the last known
   *                        completed instant. This is useful for readers when there are pending
   *                        compactions. MergeFileSlices when disabled, will return the latest file
   *                        slices without any merging, and this is needed for the writers.
   * @return List of latest file slices for all file groups in a given partition.
   */
  private static List<FileSlice> getPartitionFileSlices(HoodieTableMetaClient metaClient, String partition,
                                                        boolean mergeFileSlices) {
    // If there are no commits on the metadata table then the table's
    // default FileSystemView will not return any file slices even
    // though we may have initialized them.
    HoodieTimeline timeline = metaClient.getActiveTimeline();
    if (timeline.empty()) {
      final HoodieInstant instant = new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION,
          HoodieActiveTimeline.createNewInstantTime());
      timeline = new HoodieDefaultTimeline(Arrays.asList(instant).stream(), metaClient.getActiveTimeline()::getInstantDetails);
    }

    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, timeline);
    Stream<FileSlice> fileSliceStream;
    if (mergeFileSlices) {
      fileSliceStream = fsView.getLatestMergedFileSlicesBeforeOrOn(
          partition, timeline.filterCompletedInstants().lastInstant().get().getTimestamp());
    } else {
      fileSliceStream = fsView.getLatestFileSlices(partition);
    }
    return fileSliceStream.sorted((s1, s2) -> s1.getFileId().compareTo(s2.getFileId())).collect(Collectors.toList());
  }

  public static List<HoodieRecord> convertMetadataToColumnStatsRecords(HoodieCommitMetadata commitMetadata,
                                                                       HoodieEngineContext engineContext,
                                                                       HoodieTableMetaClient dataMetaClient,
                                                                       String instantTime) {

    try {
      List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
          .flatMap(entry -> entry.stream()).collect(Collectors.toList());
      //TODO this result may be too large to fit into memory. We may need to convert to dataframe and move this to
      // spark-client
      return HoodieTableMetadataUtil.createColumnStatsFromWriteStats(engineContext, dataMetaClient, allWriteStats);
    } catch (Exception e) {
      throw new HoodieException("Failed to generate column stats records for metadata table ", e);
    }
  }

  public static HoodieData<HoodieRecord> convertMetadataToColumnStatsRecords(HoodieCleanMetadata cleanMetadata,
                                                                             HoodieEngineContext engineContext,
                                                                             HoodieTableMetaClient datasetMetaClient,
                                                                             String instantTime) throws Exception {
    List<Pair<String, String>> deleteFileInfos = new ArrayList<>();
    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      deletedFiles.forEach(entry -> deleteFileInfos.add(Pair.of(partition, entry)));
    });

    List<String> latestColumns = getLatestColumns(datasetMetaClient);
    return HoodieList.of(engineContext.flatMap(deleteFileInfos,
        deleteFileInfo -> getColumnStats(deleteFileInfo.getKey(), deleteFileInfo.getValue(), datasetMetaClient,
            Option.of(latestColumns), true),
        deleteFileInfos.size()));
  }

  /**
   * @param engineContext
   * @param datasetMetaClient
   * @param allWriteStats
   * @return a Map < Pair (partition path, file path)> -> collection of stats >
   */
  public static List<HoodieRecord> createColumnStatsFromWriteStats(HoodieEngineContext engineContext,
                                                                   HoodieTableMetaClient datasetMetaClient,
                                                                   List<HoodieWriteStat> allWriteStats) throws Exception {
    if (allWriteStats.isEmpty()) {
      return Collections.emptyList();
    }

    List<HoodieWriteStat> prunedWriteStats = allWriteStats.stream().filter(writeStat -> {
      return !(writeStat instanceof HoodieDeltaWriteStat);
    }).collect(Collectors.toList());
    if (prunedWriteStats.isEmpty()) {
      return Collections.emptyList();
    }

    // Considering only HoodieRecord.RECORD_KEY_METADATA_FIELD for index lookup
    // TODO: Use all latest columns for full col index build and lookup
    return engineContext.flatMap(prunedWriteStats,
        writeStat -> translateWriteStatToColumnStats(writeStat, datasetMetaClient,
            Option.of(Collections.singletonList(HoodieRecord.RECORD_KEY_METADATA_FIELD))), prunedWriteStats.size());
  }

  private static List<String> getLatestColumns(HoodieTableMetaClient datasetMetaClient) throws Exception {
    if (datasetMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants() > 1) {
      TableSchemaResolver schemaResolver = new TableSchemaResolver(datasetMetaClient);
      // consider nested fields as well.
      // if column stats is enabled only for a subset of columns, directly use them instead of all columns from
      // latest table schema
      return schemaResolver.getTableAvroSchema().getFields().stream().map(entry -> entry.name()).collect(Collectors.toList());

    } else {
      return Collections.emptyList();

    }
  }

  public static Stream<HoodieRecord> translateWriteStatToColumnStats(HoodieWriteStat writeStat,
                                                                     HoodieTableMetaClient datasetMetaClient,
                                                                     Option<List<String>> latestColumns) throws Exception {
    return getColumnStats(writeStat.getPartitionPath(), writeStat.getPath(), datasetMetaClient, latestColumns, false);

  }

  public static Stream<HoodieRecord> getColumnStats(final String partitionPath, final String path,
                                                    HoodieTableMetaClient datasetMetaClient,
                                                    Option<List<String>> latestColumns,
                                                    boolean isDeleted) throws Exception {
    if (path.endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
      //TODO: we are capturing range for most columns in the schema. Should we have allowList/denyList to reduce
      // storage size of index?
      Collection<HoodieColumnStatsMetadata<Comparable>> columnStatsMetadata = new ArrayList<>();
      if (!isDeleted) {
        columnStatsMetadata = new ParquetUtils().readColumnStatsFromParquetMetadata(
            datasetMetaClient.getHadoopConf(), partitionPath, new Path(datasetMetaClient.getBasePath(), path),
            latestColumns);
      } else {
        columnStatsMetadata =
            latestColumns.get().stream().map(entry -> new HoodieColumnStatsMetadata<Comparable>(partitionPath, path,
                entry, null, null, 0, true)).collect(Collectors.toList());
      }
      return HoodieMetadataPayload.createColumnStatsRecords(columnStatsMetadata);
    } else {
      throw new HoodieException("range index not supported for path " + path);
    }
  }

}
