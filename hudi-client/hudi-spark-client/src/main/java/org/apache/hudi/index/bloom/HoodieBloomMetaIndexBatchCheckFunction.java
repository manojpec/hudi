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

package org.apache.hudi.index.bloom;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.bloom.HoodieDynamicBoundedBloomFilter;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.HoodieKeyLookupResult;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Function performing actual checking of RDD partition containing (fileId, hoodieKeys) against the actual files.
 */
public class HoodieBloomMetaIndexBatchCheckFunction implements
    Function2<Integer, Iterator<Tuple2<String, HoodieKey>>, Iterator<List<HoodieKeyLookupResult>>> {

  private static final Logger LOG = LogManager.getLogger(HoodieBloomMetaIndexBatchCheckFunction.class);
  private final HoodieTable hoodieTable;
  private final HoodieWriteConfig config;

  public HoodieBloomMetaIndexBatchCheckFunction(HoodieTable hoodieTable, HoodieWriteConfig config) {
    this.hoodieTable = hoodieTable;
    this.config = config;
  }

  @Override
  public Iterator<List<HoodieKeyLookupResult>> call(Integer integer, Iterator<Tuple2<String, HoodieKey>> tuple2Iterator) throws Exception {
    List<List<HoodieKeyLookupResult>> resultList = new ArrayList<>();
    Map<Pair<String, String>, List<HoodieKey>> fileToKeysMap = new HashMap<>();

    final Map<String, HoodieBaseFile> fileIDBaseFileMap = new HashMap<>();
    while (tuple2Iterator.hasNext()) {
      Tuple2<String, HoodieKey> entry = tuple2Iterator.next();
      final String partitionPath = entry._2.getPartitionPath();
      final String fileId = entry._1;
      if (!fileIDBaseFileMap.containsKey(fileId)) {
        Option<HoodieBaseFile> baseFile = hoodieTable.getBaseFileOnlyView().getLatestBaseFile(partitionPath, fileId);
        if (!baseFile.isPresent()) {
          throw new HoodieIndexException("Failed to find the base file for partition: " + partitionPath
              + ", fileId: " + fileId);
        }
        fileIDBaseFileMap.put(fileId, baseFile.get());
      }
      fileToKeysMap.computeIfAbsent(Pair.of(partitionPath, fileIDBaseFileMap.get(fileId).getFileName()),
          k -> new ArrayList<>()).add(entry._2);
    }
    if (fileToKeysMap.isEmpty()) {
      return Collections.emptyListIterator();
    }

    List<Pair<String, String>> partitionNameFileNameList =
        fileToKeysMap.keySet().stream().map(partitionNameFileNamePair -> {
          return Pair.of(partitionNameFileNamePair.getLeft(), partitionNameFileNamePair.getRight());
        }).collect(Collectors.toList());

    Map<Pair<String, String>, ByteBuffer> fileIDToBloomFilterByteBufferMap =
        hoodieTable.getMetadataTable().getBloomFilters(partitionNameFileNameList);

    fileToKeysMap.forEach((partitionPathFileIdPair, hoodieKeyList) -> {
      final String partitionPath = partitionPathFileIdPair.getLeft();
      final String fileName = partitionPathFileIdPair.getRight();
      final Pair<String, String> partitionFileNamePair = Pair.of(partitionPath, fileName);
      final String fileId = FSUtils.getFileId(fileName);
      ValidationUtils.checkState(!fileId.isEmpty());

      if (!fileIDToBloomFilterByteBufferMap.containsKey(partitionFileNamePair)) {
        throw new HoodieIndexException("Failed to get the bloom filter for " + partitionPathFileIdPair);
      }
      final ByteBuffer fileBloomFilterByteBuffer = fileIDToBloomFilterByteBufferMap.get(partitionFileNamePair);

      HoodieDynamicBoundedBloomFilter fileBloomFilter =
          new HoodieDynamicBoundedBloomFilter(StandardCharsets.UTF_8.decode(fileBloomFilterByteBuffer).toString(),
              BloomFilterTypeCode.DYNAMIC_V0);

      List<String> candidateRecordKeys = new ArrayList<>();
      hoodieKeyList.forEach(hoodieKey -> {
        if (fileBloomFilter.mightContain(hoodieKey.getRecordKey())) {
          candidateRecordKeys.add(hoodieKey.getRecordKey());
        }
      });

      final HoodieBaseFile dataFile = fileIDBaseFileMap.get(fileId);
      List<String> matchingKeys =
          HoodieIndexUtils.filterKeysFromFile(new Path(dataFile.getPath()), candidateRecordKeys,
              hoodieTable.getHadoopConf());
      LOG.debug(
          String.format("Total records (%d), bloom filter candidates (%d)/fp(%d), actual matches (%d)",
              hoodieKeyList.size(), candidateRecordKeys.size(),
              candidateRecordKeys.size() - matchingKeys.size(), matchingKeys.size()));

      ArrayList<HoodieKeyLookupResult> subList = new ArrayList<>();
      subList.add(new HoodieKeyLookupResult(fileId, partitionPath, dataFile.getCommitTime(),
          matchingKeys));
      resultList.add(subList);
    });

    return resultList.iterator();
  }

}
