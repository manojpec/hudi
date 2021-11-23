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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.bloom.BloomFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;

public class HoodieHFileConfig {

  // This is private in CacheConfig so have been copied here.
  private static boolean DROP_BEHIND_CACHE_COMPACTION_DEFAULT = true;
  private static KeyValue.KVComparator defaultHFileComparator = new HoodieHBaseComparators.HoodieHBaseKVComparator();

  private Compression.Algorithm compressionAlgorithm;
  private int blockSize;
  private long maxFileSize;
  private boolean prefetchBlocksOnOpen;
  private boolean cacheDataInL1;
  private boolean dropBehindCacheCompaction;
  private Configuration hadoopConf;
  private BloomFilter bloomFilter;
  private KeyValue.KVComparator hfileComparator;

  public HoodieHFileConfig(Configuration hadoopConf, Compression.Algorithm compressionAlgorithm, int blockSize,
                           long maxFileSize, BloomFilter bloomFilter) {
    this(hadoopConf, compressionAlgorithm, blockSize, maxFileSize, CacheConfig.DEFAULT_PREFETCH_ON_OPEN,
        HColumnDescriptor.DEFAULT_CACHE_DATA_IN_L1, DROP_BEHIND_CACHE_COMPACTION_DEFAULT, bloomFilter, defaultHFileComparator);
  }

  public HoodieHFileConfig(Configuration hadoopConf, Compression.Algorithm compressionAlgorithm, int blockSize,
      long maxFileSize, BloomFilter bloomFilter, KeyValue.KVComparator hfileComparator) {
    this(hadoopConf, compressionAlgorithm, blockSize, maxFileSize, CacheConfig.DEFAULT_PREFETCH_ON_OPEN,
        HColumnDescriptor.DEFAULT_CACHE_DATA_IN_L1, DROP_BEHIND_CACHE_COMPACTION_DEFAULT, bloomFilter, hfileComparator);
  }

  public HoodieHFileConfig(Configuration hadoopConf, Compression.Algorithm compressionAlgorithm, int blockSize,
                           long maxFileSize, boolean prefetchBlocksOnOpen, boolean cacheDataInL1,
                           boolean dropBehindCacheCompaction, BloomFilter bloomFilter, KeyValue.KVComparator hfileComparator) {
    this.hadoopConf = hadoopConf;
    this.compressionAlgorithm = compressionAlgorithm;
    this.blockSize = blockSize;
    this.maxFileSize = maxFileSize;
    this.prefetchBlocksOnOpen = prefetchBlocksOnOpen;
    this.cacheDataInL1 = cacheDataInL1;
    this.dropBehindCacheCompaction = dropBehindCacheCompaction;
    this.bloomFilter = bloomFilter;
    this.hfileComparator = hfileComparator;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public Compression.Algorithm getCompressionAlgorithm() {
    return compressionAlgorithm;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public long getMaxFileSize() {
    return maxFileSize;
  }

  public boolean shouldPrefetchBlocksOnOpen() {
    return prefetchBlocksOnOpen;
  }

  public boolean shouldCacheDataInL1() {
    return cacheDataInL1;
  }

  public boolean shouldDropBehindCacheCompaction() {
    return dropBehindCacheCompaction;
  }

  public boolean useBloomFilter() {
    return bloomFilter != null;
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }

  public KeyValue.KVComparator getHfileComparator() {
    return hfileComparator;
  }
}
