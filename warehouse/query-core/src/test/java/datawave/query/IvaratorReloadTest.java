package datawave.query;

import com.google.common.collect.Maps;
import datawave.core.iterators.DatawaveFieldIndexRangeIteratorJexl;
import datawave.query.iterator.SortedListKeyValueIterator;
import datawave.query.iterator.ivarator.IvaratorCacheDir;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.commons.pool.impl.GenericObjectPool.WHEN_EXHAUSTED_BLOCK;

/**
 * Verify that a rebuild of an ivarator will reuse the files from after a tear-down/rebuild
 */
public class IvaratorReloadTest {
    
    public static List<Map.Entry<Key,Value>> sourceList = new ArrayList<>();
    public static SortedKeyValueIterator source = new SortedListKeyValueIterator(sourceList);
    
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    /**
     * Basically create an ivarator, seek it and verify it persisted. Then create a second ivarator against the same directory, and verify it reused the same
     * files by verifying the files were not overwritten or changed.
     * 
     * @throws Exception
     */
    @Test
    public void reloadTest() throws Exception {
        setupKeyValues();
        
        File tempDir = temporaryFolder.newFolder();
        
        LocalFileSystem fs = new LocalFileSystem();
        fs.initialize(tempDir.toURI(), new Configuration());
        
        Path uniqueDir = new Path(tempDir.toURI().toString());
        
        DatawaveFieldIndexRangeIteratorJexl firstRangeIvarator = createRangeIvarator(fs, uniqueDir);
        firstRangeIvarator.init(source, null, null);
        Range range1 = new Range(new Key("20000105_0"), true, new Key("20000105_0" + Constants.MAX_UNICODE_STRING), false);
        firstRangeIvarator.seek(range1, new HashSet<>(), false);
        
        File shardDir = new File(tempDir, "20000105_0");
        File completeFile = new File(shardDir, "complete");
        
        IvaratorDirState state1 = getIvaratorDirState(shardDir);
        Assert.assertTrue("Missing complete file", state1.complete);
        Assert.assertFalse("Missing sorted set files", state1.sortedSetBytes.isEmpty());
        
        DatawaveFieldIndexRangeIteratorJexl secondRangeIvarator = createRangeIvarator(fs, uniqueDir);
        secondRangeIvarator.init(source, null, null);
        Range range2 = new Range(new Key("20000105_0", "MixedGeo\0-2xav59.x0c0q3.-yac0vs", "POINT\0POINT (2 3)"), false, new Key("20000105_0"
                        + Constants.MAX_UNICODE_STRING), false);
        secondRangeIvarator.seek(range2, new HashSet<>(), false);
        
        IvaratorDirState state2 = getIvaratorDirState(shardDir);
        Assert.assertEquals("Ivarator dir changed but it should not have", state1, state2);
    }
    
    public static DatawaveFieldIndexRangeIteratorJexl createRangeIvarator(FileSystem fs, Path uniqueDir) {
        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig(uniqueDir.toUri().toString());
        List<IvaratorCacheDir> cacheDirs = Collections.singletonList(new IvaratorCacheDir(config, fs, uniqueDir.toUri().toString()));
        
        // @formatter:off
        return DatawaveFieldIndexRangeIteratorJexl.builder()
                .withFieldName(new Text("POINT"))
                .withLowerBound("1f1bfaa80000000000")
                .lowerInclusive(true)
                .withUpperBound("1f240557ffffffffff")
                .upperInclusive(true)
                .withTimeFilter(null)
                .withDatatypeFilter(null)
                .negated(false)
                .withScanThreshold(1)
                .withScanTimeout(3600000)
                .withHdfsBackedSetBufferSize(10000)
                .withMaxRangeSplit(1)
                .withMaxOpenFiles(100)
                .withIvaratorCacheDirs(cacheDirs)
                .withQueryLock(null)
                .allowDirResuse(true)
                .withReturnKeyType(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME)
                .withSortedUUIDs(true)
                .withCompositeMetadata(null)
                .withCompositeSeekThreshold(10)
                .withTypeMetadata(null)
                .withSubRanges(null)
                .withIvaratorSourcePool(createIvaratorSourcePool(10))
                .build();
        // @formatter:on
    }
    
    public static GenericObjectPool<SortedKeyValueIterator<Key,Value>> createIvaratorSourcePool(int maxIvaratorSources) {
        return new GenericObjectPool<>(createIvaratorSourceFactory(), createIvaratorSourcePoolConfig(maxIvaratorSources));
    }
    
    public static BasePoolableObjectFactory<SortedKeyValueIterator<Key,Value>> createIvaratorSourceFactory() {
        return new BasePoolableObjectFactory<SortedKeyValueIterator<Key,Value>>() {
            @Override
            public SortedKeyValueIterator<Key,Value> makeObject() throws Exception {
                return new SortedListKeyValueIterator(sourceList);
            }
        };
    }
    
    public static GenericObjectPool.Config createIvaratorSourcePoolConfig(int maxIvaratorSources) {
        GenericObjectPool.Config poolConfig = new GenericObjectPool.Config();
        poolConfig.maxActive = maxIvaratorSources;
        poolConfig.maxIdle = maxIvaratorSources;
        poolConfig.minIdle = 0;
        poolConfig.whenExhaustedAction = WHEN_EXHAUSTED_BLOCK;
        return poolConfig;
    }
    
    public void setupKeyValues() {
        Key eventKey1 = new Key("20000105_0", "MixedGeo\0-2xav59.x0c0q3.-yac0vs", "POINT\0POINT (2 3)");
        sourceList.add(new AbstractMap.SimpleEntry<>(eventKey1, new Value()));
        
        Key eventKey2 = new Key("20000105_0", "MixedGeo\0-2xav59.x0c0q3.-yac0vz", "POINT\0POINT (0 1)");
        sourceList.add(new AbstractMap.SimpleEntry<>(eventKey2, new Value()));
        
        Key fiKey1 = new Key("20000105_0", "fi\0POINT", "1f1bfaa80000000000\0MixedGeo\0-2xav59.x0c0q3.-yac0vs");
        sourceList.add(new AbstractMap.SimpleEntry<>(fiKey1, new Value()));
        
        Key fiKey2 = new Key("20000105_0", "fi\0POINT", "1f240557ffffffffff\0MixedGeo\0-2xav59.x0c0q3.-yac0vz");
        sourceList.add(new AbstractMap.SimpleEntry<>(fiKey2, new Value()));
    }
    
    private class IvaratorDirState {
        Map<String,Long> sortedSetBytes = Maps.newHashMap();
        Map<String,Long> sortedSetDates = Maps.newHashMap();
        boolean complete = false;
        
        @Override
        public boolean equals(Object other) {
            if (other instanceof IvaratorDirState) {
                IvaratorDirState state = (IvaratorDirState) other;
                EqualsBuilder builder = new EqualsBuilder().append(complete, state.complete).append(sortedSetBytes, state.sortedSetBytes)
                                .append(sortedSetDates, state.sortedSetDates);
                return builder.build();
            }
            return false;
        }
        
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("complete: ").append(complete).append(", ").append("dates: ").append(sortedSetDates).append(", ").append("bytes: ")
                            .append(sortedSetBytes);
            return builder.toString();
        }
    }
    
    private IvaratorDirState getIvaratorDirState(File shardDir) {
        IvaratorDirState state = new IvaratorDirState();
        
        for (File file : shardDir.listFiles()) {
            if (file.getName().startsWith("SortedSetFile")) {
                state.sortedSetBytes.put(file.getName(), file.length());
                state.sortedSetDates.put(file.getName(), file.lastModified());
            } else if (file.getName().equals("complete")) {
                state.complete = true;
            }
        }
        return state;
    }
    
}
