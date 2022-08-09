package datawave.core.iterators;

import datawave.query.Constants;
import datawave.query.iterator.ivarator.IvaratorCacheDir;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class DatawaveFieldIndexIteratorJexlTest {
    
    @TempDir
    public File cacheDir = new File("/tmp/test/DatawaveFieldIndexIteratorJexlTest");
    
    FileSystem fs;
    List<IvaratorCacheDir> cacheDirs;
    
    @BeforeEach
    public void setup() throws IOException {
        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig(cacheDir.toURI().toString());
        fs = FileSystem.get(cacheDir.toURI(), new Configuration());
        File queryDirFile = new File(cacheDir, "query");
        queryDirFile.deleteOnExit();
        Assertions.assertTrue(queryDirFile.mkdirs());
        String queryDir = queryDirFile.toURI().toString();
        cacheDirs = Collections.singletonList(new IvaratorCacheDir(config, fs, queryDir));
    }
    
    @AfterEach
    public void cleanup() throws IOException {
        fs.close();
    }
    
    @Test
    public void buildBoundingFiRange_notUpperInclusive_singleChar_test() {
        DatawaveFieldIndexFilterIteratorJexl iteratorJexl = DatawaveFieldIndexFilterIteratorJexl.builder().upperInclusive(false).lowerInclusive(true)
                        .withMaxRangeSplit(1).withFieldName("FIELD").withFieldValue("a").withUpperBound("b").withIvaratorCacheDirs(cacheDirs).build();
        
        Text row = new Text("row");
        Text fiName = new Text("fi" + Constants.NULL + "FIELD");
        Text fieldValue = new Text("a");
        Text fieldValueNullAppended = new Text("a" + Constants.NULL);
        
        List<Range> ranges = iteratorJexl.buildBoundingFiRanges(row, fiName, fieldValue);
        
        Assertions.assertNotEquals(null, ranges);
        Assertions.assertEquals(1, ranges.size());
        Range r = ranges.get(0);
        
        // note that the end key is expected to be inclusive even though upperInclusive is set to false because the value has been decremented by one
        Assertions.assertTrue(r.isStartKeyInclusive());
        Assertions.assertTrue(r.isEndKeyInclusive());
        Assertions.assertEquals(new Key(row, fiName, fieldValueNullAppended), r.getStartKey());
        Assertions.assertEquals(new Key(row, fiName, new Text("a" + Constants.MAX_UNICODE_STRING)), r.getEndKey());
    }
    
    @Test
    public void buildBoundingFiRange_notUpperInclusive_singleCharAZ_test() {
        DatawaveFieldIndexFilterIteratorJexl iteratorJexl = DatawaveFieldIndexFilterIteratorJexl.builder().upperInclusive(false).lowerInclusive(true)
                        .withMaxRangeSplit(1).withFieldName("FIELD").withFieldValue("a").withUpperBound("z").withIvaratorCacheDirs(cacheDirs).build();
        
        Text row = new Text("row");
        Text fiName = new Text("fi" + Constants.NULL + "FIELD");
        Text fieldValue = new Text("a");
        Text fieldValueNullAppended = new Text("a" + Constants.NULL);
        
        List<Range> ranges = iteratorJexl.buildBoundingFiRanges(row, fiName, fieldValue);
        
        Assertions.assertNotEquals(null, ranges);
        Assertions.assertEquals(1, ranges.size());
        Range r = ranges.get(0);
        
        // note that the end key is expected to be inclusive even though upperInclusive is set to false because the value has been decremented by one
        Assertions.assertTrue(r.isStartKeyInclusive());
        Assertions.assertTrue(r.isEndKeyInclusive());
        Assertions.assertEquals(new Key(row, fiName, fieldValueNullAppended), r.getStartKey());
        Assertions.assertEquals(new Key(row, fiName, new Text("y" + Constants.MAX_UNICODE_STRING)), r.getEndKey());
    }
    
    @Test
    public void buildBoundingFiRange_notUpperInclusive_multiChar_test() {
        DatawaveFieldIndexFilterIteratorJexl iteratorJexl = DatawaveFieldIndexFilterIteratorJexl.builder().upperInclusive(false).lowerInclusive(true)
                        .withMaxRangeSplit(1).withFieldName("FIELD").withFieldValue("a").withUpperBound("az").withIvaratorCacheDirs(cacheDirs).build();
        
        Text row = new Text("row");
        Text fiName = new Text("fi" + Constants.NULL + "FIELD");
        Text fieldValue = new Text("aa");
        Text fieldValueNullAppended = new Text("aa" + Constants.NULL);
        
        List<Range> ranges = iteratorJexl.buildBoundingFiRanges(row, fiName, fieldValue);
        
        Assertions.assertNotEquals(null, ranges);
        Assertions.assertEquals(1, ranges.size());
        Range r = ranges.get(0);
        
        // note that the end key is expected to be inclusive even though upperInclusive is set to false because the value has been decremented by one
        Assertions.assertTrue(r.isStartKeyInclusive());
        Assertions.assertTrue(r.isEndKeyInclusive());
        Assertions.assertEquals(new Key(row, fiName, fieldValueNullAppended), r.getStartKey());
        Assertions.assertEquals(new Key(row, fiName, new Text("ay" + Constants.MAX_UNICODE_STRING)), r.getEndKey());
    }
    
    @Test
    public void buildBoundingFiRange_notUpperInclusive_multiCharStartSingleEnd_test() {
        DatawaveFieldIndexFilterIteratorJexl iteratorJexl = DatawaveFieldIndexFilterIteratorJexl.builder().upperInclusive(false).lowerInclusive(true)
                        .withMaxRangeSplit(1).withFieldName("FIELD").withFieldValue("a").withUpperBound("z").withIvaratorCacheDirs(cacheDirs).build();
        
        Text row = new Text("row");
        Text fiName = new Text("fi" + Constants.NULL + "FIELD");
        Text fieldValue = new Text("aa");
        Text fieldValueNullAppended = new Text("aa" + Constants.NULL);
        
        List<Range> ranges = iteratorJexl.buildBoundingFiRanges(row, fiName, fieldValue);
        
        Assertions.assertNotEquals(null, ranges);
        Assertions.assertEquals(1, ranges.size());
        Range r = ranges.get(0);
        
        // note that the end key is expected to be inclusive even though upperInclusive is set to false because the value has been decremented by one
        Assertions.assertTrue(r.isStartKeyInclusive());
        Assertions.assertTrue(r.isEndKeyInclusive());
        Assertions.assertEquals(new Key(row, fiName, fieldValueNullAppended), r.getStartKey());
        Assertions.assertEquals(new Key(row, fiName, new Text("y" + Constants.MAX_UNICODE_STRING)), r.getEndKey());
    }
}
