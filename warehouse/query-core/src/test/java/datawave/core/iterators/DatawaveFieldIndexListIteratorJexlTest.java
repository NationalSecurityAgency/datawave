package datawave.core.iterators;

import datawave.query.IvaratorReloadTest;
import datawave.query.iterator.ivarator.IvaratorCacheDir;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DatawaveFieldIndexListIteratorJexlTest {
    
    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    private List<IvaratorCacheDir> cacheDirs;
    
    @Before
    public void setup() throws IOException {
        File cacheDir = temporaryFolder.newFolder();
        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig(cacheDir.toURI().toString());
        FileSystem fs = FileSystem.get(cacheDir.toURI(), new Configuration());
        File queryDirFile = new File(cacheDir, "query");
        queryDirFile.deleteOnExit();
        Assert.assertTrue(queryDirFile.mkdirs());
        String queryDir = queryDirFile.toURI().toString();
        cacheDirs = Collections.singletonList(new IvaratorCacheDir(config, fs, queryDir));
    }
    
    @Test
    public void testBuildingOfBoundedRanges() {
        DatawaveFieldIndexListIteratorJexl.Builder<?> builder = DatawaveFieldIndexListIteratorJexl.builder();
        builder.withFieldName("FIELDNAME");
        builder.withIvaratorCacheDirs(cacheDirs);
        DatawaveFieldIndexListIteratorJexl iter = builder.build();
        
        Text row = new Text("20200314_1");
        Text fiName = new Text("fi\u0000FIELDNAME");
        Text value = new Text("fieldvalue");
        Range range = iter.buildBoundingRange(row, fiName, value);
        
        Key start = new Key(new Text("20200314_1"), new Text("fi\u0000FIELDNAME"), new Text("fieldvalue\0"));
        Key end = new Key(new Text("20200314_1"), new Text("fi\u0000FIELDNAME"), new Text("fieldvalue\1"));
        Range expected = new Range(start, true, end, false);
        
        assertEquals(expected, range);
    }
    
    @Test
    public void testBuildingOfBoundedRangesWithinADocumentSpecificContext() throws Exception {
        DatawaveFieldIndexListIteratorJexl.Builder<?> builder = DatawaveFieldIndexListIteratorJexl.builder();
        builder.withFieldName("FIELDNAME");
        builder.withIvaratorCacheDirs(cacheDirs);
        builder.withIvaratorSourcePool(IvaratorReloadTest.createIvaratorSourcePool(1));
        DatawaveFieldIndexListIteratorJexl iter = builder.build();
        
        // Simulate a seek to a document specific range
        Key start = new Key(new Text("20200314_1"), new Text("datatype\0uid0"));
        Key end = start.followingKey(PartialKey.ROW_COLFAM);
        Range seekRange = new Range(start, true, end, false);
        iter.seek(seekRange, DatawaveFieldIndexCachingIteratorJexl.EMPTY_COL_FAMS, true);
        
        Text row = new Text("20200314_1");
        Text fiName = new Text("fi\u0000FIELDNAME");
        Range range = iter.buildBoundingRange(row, fiName, "fieldvalue", "datatype", "uid0");
        
        start = new Key(new Text("20200314_1"), new Text("fi\u0000FIELDNAME"), new Text("fieldvalue\0datatype\0uid0"));
        end = new Key(new Text("20200314_1"), new Text("fi\u0000FIELDNAME"), new Text("fieldvalue\0datatype\0uid0\0"));
        Range expected = new Range(start, true, end, false);
        
        assertEquals(expected, range);
    }
}
