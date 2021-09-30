package datawave.query.tables.async.event;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.tables.SessionOptions;
import datawave.query.tables.async.ScannerChunk;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class VisitorFunctionTest extends EasyMockSupport {
    private VisitorFunction function;
    
    private ShardQueryConfiguration config;
    private MetadataHelper helper;
    
    @Before
    public void setup() {
        config = new ShardQueryConfiguration();
        helper = createMock(MetadataHelper.class);
    }
    
    private void setupExpects() throws TableNotFoundException, IOException, URISyntaxException {
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("FIELD1");
        indexedFields.add("FIELD2");
        
        Set<String> dataTypes = new HashSet<>();
        dataTypes.add("dataType1");
        
        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        Configuration hadoopConf = new Configuration();
        hadoopConf.addResource(hadoopConfig);
        
        File tmpFile = File.createTempFile("Ivarator", ".cache");
        File tmpDir = new File(tmpFile.getAbsoluteFile() + File.separator);
        tmpFile.delete();
        tmpDir.mkdir();
        
        URI hdfsCacheURI = new URI("file:" + tmpDir.getPath());
        FileSystem fs = FileSystem.get(hdfsCacheURI, hadoopConf);
        
        config.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());
        config.setIvaratorFstHdfsBaseURIs("file:////tmp/");
        config.setIndexedFields(indexedFields);
        config.setDatatypeFilter(dataTypes);
        config.setIvaratorCacheDirConfigs(Collections.singletonList(new IvaratorCacheDirConfig(hdfsCacheURI.toString())));
        EasyMock.expect(helper.getIndexOnlyFields(dataTypes)).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getNonEventFields(dataTypes)).andReturn(Collections.emptySet()).anyTimes();
    }
    
    @Test
    public void underTermThresholdTest() throws IOException, TableNotFoundException, URISyntaxException {
        setupExpects();
        
        // test specific expects
        config.setCleanupShardsAndDaysQueryHints(false);
        config.setBypassExecutabilityCheck(true);
        config.setSerializeQueryIterator(false);
        
        Query mockQuery = createMock(Query.class);
        config.setQuery(mockQuery);
        EasyMock.expect(mockQuery.getId()).andReturn(new UUID(0, 0)).anyTimes();
        
        // set thresholds
        config.setMaxTermThreshold(2);
        config.setMaxDepthThreshold(2);
        
        SessionOptions options = new SessionOptions();
        IteratorSetting iteratorSetting = new IteratorSetting(10, "itr", QueryIterator.class);
        iteratorSetting.addOption(QueryOptions.QUERY, "FIELD1 == 'a'");
        options.addScanIterator(iteratorSetting);
        
        ScannerChunk chunk = new ScannerChunk(options, Collections.singleton(new Range("20210101_0", "20210101_0")));
        
        replayAll();
        
        function = new VisitorFunction(config, helper);
        function.apply(chunk);
        
        verifyAll();
    }
    
    @Test
    public void overTermThresholdTest() throws IOException, TableNotFoundException, URISyntaxException {
        setupExpects();
        
        config.setCleanupShardsAndDaysQueryHints(false);
        config.setBypassExecutabilityCheck(true);
        config.setSerializeQueryIterator(false);
        
        Query mockQuery = createMock(Query.class);
        config.setQuery(mockQuery);
        EasyMock.expect(mockQuery.getId()).andReturn(new UUID(0, 0)).anyTimes();
        EasyMock.expect(mockQuery.duplicate("testQuery1")).andReturn(mockQuery).anyTimes();
        
        // set thresholds
        config.setMaxTermThreshold(1);
        config.setMaxDepthThreshold(10);
        config.setMaxOrExpansionFstThreshold(100);
        config.setMaxOrExpansionThreshold(20);
        config.setMaxOrRangeThreshold(2);
        config.setMaxRangesPerRangeIvarator(50);
        config.setMaxOrRangeThreshold(2);
        
        SessionOptions options = new SessionOptions();
        IteratorSetting iteratorSetting = new IteratorSetting(10, "itr", QueryIterator.class);
        String query = "FIELD1 == 'a' || FIELD1 == 'b'";
        iteratorSetting.addOption(QueryOptions.QUERY, query);
        options.addScanIterator(iteratorSetting);
        
        ScannerChunk chunk = new ScannerChunk(options, Collections.singleton(new Range("20210101_0", "20210101_0")));
        
        replayAll();
        
        function = new VisitorFunction(config, helper);
        ScannerChunk updatedChunk = function.apply(chunk);
        
        verifyAll();
        
        Assert.assertNotEquals(chunk, updatedChunk);
        String updatedQuery = updatedChunk.getOptions().getIterators().iterator().next().getOptions().get(QueryOptions.QUERY);
        Assert.assertNotEquals(query, updatedQuery);
        Assert.assertTrue(updatedQuery, updatedQuery.contains("_List_"));
        Assert.assertTrue(updatedQuery, updatedQuery.contains("field = 'FIELD1'"));
        Assert.assertTrue(updatedQuery, updatedQuery.contains("values\":[\"a\",\"b\"]"));
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void overTermThresholdCantReduceTest() throws IOException, TableNotFoundException, URISyntaxException {
        setupExpects();
        
        config.setCleanupShardsAndDaysQueryHints(false);
        config.setBypassExecutabilityCheck(true);
        config.setSerializeQueryIterator(false);
        
        Query mockQuery = createMock(Query.class);
        config.setQuery(mockQuery);
        EasyMock.expect(mockQuery.getId()).andReturn(new UUID(0, 0)).anyTimes();
        EasyMock.expect(mockQuery.getQueryName()).andReturn("testQuery1").anyTimes();
        EasyMock.expect(mockQuery.duplicate("testQuery1")).andReturn(mockQuery).anyTimes();
        
        // set thresholds
        config.setMaxTermThreshold(1);
        config.setMaxDepthThreshold(10);
        config.setMaxOrExpansionFstThreshold(100);
        config.setMaxOrExpansionThreshold(20);
        config.setMaxOrRangeThreshold(2);
        config.setMaxRangesPerRangeIvarator(50);
        config.setMaxOrRangeThreshold(2);
        
        SessionOptions options = new SessionOptions();
        IteratorSetting iteratorSetting = new IteratorSetting(10, "itr", QueryIterator.class);
        String query = "FIELD2 == 'a' || FIELD1 == 'b'";
        iteratorSetting.addOption(QueryOptions.QUERY, query);
        options.addScanIterator(iteratorSetting);
        
        ScannerChunk chunk = new ScannerChunk(options, Collections.singleton(new Range("20210101_0", "20210101_0")));
        
        replayAll();
        
        function = new VisitorFunction(config, helper);
        ScannerChunk updatedChunk = function.apply(chunk);
        
        verifyAll();
        
        Assert.assertNotEquals(chunk, updatedChunk);
        String updatedQuery = updatedChunk.getOptions().getIterators().iterator().next().getOptions().get(QueryOptions.QUERY);
        Assert.assertNotEquals(query, updatedQuery);
        Assert.assertTrue(updatedQuery, updatedQuery.contains("_List_"));
        Assert.assertTrue(updatedQuery, updatedQuery.contains("field = 'FIELD1'"));
        Assert.assertTrue(updatedQuery, updatedQuery.contains("values\":[\"a\",\"b\"]"));
    }
    
    @Test
    public void overTermThresholdAfterFirstReductionOverrideSecondTest() throws IOException, TableNotFoundException, URISyntaxException {
        setupExpects();
        
        config.setCleanupShardsAndDaysQueryHints(false);
        config.setBypassExecutabilityCheck(true);
        config.setSerializeQueryIterator(false);
        
        Query mockQuery = createMock(Query.class);
        config.setQuery(mockQuery);
        EasyMock.expect(mockQuery.getId()).andReturn(new UUID(0, 0)).anyTimes();
        EasyMock.expect(mockQuery.getQueryName()).andReturn("testQuery1").anyTimes();
        EasyMock.expect(mockQuery.duplicate("testQuery1")).andReturn(mockQuery).anyTimes();
        
        // set thresholds
        config.setMaxTermThreshold(5);
        config.setMaxDepthThreshold(20);
        config.setMaxOrExpansionFstThreshold(100);
        config.setMaxOrExpansionThreshold(5);
        config.setMaxOrRangeThreshold(2);
        config.setMaxRangesPerRangeIvarator(50);
        config.setMaxOrRangeThreshold(2);
        
        SessionOptions options = new SessionOptions();
        IteratorSetting iteratorSetting = new IteratorSetting(10, "itr", QueryIterator.class);
        String query = "(FIELD1 == 'a' || FIELD1 == 'b' || FIELD1 == 'c' || FIELD1 == 'd' || FIELD1 == 'e') && (FIELD1 == 'x' || FIELD1 == 'y' || FIELD2 == 'a' || FIELD2 == 'b' || FIELD2 == 'c' || FIELD2 == 'd')";
        iteratorSetting.addOption(QueryOptions.QUERY, query);
        options.addScanIterator(iteratorSetting);
        
        ScannerChunk chunk = new ScannerChunk(options, Collections.singleton(new Range("20210101_0", "20210101_0")));
        
        replayAll();
        
        function = new VisitorFunction(config, helper);
        ScannerChunk updatedChunk = function.apply(chunk);
        
        verifyAll();
        
        Assert.assertNotEquals(chunk, updatedChunk);
        String updatedQuery = updatedChunk.getOptions().getIterators().iterator().next().getOptions().get(QueryOptions.QUERY);
        Assert.assertNotEquals(query, updatedQuery);
        Assert.assertTrue(updatedQuery, updatedQuery.contains("_List_"));
        Assert.assertTrue(updatedQuery, updatedQuery.contains("field = 'FIELD1'"));
        Assert.assertTrue(updatedQuery, updatedQuery.contains("values\":[\"a\",\"b\",\"c\",\"d\",\"e\"]"));
        Assert.assertTrue(updatedQuery, updatedQuery.contains("FIELD1 == 'x' || FIELD1 == 'y'"));
        Assert.assertTrue(updatedQuery, updatedQuery.contains("&& (field = 'FIELD2') && (params = '{\"values\":[\"a\",\"b\",\"c\",\"d\"]}')"));
    }
    
    @Test
    public void rangeOverTermThresholdTest() throws IOException, TableNotFoundException, URISyntaxException {
        setupExpects();
        
        config.setCleanupShardsAndDaysQueryHints(false);
        config.setBypassExecutabilityCheck(true);
        config.setSerializeQueryIterator(false);
        
        Query mockQuery = createMock(Query.class);
        config.setQuery(mockQuery);
        EasyMock.expect(mockQuery.getId()).andReturn(new UUID(0, 0)).anyTimes();
        EasyMock.expect(mockQuery.duplicate("testQuery1")).andReturn(mockQuery).anyTimes();
        
        // set thresholds
        config.setMaxTermThreshold(1);
        config.setMaxDepthThreshold(10);
        config.setMaxOrExpansionFstThreshold(100);
        config.setMaxOrExpansionThreshold(20);
        config.setMaxOrRangeThreshold(2);
        config.setMaxRangesPerRangeIvarator(50);
        config.setMaxOrRangeThreshold(2);
        
        SessionOptions options = new SessionOptions();
        IteratorSetting iteratorSetting = new IteratorSetting(10, "itr", QueryIterator.class);
        String query = "((_Bounded_ = true) && (FIELD1 > 'a' && FIELD1 < 'y')) || ((_Bounded_ = true) && (FIELD1 > 'c' && FIELD1 < 'z'))";
        iteratorSetting.addOption(QueryOptions.QUERY, query);
        options.addScanIterator(iteratorSetting);
        
        ScannerChunk chunk = new ScannerChunk(options, Collections.singleton(new Range("20210101_0", "20210101_0")));
        
        replayAll();
        
        function = new VisitorFunction(config, helper);
        ScannerChunk updatedChunk = function.apply(chunk);
        
        verifyAll();
        
        Assert.assertNotEquals(chunk, updatedChunk);
        String updatedQuery = updatedChunk.getOptions().getIterators().iterator().next().getOptions().get(QueryOptions.QUERY);
        Assert.assertNotEquals(query, updatedQuery);
        Assert.assertTrue(updatedQuery, updatedQuery.contains("_List_"));
        Assert.assertTrue(updatedQuery, updatedQuery.contains("field = 'FIELD1'"));
        Assert.assertTrue(updatedQuery, updatedQuery.contains("ranges\":[[\"(a\",\"z)\"]"));
    }
}
