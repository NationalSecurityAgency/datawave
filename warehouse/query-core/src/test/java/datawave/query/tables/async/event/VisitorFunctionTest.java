package datawave.query.tables.async.event;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import datawave.core.query.configuration.QueryData;
import datawave.data.type.LcNoDiacriticsType;
import datawave.microservice.query.Query;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.tables.SessionOptions;
import datawave.query.tables.async.ScannerChunk;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.query.util.TypeMetadata;
import datawave.util.TableName;

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
        EasyMock.expect(helper.getIndexedFields(dataTypes)).andReturn(indexedFields).anyTimes();
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
        config.setFinalMaxTermThreshold(2);
        config.setMaxDepthThreshold(2);

        SessionOptions options = new SessionOptions();
        IteratorSetting iteratorSetting = new IteratorSetting(10, "itr", QueryIterator.class);
        iteratorSetting.addOption(QueryOptions.QUERY, "FIELD1 == 'a'");
        options.addScanIterator(iteratorSetting);

        // @formatter:off
        QueryData qd = new QueryData()
                .withTableName(TableName.SHARD)
                .withQuery("FIELD1 == 'a'")
                .withRanges(Collections.singleton(new Range("20210101_0", "20210101_0")))
                .withSettings(Collections.singletonList(iteratorSetting));
        // @formatter:on
        ScannerChunk chunk = new ScannerChunk(options, qd.getRanges(), qd);

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
        config.setFinalMaxTermThreshold(1);
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

        // @formatter:off
        QueryData qd = new QueryData()
                .withTableName(TableName.SHARD)
                .withQuery(query)
                .withRanges(Collections.singleton(new Range("20210101_0", "20210101_0")))
                .withSettings(Collections.singletonList(iteratorSetting));
        // @formatter:on

        ScannerChunk chunk = new ScannerChunk(options, qd.getRanges(), qd);

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
    public void overIvaratorTermThresholdTest() throws IOException, TableNotFoundException, URISyntaxException {
        setupExpects();

        config.setCleanupShardsAndDaysQueryHints(false);
        config.setBypassExecutabilityCheck(true);
        config.setSerializeQueryIterator(false);

        Query mockQuery = createMock(Query.class);
        config.setQuery(mockQuery);
        EasyMock.expect(mockQuery.getId()).andReturn(new UUID(0, 0)).anyTimes();
        EasyMock.expect(mockQuery.duplicate("testQuery1")).andReturn(mockQuery).anyTimes();

        // set thresholds
        config.setFinalMaxTermThreshold(10);
        config.setMaxDepthThreshold(10);
        config.setMaxOrExpansionFstThreshold(100);
        config.setMaxOrExpansionThreshold(20);
        config.setMaxOrRangeThreshold(20);
        config.setMaxRangesPerRangeIvarator(50);
        config.setMaxOrRangeThreshold(20);
        config.setMaxIvaratorTerms(2);

        SessionOptions options = new SessionOptions();
        IteratorSetting iteratorSetting = new IteratorSetting(10, "itr", QueryIterator.class);
        String query = "((_Value_ = true) && (FIELD1 =~ 'a.*')) || ((_List_ = true) && (FIELD2 == 'b'))";
        iteratorSetting.addOption(QueryOptions.QUERY, query);
        options.addScanIterator(iteratorSetting);

        // @formatter:off
        QueryData qd = new QueryData()
                .withTableName(TableName.SHARD)
                .withQuery(query)
                .withRanges(Collections.singleton(new Range("20210101_0", "20210101_0")))
                .withSettings(Collections.singletonList(iteratorSetting));
        // @formatter:on
        ScannerChunk chunk = new ScannerChunk(options, Collections.singleton(new Range("20210101_0", "20210101_0")), qd);

        replayAll();

        function = new VisitorFunction(config, helper);
        try {
            function.apply(chunk);
        } catch (Exception e) {
            Assert.fail("Expected the ivarator threshold to pass");
        }

        verifyAll();

        config.setMaxIvaratorTerms(1);

        function = new VisitorFunction(config, helper);
        function.apply(chunk);
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
        config.setFinalMaxTermThreshold(1);
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

        // @formatter:off
        QueryData qd = new QueryData()
                .withTableName(TableName.SHARD)
                .withQuery(query)
                .withRanges(Collections.singleton(new Range("20210101_0", "20210101_0")))
                .withSettings(Collections.singletonList(iteratorSetting));
        // @formatter:on
        ScannerChunk chunk = new ScannerChunk(options, qd.getRanges(), qd);

        replayAll();

        function = new VisitorFunction(config, helper);
        function.apply(chunk);
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
        config.setFinalMaxTermThreshold(5);
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

        // @formatter:off
        QueryData qd = new QueryData()
                .withTableName(TableName.SHARD)
                .withQuery(query)
                .withRanges(Collections.singleton(new Range("20210101_0", "20210101_0")))
                .withSettings(Collections.singletonList(iteratorSetting));
        // @formatter:on
        ScannerChunk chunk = new ScannerChunk(options, qd.getRanges(), qd);

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
        config.setFinalMaxTermThreshold(1);
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

        // @formatter:off
        QueryData qd = new QueryData()
                .withTableName(TableName.SHARD)
                .withQuery(query)
                .withRanges(Collections.singleton(new Range("20210101_0", "20210101_0")))
                .withSettings(Collections.singletonList(iteratorSetting));
        // @formatter:on
        ScannerChunk chunk = new ScannerChunk(options, qd.getRanges(), qd);

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

    @Test
    public void testPruneIvaratorConfigs() throws Exception {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        MetadataHelper helper = new MockMetadataHelper();
        VisitorFunction function = new VisitorFunction(config, helper);

        // this query does NOT require an Ivarator
        String query = "FOO == 'bar'";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);

        IteratorSetting settings = new IteratorSetting(20, "name", QueryIterator.class);
        settings.addOption(QueryOptions.QUERY, query);
        settings.addOption(QueryOptions.IVARATOR_NUM_RETRIES, "3");

        // assert initial state
        Set<String> keys = settings.getOptions().keySet();
        Assert.assertTrue(keys.contains(QueryOptions.QUERY));
        Assert.assertTrue(keys.contains(QueryOptions.IVARATOR_NUM_RETRIES));

        function.pruneIvaratorConfigs(script, settings);

        // verify ivarator config was pruned
        keys = settings.getOptions().keySet();
        Assert.assertTrue(keys.contains(QueryOptions.QUERY));
        Assert.assertFalse(keys.contains(QueryOptions.IVARATOR_NUM_RETRIES));
    }

    @Test
    public void testIvaratorConfigNotPruned() throws Exception {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        MetadataHelper helper = new MockMetadataHelper();
        VisitorFunction function = new VisitorFunction(config, helper);

        // this query DOES require an Ivarator
        String query = "((_Value_ = true) && (FOO == 'bar'))";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);

        IteratorSetting settings = new IteratorSetting(20, "name", QueryIterator.class);
        settings.addOption(QueryOptions.QUERY, query);
        settings.addOption(QueryOptions.IVARATOR_NUM_RETRIES, "3");

        function.pruneIvaratorConfigs(script, settings);

        // verify ivarator config were not pruned
        Set<String> keys = settings.getOptions().keySet();
        Assert.assertTrue(keys.contains(QueryOptions.QUERY));
        Assert.assertTrue(keys.contains(QueryOptions.IVARATOR_NUM_RETRIES));
    }

    @Test
    public void testPruneEmptyIteratorOptions() throws Exception {
        ShardQueryConfiguration cfg = new ShardQueryConfiguration();
        MetadataHelper hlpr = new MockMetadataHelper();
        VisitorFunction function = new VisitorFunction(cfg, hlpr);

        IteratorSetting settings = new IteratorSetting(10, "itr", QueryIterator.class);
        settings.addOption(QueryOptions.QUERY, "FOO == 'bar'");
        settings.addOption(QueryOptions.COMPOSITE_FIELDS, "");

        // assert initial state
        Set<String> keys = settings.getOptions().keySet();
        Assert.assertTrue(keys.contains(QueryOptions.QUERY));
        Assert.assertTrue(keys.contains(QueryOptions.COMPOSITE_FIELDS));

        function.pruneEmptyOptions(settings);

        // assert option with empty value was pruned
        keys = settings.getOptions().keySet();
        Assert.assertTrue(keys.contains(QueryOptions.QUERY));
        Assert.assertFalse(keys.contains(QueryOptions.COMPOSITE_FIELDS));
    }

    @Test
    public void testTypeMetadataReductionViaIncludeFields() throws Exception {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        MockMetadataHelper helper = new MockMetadataHelper();
        VisitorFunction function = new VisitorFunction(config, helper);

        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("FIELD_A == 'a'");
        IteratorSetting settings = new IteratorSetting(10, "itr", QueryIterator.class);
        loadSettings(settings);

        // add include fields
        settings.addOption(QueryOptions.PROJECTION_FIELDS, "FIELD_A,FIELD_B");

        function.reduceTypeMetadata(script, settings);

        Map<String,String> options = settings.getOptions();
        Assert.assertTrue(options.containsKey(QueryOptions.TYPE_METADATA));

        String option = options.get(QueryOptions.TYPE_METADATA);
        Assert.assertTrue(StringUtils.isNotBlank(option));

        TypeMetadata metadata = new TypeMetadata(option);
        Assert.assertEquals(Set.of("FIELD_A", "FIELD_B"), metadata.keySet());
    }

    @Test
    public void testTypeMetadataReductionViaExcludeFields() throws Exception {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        MockMetadataHelper helper = new MockMetadataHelper();
        VisitorFunction function = new VisitorFunction(config, helper);

        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("FIELD_A == 'a'");
        IteratorSetting settings = new IteratorSetting(10, "itr", QueryIterator.class);
        loadSettings(settings);

        // add exclude fields
        settings.addOption(QueryOptions.DISALLOWLISTED_FIELDS, "FIELD_B");

        function.reduceTypeMetadata(script, settings);

        Map<String,String> options = settings.getOptions();
        Assert.assertTrue(options.containsKey(QueryOptions.TYPE_METADATA));

        String option = options.get(QueryOptions.TYPE_METADATA);
        Assert.assertTrue(StringUtils.isNotBlank(option));

        TypeMetadata metadata = new TypeMetadata(option);
        Assert.assertEquals(Set.of("FIELD_A", "FIELD_C"), metadata.keySet());
    }

    private void loadSettings(IteratorSetting settings) {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("FIELD_A", "type-a", LcNoDiacriticsType.class.getSimpleName());
        metadata.put("FIELD_B", "type-a", LcNoDiacriticsType.class.getSimpleName());
        metadata.put("FIELD_C", "type-a", LcNoDiacriticsType.class.getSimpleName());
        settings.addOption(QueryOptions.TYPE_METADATA, metadata.toString());
    }
}
