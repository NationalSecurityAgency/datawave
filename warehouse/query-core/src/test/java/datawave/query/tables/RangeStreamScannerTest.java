package datawave.query.tables;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.ingest.protobuf.Uid;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.index.lookup.CreateUidsIterator;
import datawave.query.index.lookup.DataTypeFilter;
import datawave.query.index.lookup.EntryParser;
import datawave.query.index.lookup.IndexInfo;
import datawave.query.index.lookup.ScannerStream;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.util.QueryScannerHelper;
import datawave.query.util.Tuple2;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.hadoop.io.Text;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static datawave.util.TableName.SHARD_INDEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for the {@link RangeStreamScanner}
 */
public class RangeStreamScannerTest {
    
    private static Connector connector;
    
    private static ScannerFactory scannerFactory;
    
    private static ShardQueryConfiguration config;
    
    // Helper method that builds an accumulo mutation for an index entry.
    // Format: bar FOO:20190314\u0000datatype1:doc1,doc2,doc3
    private static Mutation buildMutation(String fieldName, String fieldValue, String shard, String datatype, String colViz, String... docIds) {
        List<String> docIdList = Lists.newArrayList(docIds);
        return buildMutation(fieldName, fieldValue, shard, datatype, colViz, docIdList);
    }
    
    // Helper method that builds an accumulo mutation for an index entry.
    // Format: bar FOO:20190314\u0000datatype1:doc1,doc2,doc3
    private static Mutation buildMutation(String fieldName, String fieldValue, String shard, String datatype, String colViz, List<String> docIds) {
        Text columnFamily = new Text(fieldName);
        Text columnQualifier = new Text(shard + '\u0000' + datatype);
        ColumnVisibility columnVisibility = new ColumnVisibility(colViz);
        
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addAllUID(docIds);
        builder.setIGNORE(false);
        builder.setCOUNT(docIds.size());
        Uid.List list = builder.build();
        
        Value value = new Value(list.toByteArray());
        
        Mutation mutation = new Mutation(fieldValue);
        mutation.put(columnFamily, columnQualifier, columnVisibility, value);
        return mutation;
    }
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        InMemoryInstance instance = new InMemoryInstance();
        connector = instance.getConnector("", new PasswordToken(new byte[0]));
        connector.tableOperations().create(SHARD_INDEX);
        
        scannerFactory = new ScannerFactory(connector, 1);
        
        BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1024L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        BatchWriter bw = connector.createBatchWriter(SHARD_INDEX, bwConfig);
        
        // FOO == 'bar' hits day 20190314 with 1 shard, each shard has 2 document ids.
        // This remains under the shards/day limit and under the documents/shard limit.
        bw.addMutation(buildMutation("FOO", "bar", "20190314", "datatype1", "A", "doc1", "doc2"));
        
        // FOO == 'baz' hits day 20190317 with 15 shards, each shard has 2 document ids.
        // This exceeds the shards/day limit and remains under the documents/shard limit.
        for (int ii = 0; ii < 15; ii++) {
            String shard = "20190317_" + ii;
            bw.addMutation(buildMutation("FOO", "baz", shard, "datatype1", "A", "doc1", "doc2"));
        }
        
        // FOO == 'boo' hits day 20190319 with 8 shards, each shard has 15 document ids.
        // This remains under the shards/day limit and under the documents/shard limit.
        List<String> docIds = new ArrayList<>(15);
        for (int ii = 0; ii < 15; ii++) {
            docIds.add("docId" + ii);
        }
        for (int jj = 0; jj < 8; jj++) {
            String shard = "20190319_" + jj;
            bw.addMutation(buildMutation("FOO", "boo", shard, "datatype1", "A", docIds));
        }
        
        // FOO == 'boohoo' hits day 20190319 with 15 shards, each shard has 25 document ids.
        // This exceeds the shards/day limit and exceeds the documents/shard limit.
        docIds = new ArrayList<>(25);
        for (int ii = 0; ii < 25; ii++) {
            docIds.add("docId" + ii);
        }
        for (int jj = 0; jj < 15; jj++) {
            String shard = "20190323_" + jj;
            bw.addMutation(buildMutation("FOO", "boohoo", shard, "datatype1", "A", docIds));
        }
        
        // Flush mutations and close the writer.
        bw.flush();
        bw.close();
        
        // Setup ShardQueryConfiguration
        config = new ShardQueryConfiguration();
        config.setDatatypeFilter(Sets.newHashSet("datatype1", "datatype2"));
        
        // Set begin/end date for query
        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));
        
        // Set auths for query;
        Authorizations auth1 = new Authorizations("A", "B", "C");
        Authorizations auth2 = new Authorizations("A", "D", "E");
        Authorizations auth3 = new Authorizations("A", "F", "G");
        Set<Authorizations> auths = Sets.newHashSet(auth1, auth2, auth3);
        config.setAuthorizations(auths);
        
        // Build and set datatypes
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        dataTypes.putAll("NUM", Sets.newHashSet(new NumberType()));
        
        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);
    }
    
    // Helper method largely copied from RangeStream class.
    private Range rangeForTerm(String term, String field, ShardQueryConfiguration config) {
        String startDate = DateHelper.format(config.getBeginDate());
        String endDate = DateHelper.format(config.getEndDate());
        return new Range(new Key(term, field, startDate + '_'), true, new Key(term, field, endDate + '_' + '\uffff'), false);
    }
    
    /**
     * Replicates the section of the {@link datawave.query.index.lookup.RangeStream#visit(ASTEQNode, Object)} method that builds a rangeStreamScanner.
     *
     * This version creates a simple iterator that will not alter the index records coming back in any way.
     *
     * @param fieldName
     *            - field name, like "FOO" in "FOO == 'bar'"
     * @param fieldValue
     *            - field value, like "bar" in "FOO == 'bar'"
     * @return a configured RangeStreamScanner
     */
    private RangeStreamScanner buildRangeStreamScanner(String fieldName, String fieldValue) throws Exception {
        
        String queryString = fieldName + "=='" + fieldValue + "'";
        
        // Build the executors
        int maxLookup = (int) Math.max(Math.ceil(config.getNumIndexLookupThreads()), 1);
        BlockingQueue<Runnable> runnables = new LinkedBlockingDeque<>();
        int executeLookupMin = Math.max(maxLookup / 2, 1);
        ExecutorService streamExecutor = new ThreadPoolExecutor(executeLookupMin, maxLookup, 100, TimeUnit.MILLISECONDS, runnables);
        
        int priority = 50; // Iterator priority
        
        // Build options for RangeStreamScanner
        SessionOptions options = new SessionOptions();
        options.fetchColumnFamily(new Text(fieldName));
        
        // Set iterator datatype filter
        IteratorSetting is = new IteratorSetting(priority++, DataTypeFilter.class);
        is.addOption(DataTypeFilter.TYPES, config.getDatatypeFilterAsString());
        options.addScanIterator(is);
        
        // Set iterator option. Do not collapse uids into shard ranges, just pass results back.
        final IteratorSetting uidSetting = new IteratorSetting(priority++, CreateUidsIterator.class);
        uidSetting.addOption(CreateUidsIterator.COLLAPSE_UIDS, Boolean.valueOf(false).toString());
        options.addScanIterator(uidSetting);
        
        // Set query option
        options.addScanIterator(QueryScannerHelper.getQueryInfoIterator(config.getQuery(), false, queryString));
        
        // Build RangeStreamScanner
        RangeStreamScanner rangeStreamScanner = scannerFactory.newRangeScanner(config.getIndexTableName(), config.getAuthorizations(), config.getQuery(),
                        config.getShardsPerDayThreshold());
        rangeStreamScanner.setMaxResults(config.getMaxIndexBatchSize());
        rangeStreamScanner.setExecutor(streamExecutor);
        rangeStreamScanner.setRanges(Collections.singleton(rangeForTerm(fieldValue, fieldName, config))).setOptions(options);
        
        return rangeStreamScanner;
    }
    
    /**
     * Make sure a simple scan returns correctly. FOO == 'bar' hits day 20190314 with 1 shard, each shard has 2 document ids.
     */
    @Test
    public void testTheSimplestOfScans() throws Exception {
        
        // Components that define the query: "FOO == 'bar'"
        String fieldName = "FOO";
        String fieldValue = "bar";
        ASTEQNode eqNode = (ASTEQNode) JexlNodeFactory.buildEQNode(fieldName, fieldValue);
        
        // Construct a ScannerStream from RangeStreamScanner, iterator, entry parser.
        RangeStreamScanner rangeStreamScanner = buildRangeStreamScanner(fieldName, fieldValue);
        EntryParser entryParser = new EntryParser(eqNode, fieldName, fieldValue, config.getIndexedFields());
        Iterator<Tuple2<String,IndexInfo>> iterator = Iterators.transform(rangeStreamScanner, entryParser);
        ScannerStream scannerStream = ScannerStream.initialized(iterator, eqNode);
        
        // Assert the iterator correctly iterates over the iterables without irritating the unit test.
        assertTrue(scannerStream.hasNext());
        int shardCount = 0;
        int documentCount = 0;
        while (scannerStream.hasNext()) {
            Tuple2<String,IndexInfo> entry = scannerStream.next();
            assertEquals("Expected shard to start with '20190314' but was: " + entry.first(), "20190314", entry.first());
            assertEquals(2, entry.second().count());
            shardCount++;
            documentCount += entry.second().count();
        }
        assertEquals(1, shardCount);
        assertEquals(2, documentCount);
        assertFalse(scannerStream.hasNext());
    }
    
    /**
     * FOO == 'baz' hits day 20190317 with 15 shards, each shard has 2 document ids.
     */
    @Test
    public void testExceedShardDayThreshold() throws Exception {
        
        // Components that define the query: "FOO == 'baz'"
        String fieldName = "FOO";
        String fieldValue = "baz";
        ASTEQNode eqNode = (ASTEQNode) JexlNodeFactory.buildEQNode(fieldName, fieldValue);
        
        // Construct a ScannerStream from RangeStreamScanner, iterator, entry parser.
        RangeStreamScanner rangeStreamScanner = buildRangeStreamScanner(fieldName, fieldValue);
        EntryParser entryParser = new EntryParser(eqNode, fieldName, fieldValue, config.getIndexedFields());
        Iterator<Tuple2<String,IndexInfo>> iterator = Iterators.transform(rangeStreamScanner, entryParser);
        ScannerStream scannerStream = ScannerStream.initialized(iterator, eqNode);
        
        // Assert the iterator correctly iterates over the iterables without irritating the unit test.
        assertTrue(scannerStream.hasNext());
        int shardCount = 0;
        int documentCount = 0;
        while (scannerStream.hasNext()) {
            Tuple2<String,IndexInfo> entry = scannerStream.next();
            assertTrue("Expected shard to start with '20190317_' but was: " + entry.first(), entry.first().startsWith("20190317_"));
            assertEquals(2, entry.second().count());
            shardCount++;
            documentCount += entry.second().count();
            
        }
        assertEquals(15, shardCount);
        assertEquals(30, documentCount);
        assertFalse(scannerStream.hasNext());
    }
    
    /**
     * FOO == 'boo' hits day 20190319 with 8 shards, each shard has 15 document ids.
     */
    @Test
    public void testExceedMaxMedianDocumentsPerShard() throws Exception {
        
        // Components that define the query: "FOO == 'boo'"
        String fieldName = "FOO";
        String fieldValue = "boo";
        ASTEQNode eqNode = (ASTEQNode) JexlNodeFactory.buildEQNode(fieldName, fieldValue);
        
        // Construct a ScannerStream from RangeStreamScanner, iterator, entry parser.
        RangeStreamScanner rangeStreamScanner = buildRangeStreamScanner(fieldName, fieldValue);
        EntryParser entryParser = new EntryParser(eqNode, fieldName, fieldValue, config.getIndexedFields());
        Iterator<Tuple2<String,IndexInfo>> iterator = Iterators.transform(rangeStreamScanner, entryParser);
        ScannerStream scannerStream = ScannerStream.initialized(iterator, eqNode);
        
        // Assert the iterator correctly iterates over the iterables without irritating the unit test.
        assertTrue(scannerStream.hasNext());
        int shardCount = 0;
        int documentCount = 0;
        while (scannerStream.hasNext()) {
            Tuple2<String,IndexInfo> entry = scannerStream.next();
            assertTrue("Expected shard to start with '20190319_' but was: " + entry.first(), entry.first().startsWith("20190319_"));
            assertEquals(15, entry.second().count());
            shardCount++;
            documentCount += entry.second().count();
        }
        assertEquals(8, shardCount);
        assertEquals(120, documentCount);
        assertFalse(scannerStream.hasNext());
    }
    
    /**
     * FOO == 'boohoo' hits day 20190319 with 15 shards, each shard has 25 document ids.
     */
    @Test
    public void testExceedShardsPerDayThresholdAndDocumentsPerShardThreshold() throws Exception {
        
        // Components that define the query: "FOO == 'boohoo'"
        String fieldName = "FOO";
        String fieldValue = "boohoo";
        ASTEQNode eqNode = (ASTEQNode) JexlNodeFactory.buildEQNode(fieldName, fieldValue);
        
        // Construct a ScannerStream from RangeStreamScanner, iterator, entry parser.
        RangeStreamScanner rangeStreamScanner = buildRangeStreamScanner(fieldName, fieldValue);
        EntryParser entryParser = new EntryParser(eqNode, fieldName, fieldValue, config.getIndexedFields());
        Iterator<Tuple2<String,IndexInfo>> iterator = Iterators.transform(rangeStreamScanner, entryParser);
        ScannerStream scannerStream = ScannerStream.initialized(iterator, eqNode);
        
        // Assert the iterator correctly iterates over the iterables without irritating the unit test.
        assertTrue(scannerStream.hasNext());
        int shardCount = 0;
        int documentCount = 0;
        while (scannerStream.hasNext()) {
            Tuple2<String,IndexInfo> entry = scannerStream.next();
            assertTrue("Expected shard to start with '20190323' but was: " + entry.first(), entry.first().startsWith("20190323"));
            shardCount++;
            documentCount += entry.second().count();
        }
        // A single range with a count of -1 means the shard ranges were collapsed into a day range.
        assertEquals(1, shardCount);
        assertEquals(-1, documentCount);
        assertFalse(scannerStream.hasNext());
    }
    
    /**
     * Tests that the RangeStreamScanner correctly extracts the date from an accumulo key.
     */
    @Test
    public void testGetDay() throws Exception {
        // Build RangeStreamScanner
        ScannerFactory scanners = new ScannerFactory(connector, 1);
        RangeStreamScanner rangeStreamScanner = scanners.newRangeScanner(config.getIndexTableName(), config.getAuthorizations(), config.getQuery(),
                        config.getShardsPerDayThreshold());
        
        Key key = new Key("row".getBytes(), "cf".getBytes(), "20190314".getBytes());
        String expectedDay = "20190314";
        assertEquals(expectedDay, rangeStreamScanner.getDay(key));
        
        key = new Key("row".getBytes(), "cf".getBytes());
        assertNull(rangeStreamScanner.getDay(key));
    }
}
