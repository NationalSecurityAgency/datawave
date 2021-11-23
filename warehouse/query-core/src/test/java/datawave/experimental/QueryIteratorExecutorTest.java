package datawave.experimental;

import com.google.common.collect.Sets;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.experimental.util.AccumuloUtil;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Document;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.QueryOptions;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.SessionOptions;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.async.ScannerChunk;
import datawave.query.util.AllFieldMetadataHelper;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.util.TableName;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration test for the {@link QueryIteratorExecutor}
 */
public class QueryIteratorExecutorTest {
    
    private static final Logger log = Logger.getLogger(QueryIteratorExecutorTest.class);
    
    private static AccumuloUtil util;
    
    private static final Type<?> numberType = new NumberType();
    
    private final Set<String> aliceUids = util.getAliceUids();
    private final Set<String> bobUids = util.getBobUids();
    private final Set<String> eveUids = util.getEveUids();
    private final Set<String> extraUids = util.getExtraUids();
    private final Set<String> oberonUids = util.getOberonUids();
    
    private final KryoDocumentDeserializer deserializer = new KryoDocumentDeserializer();
    
    private final ExecutorService fieldIndexPool = Executors.newFixedThreadPool(10);
    private final ExecutorService evaluationPool = Executors.newFixedThreadPool(10);
    
    @BeforeClass
    public static void setup() throws Exception {
        util = new AccumuloUtil();
        util.create(QueryIteratorExecutorTest.class.getSimpleName());
        util.loadData();
        
        Logger.getLogger(AllFieldMetadataHelper.class).setLevel(Level.OFF);
        Logger.getLogger(MetadataHelperFactory.class).setLevel(Level.OFF);
    }
    
    @Test
    public void testSimpleEquality() {
        String query = "FIRST_NAME == 'bob'";
        test(query, bobUids);
    }
    
    @Test
    public void testEqualityOfAlice() {
        String query = "FIRST_NAME == 'alice'";
        test(query, aliceUids);
    }
    
    @Test
    public void testIntersection() {
        String query = "FIRST_NAME == 'alice' && FIRST_NAME == 'bob'";
        test(query, Sets.intersection(aliceUids, bobUids));
    }
    
    @Test
    public void testUnion() {
        String query = "FIRST_NAME == 'alice' || FIRST_NAME == 'bob'";
        test(query, Sets.union(aliceUids, bobUids));
    }
    
    @Test
    public void testIntersectionOfIndexOnlyFields() {
        String query = "(TOK == 'brute' && TOK == 'forced')";
        test(query, Collections.singleton(util.getUid4()));
    }
    
    @Test
    public void testUnionOfIndexOnlyFields() {
        String query = "(TOK == 'brute' || TOK == 'forced')";
        test(query, Collections.singleton(util.getUid4()));
    }
    
    @Test
    public void testContentPhraseFunction() {
        String query = "(content:phrase(termOffsetMap,'brute','forced') && (TOK == 'brute' && TOK == 'forced'))";
        test(query, Collections.singleton(util.getUid4()));
    }
    
    @Test
    public void testContentAdjacentFunction() {
        String query = "(content:adjacent(termOffsetMap,'alice','sent') && (TOK == 'alice' && TOK == 'sent'))";
        test(query, Sets.newHashSet(util.getUid0(), util.getUid3()));
    }
    
    @Test
    public void testContentWithinFunction() {
        String query = "(content:within(4, termOffsetMap,'alice','bob') && (TOK == 'alice' && TOK == 'bob'))";
        test(query, Sets.newHashSet(util.getUid2(), util.getUid3()));
    }
    
    @Test
    public void testNotNull() {
        String query = "FIRST_NAME == 'eve' && !(MSG_SIZE == null)";
        // eve is all even, msg size is not null in first five.
        // expected should be uid0, uid2, uid4
        test(query, Sets.newHashSet(util.getUid0(), util.getUid2(), util.getUid4()));
    }
    
    @Test
    public void testNotNull2() {
        // functionally the same query as above test, different form
        String query = "FIRST_NAME == 'eve' && MSG_SIZE != null";
        test(query, Sets.newHashSet(util.getUid0(), util.getUid2(), util.getUid4()));
    }
    
    @Test
    public void testNestedUnionOfNotNulls() {
        String query = "FIRST_NAME == 'eve' && (!(MSG_SIZE == null) || !(EVENT_ONLY == null))";
        test(query, util.getEveUids());
    }
    
    @Test
    public void testUnionOfIncludeRegexFunctions() {
        String query = "FIRST_NAME == 'eve' && !(filter:includeRegex(MSG_SIZE,'.*abc.*') || filter:includeRegex(MSG_SIZE,'.*xyz.*'))";
        test(query, util.getEveUids());
    }
    
    @Ignore
    @Test
    public void testQueryAgainstEventOnlyFullContent() {
        String query = "FIRST_NAME == 'eve' && MSG == 'event only message'";
        test(query, Collections.singleton(util.getUid0()));
    }
    
    @Test
    public void testQueryAgainstEventOnlyTokens() {
        String query = "FIRST_NAME == 'eve' && MSG == 'event' && MSG == 'only' && MSG == 'message'";
        test(query, Collections.singleton(util.getUid0()));
    }
    
    // indexed AND eval only
    
    // indexed AND function
    
    // indexed AND (eval only OR eval only)
    
    // indexed AND (function OR function)
    
    // indexed AND (eval only OR function)
    
    /**
     * Test both a shard range and document range lookup
     *
     * @param query
     *            a query string
     * @param expected
     *            a set of expected document uids
     */
    private void test(String query, Set<String> expected) {
        testExecutorWithShardRange(query, expected);
        testExecutorWithDocRange(query, expected);
        
        // also run using the shard query logic
        testWithShardQueryLogic(query, expected, false, false);
        testWithShardQueryLogic(query, expected, true, false);
        
        testWithShardQueryLogic(query, expected, false, true);
        testWithShardQueryLogic(query, expected, true, true);
    }
    
    private void testExecutorWithShardRange(String query, Set<String> expected) {
        // test shard range execution first
        LinkedBlockingQueue<Map.Entry<Key,Document>> results = new LinkedBlockingQueue<>();
        QueryIteratorExecutor executor = new QueryIteratorExecutor(results, fieldIndexPool, evaluationPool, util.getConnector(), TableName.SHARD,
                        util.getAuths(), util.getMetadataHelper());
        executor.setScannerChunk(buildScannerChunk(query, null));
        
        Future<?> f = fieldIndexPool.submit(executor);
        try {
            Object done = f.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            fail("test failed while executing shard range: " + e.getMessage());
        }
        
        assertEquals("expected uid count does not match result count", expected.size(), results.size());
        
        Set<String> resultUids = new HashSet<>();
        while (!results.isEmpty()) {
            Map.Entry<Key,Document> entry = results.poll();
            if (entry != null) {
                resultUids.add(uidFromDocKey(entry.getKey()));
            }
        }
        
        assertEquals(expected, resultUids);
    }
    
    private void testExecutorWithDocRange(String query, Set<String> expected) {
        // test document range execution using the expected uids
        LinkedBlockingQueue<Map.Entry<Key,Document>> results = new LinkedBlockingQueue<>();
        for (String expect : expected) {
            QueryIteratorExecutor executor = new QueryIteratorExecutor(results, fieldIndexPool, evaluationPool, util.getConnector(), TableName.SHARD,
                            util.getAuths(), util.getMetadataHelper());
            executor.setScannerChunk(buildScannerChunk(query, expect));
            executor.executeForDocumentRange("dt\0" + expect);
            assertFalse(results.isEmpty());
            Map.Entry<Key,Document> entry = results.poll();
            assertEquals(expect, uidFromDocKey(entry.getKey()));
        }
        assertTrue("Expected executor to be done but there were more results", results.isEmpty());
    }
    
    private void testWithShardQueryLogic(String query, Set<String> expected, boolean collapseUids, boolean useMicroserviceFacade) {
        try {
            // setup query options here
            Map<String,String> options = new HashMap<>();
            // options.put(QueryParameters.RAW_DATA_ONLY, "true");
            
            // default dates
            SimpleDateFormat shardDateFormatter = new SimpleDateFormat("yyyyMMdd");
            Date startDate = shardDateFormatter.parse("20201212");
            Date endDate = shardDateFormatter.parse("20201225");
            
            // create a query impl
            QueryImpl q = new QueryImpl();
            q.setBeginDate(startDate);
            q.setEndDate(endDate);
            q.setQuery(query);
            q.setParameters(options);
            q.setId(UUID.randomUUID());
            q.setPagesize(Integer.MAX_VALUE);
            q.setQueryAuthorizations(util.getAuths().toString());
            
            // setup query logic
            Connector conn = util.getConnector();
            Set<Authorizations> authSet = Collections.singleton(util.getAuths());
            ShardQueryLogic logic = new ShardQueryLogic();
            logic.setMetadataTableName(TableName.METADATA);
            logic.setDateIndexTableName(TableName.DATE_INDEX);
            logic.setTableName(TableName.SHARD);
            logic.setIndexTableName(TableName.SHARD_INDEX);
            logic.setReverseIndexTableName(TableName.SHARD_RINDEX);
            logic.setModelTableName(TableName.METADATA);
            logic.setMaxResults(5000);
            logic.setMaxWork(25000);
            
            logic.setFullTableScanEnabled(false);
            logic.setIncludeDataTypeAsField(true);
            
            logic.setDateIndexHelperFactory(new DateIndexHelperFactory());
            logic.setMarkingFunctions(new MarkingFunctions.Default());
            logic.setMetadataHelperFactory(util.getMetadataHelperFactory());
            logic.setQueryPlanner(new DefaultQueryPlanner());
            logic.setResponseObjectFactory(new DefaultResponseObjectFactory());
            
            logic.setCollectTimingDetails(false);
            logic.setLogTimingDetails(false);
            logic.setMinimumSelectivity(0.03D);
            logic.setMaxIndexScanTimeMillis(5000);
            
            // experiments
            logic.setHitList(true);
            logic.setCollapseUids(collapseUids);
            if (useMicroserviceFacade) {
                logic.setUseMicroservice(true);
            } else {
                logic.setUseMicroservice(false);
            }
            
            GenericQueryConfiguration config = logic.initialize(conn, q, authSet);
            logic.setupQuery(config);
            
            // assert results
            Key key;
            Document document;
            Map.Entry<Key,Document> deser;
            Set<String> countedUids = new HashSet<>();
            for (Map.Entry<Key,Value> entry : logic) {
                log.info("entry: " + entry.toString());
                deser = deserializer.apply(entry);
                key = deser.getKey();
                document = deser.getValue();
                countedUids.add(uidFromDocKey(key));
            }
            assertEquals(expected, countedUids);
            
        } catch (Exception e) {
            log.error("Exception while running ShardQueryLogic: " + e.getMessage());
            e.printStackTrace();
            fail("Failed to complete test while executing ShardQueryLogic");
        }
    }
    
    /**
     * Build out the pieces we need to run the query
     * 
     * @param query
     * @param uid
     * @return
     */
    private ScannerChunk buildScannerChunk(String query, String uid) {
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setAuthorizations(Collections.singleton(util.getAuths()));
        
        IteratorSetting iterSetting = new IteratorSetting(140, QueryIterator.class);
        iterSetting.addOption(QueryOptions.QUERY, query);
        iterSetting.addOption(QueryOptions.QUERY_ID, UUID.randomUUID().toString());
        iterSetting.addOption(QueryOptions.INDEX_ONLY_FIELDS, "TOK");
        iterSetting.addOption(QueryOptions.INDEXED_FIELDS, "TOK,FIRST_NAME,MSG_SIZE");
        iterSetting.addOption(QueryOptions.TERM_FREQUENCY_FIELDS, "TOK,MSG");
        if (query.contains("content:")) {
            iterSetting.addOption(QueryOptions.TERM_FREQUENCIES_REQUIRED, "true");
        } else {
            iterSetting.addOption(QueryOptions.TERM_FREQUENCIES_REQUIRED, "false");
        }
        SessionOptions options = new SessionOptions();
        options.addScanIterator(iterSetting);
        options.setQueryConfig(config);
        
        Key startKey;
        if (uid == null) {
            startKey = new Key("20201212_0");
        } else {
            startKey = new Key("20201212_0", "dt\0" + uid);
        }
        Key endKey = startKey.followingKey(PartialKey.ROW_COLFAM);
        Range range = new Range(startKey, true, endKey, false);
        Collection<Range> ranges = Collections.singleton(range);
        return new ScannerChunk(options, ranges);
    }
    
    private String uidFromDocKey(Key key) {
        String cf = key.getColumnFamily().toString();
        int index = cf.indexOf('\u0000');
        return cf.substring(index + 1);
    }
    
}
