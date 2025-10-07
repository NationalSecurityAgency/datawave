package datawave.query.tables.ssdeep;

import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.BUCKET_COUNT;
import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.BUCKET_ENCODING_BASE;
import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.BUCKET_ENCODING_LENGTH;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.AbstractQueryLogicTransformer;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.helpers.PrintUtility;
import datawave.ingest.mapreduce.handler.ssdeep.SSDeepIndexHandler;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.tables.ssdeep.testframework.SSDeepDataType;
import datawave.query.tables.ssdeep.testframework.SSDeepFields;
import datawave.query.tables.ssdeep.testframework.SSDeepQueryTestTableHelper;
import datawave.query.tables.ssdeep.util.SSDeepTestUtil;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.QueryLogicTestHarness;
import datawave.query.util.MetadataHelperFactory;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.result.EventQueryResponseBase;

/**
 * Ingests some test data into the ssdeepIndex and shard tables and then tests that various SSDeep query logics against that data produce the expected results
 */
public class SSDeepIngestQueryTest extends AbstractFunctionalQuery {

    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();

    private static final Logger log = Logger.getLogger(SSDeepIngestQueryTest.class);

    SSDeepSimilarityQueryLogic similarityQueryLogic;

    SSDeepDiscoveryQueryLogic discoveryQueryLogic;

    SSDeepChainedDiscoveryQueryLogic similarityDiscoveryQueryLogic;

    @BeforeClass
    public static void filterSetup() throws Exception {
        log.setLevel(Level.DEBUG);
        Logger printLog = Logger.getLogger(PrintUtility.class);
        printLog.setLevel(Level.DEBUG);

        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new SSDeepFields();
        dataTypes.add(new SSDeepDataType(SSDeepDataType.SSDeepEntry.ssdeep, generic));

        SSDeepQueryTestTableHelper ssDeepQueryTestTableHelper = new SSDeepQueryTestTableHelper(SSDeepIngestQueryTest.class.getName(), log,
                        RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER, RebuildingScannerTestHelper.INTERRUPT.NEVER);
        accumuloSetup.setData(FileType.CSV, dataTypes);
        client = accumuloSetup.loadTables(ssDeepQueryTestTableHelper);
    }

    @Before
    public void setupQuery() {
        MarkingFunctions markingFunctions = new MarkingFunctions.Default();
        ResponseObjectFactory responseFactory = new DefaultResponseObjectFactory();
        MetadataHelperFactory metadataHelperFactory = new MetadataHelperFactory();

        similarityQueryLogic = new SSDeepSimilarityQueryLogic();
        similarityQueryLogic.setTableName(SSDeepIndexHandler.DEFAULT_SSDEEP_INDEX_TABLE_NAME);
        similarityQueryLogic.setMarkingFunctions(markingFunctions);
        similarityQueryLogic.setResponseObjectFactory(responseFactory);
        similarityQueryLogic.setBucketEncodingBase(BUCKET_ENCODING_BASE);
        similarityQueryLogic.setBucketEncodingLength(BUCKET_ENCODING_LENGTH);
        similarityQueryLogic.setIndexBuckets(BUCKET_COUNT);

        discoveryQueryLogic = new SSDeepDiscoveryQueryLogic();
        discoveryQueryLogic.setTableName("shardIndex");
        discoveryQueryLogic.setIndexTableName("shardIndex");
        discoveryQueryLogic.setReverseIndexTableName("shardReverseIndex");
        discoveryQueryLogic.setMetadataTableName("metadata");
        discoveryQueryLogic.setModelName("DATAWAVE");
        discoveryQueryLogic.setMarkingFunctions(markingFunctions);
        discoveryQueryLogic.setMetadataHelperFactory(metadataHelperFactory);
        discoveryQueryLogic.setResponseObjectFactory(responseFactory);

        // FUTURE: Implement a streaming chain strategy for the SSDeepChainedDiscoveryQueryLogic
        FullSSDeepDiscoveryChainStrategy ssdeepDiscoveryChainStrategy = new FullSSDeepDiscoveryChainStrategy();

        similarityDiscoveryQueryLogic = new SSDeepChainedDiscoveryQueryLogic();
        similarityDiscoveryQueryLogic.setTableName("ssdeepIndex");
        similarityDiscoveryQueryLogic.setLogic1(similarityQueryLogic);
        similarityDiscoveryQueryLogic.setLogic2(discoveryQueryLogic);
        similarityDiscoveryQueryLogic.setChainStrategy(ssdeepDiscoveryChainStrategy);

        // init must set auths
        testInit();

        SubjectIssuerDNPair dn = SubjectIssuerDNPair.of("userDn", "issuerDn");
        DatawaveUser user = new DatawaveUser(dn, DatawaveUser.UserType.USER, Sets.newHashSet(this.auths.toString().split(",")), null, null, -1L);
        principal = new DatawavePrincipal(Collections.singleton(user));

        testHarness = new QueryLogicTestHarness(this);
    }

    protected void testInit() {
        this.auths = SSDeepDataType.getTestAuths();
        this.documentKey = SSDeepDataType.SSDeepField.EVENT_ID.name();
    }

    public SSDeepIngestQueryTest() {
        super(SSDeepDataType.getManager());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testSSDeepSimilarity() throws Exception {
        similarityQueryLogic.getConfig().setDedupeSimilarityHashes(false);
        log.info("------ testSSDeepSimilarity ------");
        @SuppressWarnings("SpellCheckingInspection")
        String testSSDeep = "384:nv/fP9FmWVMdRFj2aTgSO+u5QT4ZE1PIVS:nDmWOdRFNTTs504cQS";
        String query = "CHECKSUM_SSDEEP:" + testSSDeep;
        String expectedOverlaps = "384:+u5QT4Z, 384:/fP9FmW, 384:2aTgSO+, 384:4ZE1PIV, 384:5QT4ZE1, 384:9FmWVMd, 384:Fj2aTgS, 384:FmWVMdR, 384:MdRFj2a, 384:O+u5QT4, 384:P9FmWVM, 384:QT4ZE1P, 384:RFj2aTg, 384:SO+u5QT, 384:T4ZE1PI, 384:TgSO+u5, 384:VMdRFj2, 384:WVMdRFj, 384:ZE1PIVS, 384:aTgSO+u, 384:dRFj2aT, 384:fP9FmWV, 384:gSO+u5Q, 384:j2aTgSO, 384:mWVMdRF, 384:nv/fP9F, 384:u5QT4ZE, 384:v/fP9Fm, 768:DmWOdRF, 768:FNTTs50, 768:NTTs504, 768:OdRFNTT, 768:RFNTTs5, 768:TTs504c, 768:Ts504cQ, 768:WOdRFNT, 768:dRFNTTs, 768:mWOdRFN, 768:nDmWOdR, 768:s504cQS";
        EventQueryResponseBase response = runSSDeepQuery(query, similarityQueryLogic, 0);

        List<EventBase> events = response.getEvents();
        Assert.assertEquals(40, events.size());

        Map<String,Map<String,String>> observedEvents = extractObservedEvents(events);

        SSDeepTestUtil.assertSSDeepSimilarityMatch(testSSDeep, testSSDeep, "40", expectedOverlaps, "100", observedEvents);

        // now repeat with dedupe true
        similarityQueryLogic.getConfig().setDedupeSimilarityHashes(true);
        response = runSSDeepQuery(query, similarityQueryLogic, 0);

        events = response.getEvents();
        Assert.assertEquals(1, events.size());

        observedEvents = extractObservedEvents(events);

        SSDeepTestUtil.assertSSDeepSimilarityMatch(testSSDeep, testSSDeep, "40", expectedOverlaps, "100", observedEvents);
    }

    @Test
    public void testSSDeepDiscovery() throws Exception {
        log.info("------ testSSDeepDiscovery ------");
        String testSSDeep = "384:nv/fP9FmWVMdRFj2aTgSO+u5QT4ZE1PIVS:nDmWOdRFNTTs504cQS";
        String query = "CHECKSUM_SSDEEP:\"" + testSSDeep + "\"";
        EventQueryResponseBase response = runSSDeepQuery(query, discoveryQueryLogic, 0);

        List<EventBase> events = response.getEvents();
        Assert.assertEquals(1, events.size());
        Map<String,Map<String,String>> observedEvents = extractObservedEvents(events);

        Map.Entry<String,Map<String,String>> result = observedEvents.entrySet().iterator().next();
        Map<String,String> resultFields = result.getValue();
        Assert.assertEquals(testSSDeep, resultFields.remove("VALUE"));
        Assert.assertEquals("CHECKSUM_SSDEEP", resultFields.remove("FIELD"));
        Assert.assertEquals("20201031", resultFields.remove("DATE"));
        Assert.assertEquals("ssdeep", resultFields.remove("DATA TYPE"));
        Assert.assertEquals("4", resultFields.remove("RECORD COUNT"));

        // At this point, the results have not been enriched with these fields, so they should not exist.
        Assert.assertNull(null, resultFields.remove("QUERY"));
        Assert.assertNull(null, resultFields.remove("WEIGHTED_SCORE"));

        Assert.assertTrue("Results had unexpected fields: " + resultFields, resultFields.isEmpty());
    }

    @Test
    public void testChainedSSDeepDiscovery() throws Exception {
        ((FullSSDeepDiscoveryChainStrategy) this.similarityDiscoveryQueryLogic.getChainStrategy()).setBatchSize(1);

        log.info("------ testChainedSSDeepDiscovery ------");
        String testSSDeep = "384:nv/fP9FmWVMdRFj2aTgSO+u5QT4ZE1PIVS:nDmWOdRFNTTs504---";
        String testSSDeep2 = "192:1s---FXYvPToUCpabe0qHEbM0NkaA80W---ixZzS7:1r4X6oU6ZHEbp1biW7";
        String targetSSDeep = "384:nv/fP9FmWVMdRFj2aTgSO+u5QT4ZE1PIVS:nDmWOdRFNTTs504cQS";
        String query = "CHECKSUM_SSDEEP:" + testSSDeep + " OR CHECKSUM_SSDEEP:" + testSSDeep2;
        String expectedOverlaps = "384:+u5QT4Z, 384:/fP9FmW, 384:2aTgSO+, 384:4ZE1PIV, 384:5QT4ZE1, 384:9FmWVMd, 384:Fj2aTgS, 384:FmWVMdR, 384:MdRFj2a, 384:O+u5QT4, 384:P9FmWVM, 384:QT4ZE1P, 384:RFj2aTg, 384:SO+u5QT, 384:T4ZE1PI, 384:TgSO+u5, 384:VMdRFj2, 384:WVMdRFj, 384:ZE1PIVS, 384:aTgSO+u, 384:dRFj2aT, 384:fP9FmWV, 384:gSO+u5Q, 384:j2aTgSO, 384:mWVMdRF, 384:nv/fP9F, 384:u5QT4ZE, 384:v/fP9Fm, 768:DmWOdRF, 768:FNTTs50, 768:NTTs504, 768:OdRFNTT, 768:RFNTTs5, 768:WOdRFNT, 768:dRFNTTs, 768:mWOdRFN, 768:nDmWOdR";

        EventQueryResponseBase response = runSSDeepQuery(query, similarityDiscoveryQueryLogic, 0);

        List<EventBase> events = response.getEvents();
        Assert.assertEquals(2, events.size());
        Map<String,Map<String,String>> observedEvents = extractObservedEvents(events);

        Map.Entry<String,Map<String,String>> result = observedEvents.entrySet().iterator().next();
        Map<String,String> resultFields = result.getValue();
        Assert.assertEquals(targetSSDeep, resultFields.remove("VALUE"));

        Assert.assertEquals("CHECKSUM_SSDEEP", resultFields.remove("FIELD"));
        Assert.assertEquals("20201031", resultFields.remove("DATE"));
        Assert.assertEquals("ssdeep", resultFields.remove("DATA TYPE"));
        Assert.assertEquals("4", resultFields.remove("RECORD COUNT"));

        // The results have been enriched with these fields at this point.
        Assert.assertEquals(testSSDeep, resultFields.remove("QUERY"));
        Assert.assertEquals("100", resultFields.remove("WEIGHTED_SCORE"));
        Assert.assertEquals("37", resultFields.remove("OVERLAP_SCORE"));
        Assert.assertEquals(expectedOverlaps, resultFields.remove("OVERLAP_SSDEEP_NGRAMS"));

        Assert.assertTrue("Results had unexpected fields: " + resultFields, resultFields.isEmpty());

    }

    @Test
    public void testChainedSSDeepDiscoveryNoResults() throws Exception {
        log.info("------ testChainedSSDeepDiscoveryNoResults ------");
        String testSSDeep = "384:aabbccddeeff:abcdef";
        String query = "CHECKSUM_SSDEEP:" + testSSDeep;

        EventQueryResponseBase response = runSSDeepQuery(query, similarityDiscoveryQueryLogic, 0);
    }

    @Test(expected = SSDeepRuntimeQueryException.class)
    public void testChainedSSDeepDiscoveryNoValidQuery() throws Exception {
        log.info("------ testChainedSSDeepDiscoveryNoValidQuery ------");
        String testSSDeep = "384:zzzzzzzzzzzz:zzzzzz";
        String query = "CHECKSUM_SSDEEP:" + testSSDeep;

        EventQueryResponseBase response = runSSDeepQuery(query, similarityDiscoveryQueryLogic, 0);
    }

    @SuppressWarnings("rawtypes")
    public EventQueryResponseBase runSSDeepQuery(String query, QueryLogic<?> queryLogic, int minScoreThreshold) throws Exception {
        QueryImpl q = new QueryImpl();
        q.setQuery(query);
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE);
        q.setQueryAuthorizations(auths.toString());

        if (minScoreThreshold > 0) {
            q.addParameter(SSDeepScoringFunction.MIN_SSDEEP_SCORE_PARAMETER, String.valueOf(minScoreThreshold));
        }

        RunningQuery runner = new RunningQuery(client, AccumuloConnectionFactory.Priority.NORMAL, queryLogic, q, "", principal, new QueryMetricFactoryImpl());
        TransformIterator transformIterator = runner.getTransformIterator();
        AbstractQueryLogicTransformer<?,?> transformer = (AbstractQueryLogicTransformer<?,?>) transformIterator.getTransformer();
        return (EventQueryResponseBase) transformer.createResponse(runner.next());
    }

    /** Extract the events from a set of results into an easy to manage data structure for validation */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Map<String,Map<String,String>> extractObservedEvents(List<EventBase> events) {
        int eventCount = events.size();
        Map<String,Map<String,String>> observedEvents = new HashMap<>();
        if (eventCount > 0) {
            for (EventBase e : events) {
                Map<String,String> observedFields = new HashMap<>();
                String querySsdeep = "UNKNOWN_QUERY";
                String matchingSsdeep = "UNKNOWN_MATCH";

                List<FieldBase> fields = e.getFields();
                for (FieldBase f : fields) {
                    if (f.getName().equals("QUERY_SSDEEP")) {
                        querySsdeep = f.getValueString();
                    }
                    if (f.getName().equals("MATCHING_SSDEEP")) {
                        matchingSsdeep = f.getValueString();
                    }
                    observedFields.put(f.getName(), f.getValueString());
                }

                String eventKey = querySsdeep + "#" + matchingSsdeep;
                observedEvents.put(eventKey, observedFields);
            }
        }
        return observedEvents;
    }
}
