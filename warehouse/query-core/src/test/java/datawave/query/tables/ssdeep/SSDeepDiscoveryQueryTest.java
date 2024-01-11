package datawave.query.tables.ssdeep;

import com.google.common.collect.Sets;
import datawave.helpers.PrintUtility;
import datawave.marking.MarkingFunctions;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.discovery.DiscoveryLogic;
import datawave.query.discovery.DiscoveryTransformer;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.QueryLogicTestHarness;
import datawave.query.transformer.SSDeepSimilarityQueryTransformer;
import datawave.query.util.MetadataHelperFactory;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.result.EventQueryResponseBase;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static datawave.query.tables.ssdeep.SSDeepTestUtil.BUCKET_COUNT;
import static datawave.query.tables.ssdeep.SSDeepTestUtil.BUCKET_ENCODING_BASE;
import static datawave.query.tables.ssdeep.SSDeepTestUtil.BUCKET_ENCODING_LENGTH;

public class SSDeepDiscoveryQueryTest extends AbstractFunctionalQuery {

    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();

    private static final Logger log = Logger.getLogger(SSDeepDiscoveryQueryTest.class);

    SSDeepSimilarityQueryLogic similarityQueryLogic;

    DiscoveryLogic discoveryQueryLogic;

    SSDeepDiscoveryQueryTable similarityDiscoveryQueryLogic;

    @BeforeClass
    public static void filterSetup() throws Exception {
        log.setLevel(Level.DEBUG);
        Logger printLog = Logger.getLogger(PrintUtility.class);
        printLog.setLevel(Level.DEBUG);

        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new SSDeepFields();
        dataTypes.add(new SSDeepDataType(SSDeepDataType.SSDeepEntry.ssdeep, generic));

        SSDeepQueryTestTableHelper ssDeepQueryTestTableHelper = new SSDeepQueryTestTableHelper(SSDeepDiscoveryQueryTest.class.getName(), log, RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER, RebuildingScannerTestHelper.INTERRUPT.NEVER);
        accumuloSetup.setData(FileType.CSV, dataTypes);
        client = accumuloSetup.loadTables(ssDeepQueryTestTableHelper);
    }

    @Before
    public void setupQuery() {
        MarkingFunctions markingFunctions = new MarkingFunctions.Default();
        ResponseObjectFactory responseFactory = new DefaultResponseObjectFactory();
        MetadataHelperFactory metadataHelperFactory = new MetadataHelperFactory();

        similarityQueryLogic = new SSDeepSimilarityQueryLogic();
        similarityQueryLogic.setTableName("ssdeepIndex");
        similarityQueryLogic.setMarkingFunctions(markingFunctions);
        similarityQueryLogic.setResponseObjectFactory(responseFactory);
        similarityQueryLogic.setBucketEncodingBase(BUCKET_ENCODING_BASE);
        similarityQueryLogic.setBucketEncodingLength(BUCKET_ENCODING_LENGTH);
        similarityQueryLogic.setIndexBuckets(BUCKET_COUNT);

        discoveryQueryLogic = new DiscoveryLogic();
        discoveryQueryLogic.setTableName("shardIndex");
        discoveryQueryLogic.setIndexTableName("shardIndex");
        discoveryQueryLogic.setReverseIndexTableName("shardReverseIndex");
        discoveryQueryLogic.setModelTableName("metadata");
        discoveryQueryLogic.setMarkingFunctions(markingFunctions);
        discoveryQueryLogic.setMetadataHelperFactory(metadataHelperFactory);
        discoveryQueryLogic.setResponseObjectFactory(responseFactory);

        //TODO: this implementation currently does not properly initialize the latter logic at the right time
        // this means that the model is null when we attempt to get the transformer initially. For now,
        // we'll develop using the full chain strategy.
        StreamingSSDeepDiscoveryChainStrategy ssdeepStreamedChainStrategy = new StreamingSSDeepDiscoveryChainStrategy();
        ssdeepStreamedChainStrategy.setMaxResultsToBuffer(1); // disable buffering for this test

        //TODO: This implementation works for now, but will likely not scale.
        FullSSDeepDiscoveryChainStrategy ssdeepFullChainStrategy = new FullSSDeepDiscoveryChainStrategy();

        similarityDiscoveryQueryLogic = new SSDeepDiscoveryQueryTable();
        similarityDiscoveryQueryLogic.setTableName("ssdeepIndex");
        similarityDiscoveryQueryLogic.setLogic1(similarityQueryLogic);
        similarityDiscoveryQueryLogic.setLogic2(discoveryQueryLogic);
        similarityDiscoveryQueryLogic.setChainStrategy(ssdeepFullChainStrategy);

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

    public SSDeepDiscoveryQueryTest() {
        super(SSDeepDataType.getManager());
    }

    @Test
    public void testSSDeepSimilarity() throws Exception {
        log.info("------ testSSDeepSimilarity ------");
        String testSSDeep = "384:nv/fP9FmWVMdRFj2aTgSO+u5QT4ZE1PIVS:nDmWOdRFNTTs504cQS";
        String query = "CHECKSUM_SSDEEP:" + testSSDeep;
        EventQueryResponseBase response = runSSDeepSimilarityQuery(query, 0);
        List<EventBase> events = response.getEvents();
        int eventCount = events.size();
        Map<String,Map<String,String>> observedEvents = extractObservedEvents(events);
        Assert.assertEquals(1, eventCount);

        SSDeepTestUtil.assertSSDeepSimilarityMatch(testSSDeep, testSSDeep, "38.0", "1", "100", observedEvents);
    }

    @Test
    public void testDiscovery() throws Exception {
        log.info("------ testDiscovery ------");
        String testSSDeep = "384:nv/fP9FmWVMdRFj2aTgSO+u5QT4ZE1PIVS:nDmWOdRFNTTs504cQS";
        String query = "CHECKSUM_SSDEEP:\"" + testSSDeep + "\"";
        EventQueryResponseBase response = runDiscoveryQuery(query, 0);
        List<EventBase> events = response.getEvents();
        int eventCount = events.size();
        Map<String,Map<String,String>> observedEvents = extractObservedEvents(events);
        Assert.assertEquals(1, eventCount);
    }

    @Test
    public void testChainedSSDeepDiscovery() throws Exception {
        Logger.getLogger(StreamingSSDeepDiscoveryChainStrategy.SSDeepDiscoveryChainedIterator.class).setLevel(Level.DEBUG);

        log.info("------ testSSDeepDiscovery ------");
        String testSSDeep = "384:nv/fP9FmWVMdRFj2aTgSO+u5QT4ZE1PIVS:nDmWOdRFNTTs504---";
        String targetSSDeep = "384:nv/fP9FmWVMdRFj2aTgSO+u5QT4ZE1PIVS:nDmWOdRFNTTs504cQS";
        String query = "CHECKSUM_SSDEEP:" + testSSDeep;
        EventQueryResponseBase response = runChainedQuery(query, 0);
        List<EventBase> events = response.getEvents();
        int eventCount = events.size();
        Map<String,Map<String,String>> observedEvents = extractObservedEvents(events);
        Assert.assertEquals(1, eventCount);

        Map.Entry<String, Map<String,String>> result = observedEvents.entrySet().iterator().next();
        Map<String, String> resultFields = result.getValue();
        Assert.assertEquals(targetSSDeep, resultFields.get("VALUE"));
        Assert.assertEquals("CHECKSUM_SSDEEP",resultFields.get("FIELD"));
        Assert.assertEquals("20201031", resultFields.get("DATE"));
        Assert.assertEquals("ssdeep", resultFields.get("DATA TYPE"));
        Assert.assertEquals("4", resultFields.get("RECORD COUNT"));
    }


    public EventQueryResponseBase runSSDeepSimilarityQuery(String query, int minScoreThreshold) throws Exception {
        QueryImpl q = new QueryImpl();
        q.setQuery(query);
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE);
        q.setQueryAuthorizations(auths.toString());

        if (minScoreThreshold > 0) {
            q.addParameter(SSDeepSimilarityQueryTransformer.MIN_SSDEEP_SCORE_PARAMETER, String.valueOf(minScoreThreshold));
        }

        RunningQuery runner = new RunningQuery(client, AccumuloConnectionFactory.Priority.NORMAL, similarityQueryLogic, q, "", principal,
                new QueryMetricFactoryImpl());
        TransformIterator transformIterator = runner.getTransformIterator();
        SSDeepSimilarityQueryTransformer transformer = (SSDeepSimilarityQueryTransformer) transformIterator.getTransformer();
        EventQueryResponseBase response = (EventQueryResponseBase) transformer.createResponse(runner.next());

        return response;
    }

    public EventQueryResponseBase runDiscoveryQuery(String query, int minScoreThreshold) throws Exception {
        QueryImpl q = new QueryImpl();
        q.setQuery(query);
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE);
        q.setQueryAuthorizations(auths.toString());

        if (minScoreThreshold > 0) {
            q.addParameter(SSDeepSimilarityQueryTransformer.MIN_SSDEEP_SCORE_PARAMETER, String.valueOf(minScoreThreshold));
        }

        RunningQuery runner = new RunningQuery(client, AccumuloConnectionFactory.Priority.NORMAL, discoveryQueryLogic, q, "", principal,
                new QueryMetricFactoryImpl());
        TransformIterator transformIterator = runner.getTransformIterator();
        DiscoveryTransformer transformer = (DiscoveryTransformer) transformIterator.getTransformer();
        EventQueryResponseBase response = (EventQueryResponseBase) transformer.createResponse(runner.next());

        return response;
    }

    public EventQueryResponseBase runChainedQuery(String query, int minScoreThreshold) throws Exception {
        QueryImpl q = new QueryImpl();
        q.setQuery(query);
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE);
        q.setQueryAuthorizations(auths.toString());

        if (minScoreThreshold > 0) {
            q.addParameter(SSDeepSimilarityQueryTransformer.MIN_SSDEEP_SCORE_PARAMETER, String.valueOf(minScoreThreshold));
        }

        RunningQuery runner = new RunningQuery(client, AccumuloConnectionFactory.Priority.NORMAL, similarityDiscoveryQueryLogic, q, "", principal,
                new QueryMetricFactoryImpl());
        TransformIterator transformIterator = runner.getTransformIterator();
        DiscoveryTransformer transformer = (DiscoveryTransformer) transformIterator.getTransformer();
        EventQueryResponseBase response = (EventQueryResponseBase) transformer.createResponse(runner.next());

        return response;
    }


    /** Extract the events from a set of results into an easy to manage data structure for validation */
    public Map<String, Map<String,String>> extractObservedEvents(List<EventBase> events) {
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
