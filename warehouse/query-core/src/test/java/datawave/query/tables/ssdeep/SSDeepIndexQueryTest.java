package datawave.query.tables.ssdeep;

import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.EXPECTED_2_2_OVERLAPS;
import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.EXPECTED_2_3_OVERLAPS;
import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.EXPECTED_2_4_OVERLAPS;
import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.TEST_SSDEEPS;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.ingest.mapreduce.handler.ssdeep.SSDeepIndexHandler;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.query.tables.ssdeep.util.SSDeepTestUtil;
import datawave.query.testframework.AbstractDataTypeConfig;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.util.ssdeep.BucketAccumuloKeyGenerator;
import datawave.util.ssdeep.NGramByteHashGenerator;
import datawave.util.ssdeep.NGramGenerator;
import datawave.util.ssdeep.NGramTuple;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.result.EventQueryResponseBase;

/** Simple unit test against the SSDeepIndex / SSDeepSimilarityLogic code */
public class SSDeepIndexQueryTest {

    private static final Logger log = Logger.getLogger(SSDeepIndexQueryTest.class);

    private static final Authorizations auths = AbstractDataTypeConfig.getTestAuths();

    protected static AccumuloClient accumuloClient;

    protected SSDeepSimilarityQueryLogic logic;

    protected DatawavePrincipal principal;

    public static final int BUCKET_COUNT = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_COUNT;
    public static final int BUCKET_ENCODING_BASE = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_ENCODING_BASE;
    public static final int BUCKET_ENCODING_LENGTH = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_ENCODING_LENGTH;

    public static void indexSSDeepTestData(AccumuloClient accumuloClient) throws Exception {
        // configuration
        String ssdeepTableName = SSDeepIndexHandler.DEFAULT_SSDEEP_INDEX_TABLE_NAME;
        int ngramSize = NGramGenerator.DEFAULT_NGRAM_SIZE;
        int minHashSize = NGramGenerator.DEFAULT_MIN_HASH_SIZE;

        // input
        Stream<String> ssdeepLines = Stream.of(TEST_SSDEEPS);

        // processing
        final NGramByteHashGenerator nGramGenerator = new NGramByteHashGenerator(ngramSize, BUCKET_COUNT, minHashSize);
        final BucketAccumuloKeyGenerator accumuloKeyGenerator = new BucketAccumuloKeyGenerator(BUCKET_COUNT, BUCKET_ENCODING_BASE, BUCKET_ENCODING_LENGTH);

        // output
        BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
        final BatchWriter bw = accumuloClient.createBatchWriter(ssdeepTableName, batchWriterConfig);

        // operations
        ssdeepLines.forEach(s -> {
            try {
                Iterator<ImmutablePair<NGramTuple,byte[]>> it = nGramGenerator.call(s);
                while (it.hasNext()) {
                    ImmutablePair<NGramTuple,byte[]> nt = it.next();
                    ImmutablePair<Key,Value> at = accumuloKeyGenerator.call(nt);
                    Key k = at.getKey();
                    Mutation m = new Mutation(k.getRow());
                    ColumnVisibility cv = new ColumnVisibility(k.getColumnVisibility());
                    m.put(k.getColumnFamily(), k.getColumnQualifier(), cv, k.getTimestamp(), at.getValue());
                    bw.addMutation(m);
                }
                bw.flush();
            } catch (Exception e) {
                log.error("Exception loading ssdeep hashes", e);
                fail("Exception while loading ssdeep hashes: " + e.getMessage());
            }
        });

        bw.close();
    }

    @BeforeClass
    public static void loadData() throws Exception {
        final String tableName = SSDeepIndexHandler.DEFAULT_SSDEEP_INDEX_TABLE_NAME;

        InMemoryInstance i = new InMemoryInstance("ssdeepTestInstance");
        accumuloClient = new InMemoryAccumuloClient("root", i);

        /* create the table */
        TableOperations tops = accumuloClient.tableOperations();
        if (tops.exists(tableName)) {
            tops.delete(tableName);
        }
        tops.create(tableName);

        /* add ssdeep data to the table */
        indexSSDeepTestData(accumuloClient);

        /* dump the table */
        logSSDeepTestData(tableName);
    }

    @Before
    public void setUpQuery() {
        logic = new SSDeepSimilarityQueryLogic();
        logic.setTableName(SSDeepIndexHandler.DEFAULT_SSDEEP_INDEX_TABLE_NAME);
        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        logic.setBucketEncodingBase(BUCKET_ENCODING_BASE);
        logic.setBucketEncodingLength(BUCKET_ENCODING_LENGTH);
        logic.setIndexBuckets(BUCKET_COUNT);

        SubjectIssuerDNPair dn = SubjectIssuerDNPair.of("userDn", "issuerDn");
        DatawaveUser user = new DatawaveUser(dn, DatawaveUser.UserType.USER, Sets.newHashSet(auths.toString().split(",")), null, null, -1L);
        principal = new DatawavePrincipal(Collections.singleton(user));
    }

    @Test
    /* Test that a single query ssdeep with no match score threshold returns the expected results */
    public void testSingleQueryNoMinScore() throws Exception {
        runSingleQuery(false);
    }

    @Test
    /* Test that a single query ssdeep with a min score threshold returns the expected results */
    public void testSingleQueryMinScore() throws Exception {
        runSingleQuery(true);
    }

    private static void logSSDeepTestData(String tableName) throws TableNotFoundException {
        Scanner scanner = accumuloClient.createScanner(tableName, auths);
        Iterator<Map.Entry<Key,Value>> iterator = scanner.iterator();
        log.debug("*************** " + tableName + " ********************");
        while (iterator.hasNext()) {
            Map.Entry<Key,Value> entry = iterator.next();
            log.debug(entry);
        }
        scanner.close();
    }

    public void runSingleQuery(boolean applyMinScoreThreshold) throws Exception {
        String query = "CHECKSUM_SSDEEP:" + TEST_SSDEEPS[2];

        final int minScoreThreshold = applyMinScoreThreshold ? 67 : 0;
        final int expectedEventCount = applyMinScoreThreshold ? 2 : 3;

        EventQueryResponseBase response = runSSDeepQuery(query, minScoreThreshold);
        List<EventBase> events = response.getEvents();
        int eventCount = events.size();
        Map<String,Map<String,String>> observedEvents = extractObservedEvents(events);

        Assert.assertEquals(expectedEventCount, eventCount);

        // find the fields for the self match example.
        SSDeepTestUtil.assertSSDeepSimilarityMatch(TEST_SSDEEPS[2], TEST_SSDEEPS[2], "67", EXPECTED_2_2_OVERLAPS, "100", observedEvents);

        // find and validate the fields for the partial match example.
        SSDeepTestUtil.assertSSDeepSimilarityMatch(TEST_SSDEEPS[2], TEST_SSDEEPS[3], "53", EXPECTED_2_3_OVERLAPS, "96", observedEvents);

        if (applyMinScoreThreshold)
            assertNoMatch(TEST_SSDEEPS[2], TEST_SSDEEPS[3], observedEvents);
        else
            SSDeepTestUtil.assertSSDeepSimilarityMatch(TEST_SSDEEPS[2], TEST_SSDEEPS[4], "9", EXPECTED_2_4_OVERLAPS, "63", observedEvents);
    }

    public EventQueryResponseBase runSSDeepQuery(String query, int minScoreThreshold) throws Exception {

        QueryImpl q = new QueryImpl();
        q.setQuery(query);
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE);
        q.setQueryAuthorizations(auths.toString());

        if (minScoreThreshold > 0) {
            q.addParameter(SSDeepScoringFunction.MIN_SSDEEP_SCORE_PARAMETER, String.valueOf(minScoreThreshold));
        }

        RunningQuery runner = new RunningQuery(accumuloClient, AccumuloConnectionFactory.Priority.NORMAL, this.logic, q, "", principal,
                        new QueryMetricFactoryImpl());
        TransformIterator transformIterator = runner.getTransformIterator();
        SSDeepSimilarityQueryTransformer transformer = (SSDeepSimilarityQueryTransformer) transformIterator.getTransformer();
        EventQueryResponseBase response = (EventQueryResponseBase) transformer.createResponse(runner.next());

        return response;
    }

    /** Extract the events from a set of results into an easy to manage data structure for validation */
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

    /**
     * Assert that the results do not contain a match between the specified query and matching ssdeep
     *
     * @param querySsdeep
     *            the query ssdeep we do not expect to find in the match results
     * @param matchingSsdeep
     *            the matching ssdeep we do not expect to find i nthe match results
     * @param observedEvents
     *            the map of the observed events, created by extractObservedEvents on the event list obtained from query exeuction.
     */
    public static void assertNoMatch(String querySsdeep, String matchingSsdeep, Map<String,Map<String,String>> observedEvents) {
        final Map<String,String> observedFields = observedEvents.get(querySsdeep + "#" + matchingSsdeep);
        Assert.assertTrue("Observed fields was not empty", observedFields.isEmpty());

    }
}
