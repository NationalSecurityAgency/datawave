package datawave.query.tables.ssdeep;

import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.BUCKET_COUNT;
import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.BUCKET_ENCODING_BASE;
import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.BUCKET_ENCODING_LENGTH;
import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.EXPECTED_2_2_OVERLAPS;
import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.EXPECTED_2_3_OVERLAPS;
import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.EXPECTED_2_4_OVERLAPS;
import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.TEST_SSDEEPS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
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
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.tables.ssdeep.util.SSDeepTestUtil;
import datawave.query.testframework.AbstractDataTypeConfig;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.result.EventQueryResponseBase;

/** Additional unit test against the SSDeepIndex / SSDeepSimilarityLogic code */
public class SSDeepSimilarityQueryTest {

    private static final Logger log = Logger.getLogger(SSDeepSimilarityQueryTest.class);

    private static final Authorizations auths = AbstractDataTypeConfig.getTestAuths();

    protected static AccumuloClient accumuloClient;

    protected SSDeepSimilarityQueryLogic logic;

    protected DatawavePrincipal principal;

    @BeforeClass
    public static void loadData() throws Exception {
        final String tableName = SSDeepIndexHandler.DEFAULT_SSDEEP_INDEX_TABLE_NAME;

        InMemoryInstance inMemoryInstance = new InMemoryInstance("ssdeepTestInstance");
        accumuloClient = new InMemoryAccumuloClient("root", inMemoryInstance);

        /* create the table */
        TableOperations tops = accumuloClient.tableOperations();
        if (tops.exists(tableName)) {
            tops.delete(tableName);
        }
        tops.create(tableName);

        /* add ssdeep data to the table */
        SSDeepTestUtil.loadSSDeepIndexTextData(accumuloClient);

        /* dump the table */
        logSSDeepTestData();
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
    public void testSingleQueryNoMinScore() throws Exception {
        runSingleQuery(false);
    }

    @Test
    public void testSingleQueryMinScore() throws Exception {
        runSingleQuery(true);
    }

    @Test(expected = DatawaveFatalQueryException.class)
    public void testMaxResultsLimit() throws Exception {
        logic.setMaxResults(2);
        runSingleQuery(false);
    }

    @Test(expected = DatawaveFatalQueryException.class)
    public void testMaxHashLimit() throws Exception {
        logic.setMaxHashes(1);
        String query = "CHECKSUM_SSDEEP:" + TEST_SSDEEPS[2] + " OR CHECKSUM_SSDEEP:" + TEST_SSDEEPS[3];
        runSSDeepQuery(query, 0);
    }

    @Test
    public void testMaxHashesPerNGram() throws Exception {
        // block all hashes
        logic.setMaxHashesPerNGram(0);
        // this normally would have results [see below]
        String query = "CHECKSUM_SSDEEP:" + TEST_SSDEEPS[2];
        // no ngrams in similarity means no results
        EventQueryResponseBase response = runSSDeepQuery(query, 0);
        assertNull(response.getEvents());

        // verify with the value reset its fine
        logic.setMaxHashesPerNGram(10);
        // provate this now has results
        query = "CHECKSUM_SSDEEP:" + TEST_SSDEEPS[2];
        // no ngrams in similarity means no results
        response = runSSDeepQuery(query, 0);
        assertNotNull(response.getEvents());
    }

    private static void logSSDeepTestData() throws TableNotFoundException {
        Scanner scanner = accumuloClient.createScanner(SSDeepIndexHandler.DEFAULT_SSDEEP_INDEX_TABLE_NAME, auths);
        Iterator<Map.Entry<Key,Value>> iterator = scanner.iterator();
        log.debug("*************** ssdeepIndex ********************");
        while (iterator.hasNext()) {
            Map.Entry<Key,Value> entry = iterator.next();
            log.debug(entry);
        }
        scanner.close();
    }

    @SuppressWarnings("rawtypes")
    public void runSingleQuery(boolean applyMinScoreThreshold) throws Exception {
        String query = "CHECKSUM_SSDEEP:" + TEST_SSDEEPS[2];

        final int minScoreThreshold = applyMinScoreThreshold ? 67 : 0;
        final int expectedEventCount = applyMinScoreThreshold ? 2 : 3;

        EventQueryResponseBase response = runSSDeepQuery(query, minScoreThreshold);
        List<EventBase> events = response.getEvents();
        int eventCount = events.size();
        Map<String,Map<String,String>> observedEvents = SSDeepTestUtil.extractObservedEvents(events);

        Assert.assertEquals(expectedEventCount, eventCount);

        // find the fields for the self match example.
        SSDeepTestUtil.assertSSDeepSimilarityMatch(TEST_SSDEEPS[2], TEST_SSDEEPS[2], "67", EXPECTED_2_2_OVERLAPS, "100", observedEvents);

        // find and validate the fields for the partial match example.
        SSDeepTestUtil.assertSSDeepSimilarityMatch(TEST_SSDEEPS[2], TEST_SSDEEPS[3], "53", EXPECTED_2_3_OVERLAPS, "96", observedEvents);

        if (applyMinScoreThreshold)
            SSDeepTestUtil.assertNoMatch(TEST_SSDEEPS[2], TEST_SSDEEPS[3], observedEvents);
        else
            SSDeepTestUtil.assertSSDeepSimilarityMatch(TEST_SSDEEPS[2], TEST_SSDEEPS[4], "9", EXPECTED_2_4_OVERLAPS, "63", observedEvents);
    }

    @SuppressWarnings("rawtypes")
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
        return (EventQueryResponseBase) transformer.createResponse(runner.next());
    }

}
