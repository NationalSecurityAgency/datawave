package datawave.query.tables.ssdeep.util;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.junit.Assert;

import datawave.ingest.mapreduce.handler.ssdeep.SSDeepIndexHandler;
import datawave.query.tables.ssdeep.SSDeepSimilarityQueryTest;
import datawave.util.ssdeep.BucketAccumuloKeyGenerator;
import datawave.util.ssdeep.NGramByteHashGenerator;
import datawave.util.ssdeep.NGramGenerator;
import datawave.util.ssdeep.NGramTuple;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;

public class SSDeepTestUtil {

    private static final Logger log = Logger.getLogger(SSDeepSimilarityQueryTest.class);

    @SuppressWarnings("SpellCheckingInspection")
    public static String[] TEST_SSDEEPS = {"12288:002r/VG4GjeZHkwuPikQ7lKH5p5H9x1beZHkwulizQ1lK55pGxlXTd8zbW:002LVG4GjeZEXi37l6Br1beZEdic1lmu",
            "6144:02C3nq73v1kHGhs6y7ppFj93NRW6/ftZTgC6e8o4toHZmk6ZxoXb0ns:02C4cGCLjj9Swfj9koHEk6/Fns",
            "3072:02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP:02MKlWQ7Sg3d4bO968rm7JO",
            "3072:03jscyaGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP:03NLmXR7Sg3d4bO968rm7JO",
            "3072:03jscyaZZZZZYYYYXXXWWdXVmbOn5u46KjnzWWWXXXXYYYYYYZZZZZZZ:03NLmXR7ZZZYYXW9WXYYZZZ",
            "48:1aBhsiUw69/UXX0x0qzNkVkydf2klA8a7Z35:155w69MXAlNkmkWTF5", "196608:wEEE+EEEEE0LEEEEEEEEEEREEEEhEEETEEEEEWUEEEJEEEEcEEEEEEEE3EEEEEEN:",
            "1536:0YgNvw/OmgPgiQeI+25Nh6+RS5Qa8LmbyfAiIRgizy1cBx76UKYbD+iD/RYgNvw6:", "12288:222222222222222222222222222222222:"};

    public static final int BUCKET_COUNT = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_COUNT;
    public static final int BUCKET_ENCODING_BASE = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_ENCODING_BASE;
    public static final int BUCKET_ENCODING_LENGTH = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_ENCODING_LENGTH;

    public static void loadSSDeepIndexTextData(AccumuloClient accumuloClient) throws Exception {
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

    /**
     * assert that a match exists between the specified query and matching ssdeep and that the match has the expected properties
     *
     * @param querySsdeep
     *            the query ssdeep we expect to find in the match results
     * @param matchingSsdeep
     *            the matching ssdeep we expect to find in the match results.
     * @param matchScore
     *            the base match score
     * @param weightedScore
     *            the weighted match score.
     * @param observedEvents
     *            the map of observed events, created by extractObservedEvents on the event list obtained from query execution.
     */
    public static void assertSSDeepSimilarityMatch(String querySsdeep, String matchingSsdeep, String matchScore, String weightedScore,
                    Map<String,Map<String,String>> observedEvents) {
        final Map<String,String> observedFields = observedEvents.get(querySsdeep + "#" + matchingSsdeep);
        Assert.assertNotNull("Observed fields was null", observedFields);
        Assert.assertFalse("Observed fields was unexpectedly empty", observedFields.isEmpty());
        Assert.assertEquals(matchScore, observedFields.remove("MATCH_SCORE"));
        Assert.assertEquals(weightedScore, observedFields.remove("WEIGHTED_SCORE"));
        Assert.assertEquals(querySsdeep, observedFields.remove("QUERY_SSDEEP"));
        Assert.assertEquals(matchingSsdeep, observedFields.remove("MATCHING_SSDEEP"));
        Assert.assertTrue("Observed unexpected field(s) in full match: " + observedFields, observedFields.isEmpty());
    }

    /**
     * Assert that the results do not contain a match between the specified query and matching ssdeep
     *
     * @param querySsdeep
     *            the query ssdeep we do not expect to find in the match results
     * @param matchingSsdeep
     *            the matching ssdeep we do not expect to find in the match results
     * @param observedEvents
     *            the map of the observed events, created by extractObservedEvents on the event list obtained from query execution.
     */
    public static void assertNoMatch(String querySsdeep, String matchingSsdeep, Map<String,Map<String,String>> observedEvents) {
        final Map<String,String> observedFields = observedEvents.get(querySsdeep + "#" + matchingSsdeep);
        Assert.assertTrue("Observed fields was not empty", observedFields.isEmpty());

    }

    /** Extract the events from a set of results into an easy to manage data structure for validation */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static Map<String,Map<String,String>> extractObservedEvents(List<EventBase> events) {
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
