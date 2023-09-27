package datawave.query;

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
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.marking.MarkingFunctions;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.query.tables.SSDeepSimilarityQueryLogic;
import datawave.query.testframework.AbstractDataTypeConfig;
import datawave.query.transformer.SSDeepSimilarityQueryTransformer;
import datawave.query.util.Tuple2;
import datawave.query.util.ssdeep.BucketAccumuloKeyGenerator;
import datawave.query.util.ssdeep.NGramByteHashGenerator;
import datawave.query.util.ssdeep.NGramTuple;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.result.EventQueryResponseBase;

public class SSDeepQueryTest {

    public static String[] TEST_SSDEEPS = {"12288:002r/VG4GjeZHkwuPikQ7lKH5p5H9x1beZHkwulizQ1lK55pGxlXTd8zbW:002LVG4GjeZEXi37l6Br1beZEdic1lmu",
            "6144:02C3nq73v1kHGhs6y7ppFj93NRW6/ftZTgC6e8o4toHZmk6ZxoXb0ns:02C4cGCLjj9Swfj9koHEk6/Fns",
            "3072:02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP:02MKlWQ7Sg3d4bO968rm7JO",
            "48:1aBhsiUw69/UXX0x0qzNkVkydf2klA8a7Z35:155w69MXAlNkmkWTF5", "196608:wEEE+EEEEE0LEEEEEEEEEEREEEEhEEETEEEEEWUEEEJEEEEcEEEEEEEE3EEEEEEN:",
            "1536:0YgNvw/OmgPgiQeI+25Nh6+RS5Qa8LmbyfAiIRgizy1cBx76UKYbD+iD/RYgNvw6:", "12288:222222222222222222222222222222222:"};

    private static final Logger log = Logger.getLogger(SSDeepQueryTest.class);

    private static final Authorizations auths = AbstractDataTypeConfig.getTestAuths();

    protected static AccumuloClient accumuloClient;

    protected SSDeepSimilarityQueryLogic logic;

    protected DatawavePrincipal principal;

    public static final int BUCKET_COUNT = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_COUNT;
    public static final int BUCKET_ENCODING_BASE = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_ENCODING_BASE;
    public static final int BUCKET_ENCODING_LENGTH = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_ENCODING_LENGTH;

    public static void indexSSDeepTestData(AccumuloClient accumuloClient) throws Exception {
        // configuration
        String ssdeepTableName = "ssdeepIndex";
        int ngramSize = 7;
        int minHashSize = 3;

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
                Iterator<Tuple2<NGramTuple,byte[]>> it = nGramGenerator.call(s);
                while (it.hasNext()) {
                    Tuple2<NGramTuple,byte[]> nt = it.next();
                    Tuple2<Key,Value> at = accumuloKeyGenerator.call(nt);
                    Key k = at.first();
                    Mutation m = new Mutation(k.getRow());
                    ColumnVisibility cv = new ColumnVisibility(k.getColumnVisibility());
                    m.put(k.getColumnFamily(), k.getColumnQualifier(), cv, k.getTimestamp(), at.second());
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
        final String tableName = "ssdeepIndex";

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

    @Before
    public void setUpQuery() {
        logic = new SSDeepSimilarityQueryLogic();
        logic.setTableName("ssdeepIndex");
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
    public void testSingleQuery() throws Exception {
        String query = "CHECKSUM_SSDEEP:" + TEST_SSDEEPS[2];
        EventQueryResponseBase response = runSSDeepQuery(query);
        List<EventBase> events = response.getEvents();
        int eventCount = events.size();

        Map<String,String> observedFields = new HashMap<>();
        if (eventCount > 0) {
            for (EventBase e : events) {
                List<FieldBase> fields = e.getFields();
                for (FieldBase f : fields) {
                    observedFields.put(f.getName(), f.getValueString());
                }
            }
        }

        Assert.assertFalse("Observed fields was unexpectedly empty", observedFields.isEmpty());
        Assert.assertEquals("65.0", observedFields.remove("MATCH_SCORE"));
        Assert.assertEquals("1", observedFields.remove("MATCH_RANK"));
        Assert.assertEquals("3072:02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP:02MKlWQ7Sg3d4bO968rm7JO", observedFields.remove("QUERY_SSDEEP"));
        Assert.assertEquals("3072:02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP:02MKlWQ7Sg3d4bO968rm7JO", observedFields.remove("MATCHING_SSDEEP"));
        Assert.assertTrue("Observed unexpected field(s): " + observedFields, observedFields.isEmpty());
        Assert.assertEquals(1, eventCount);
    }

    public EventQueryResponseBase runSSDeepQuery(String query) throws Exception {
        QueryImpl q = new QueryImpl();
        q.setQuery(query);
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE);
        q.setQueryAuthorizations(auths.toString());

        RunningQuery runner = new RunningQuery(accumuloClient, AccumuloConnectionFactory.Priority.NORMAL, this.logic, q, "", principal,
                        new QueryMetricFactoryImpl());
        TransformIterator transformIterator = runner.getTransformIterator();
        SSDeepSimilarityQueryTransformer transformer = (SSDeepSimilarityQueryTransformer) transformIterator.getTransformer();
        EventQueryResponseBase response = (EventQueryResponseBase) transformer.createResponse(runner.next());

        return response;
    }
}
