package datawave.query.iterator;

import static datawave.query.jexl.visitors.QueryFieldMetadataVisitor.FieldMetadata;
import static datawave.util.TableName.SHARD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.hash.HashUID;
import datawave.data.hash.UID;
import datawave.data.hash.UIDBuilder;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.data.parsers.EventKey;
import datawave.query.data.parsers.FieldIndexKey;
import datawave.query.data.parsers.KeyParser;
import datawave.query.jexl.functions.FiAggregator;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.jexl.functions.IdentityAggregator;
import datawave.query.jexl.functions.TLDFiAggregator;
import datawave.query.tld.TLDFieldIndexAggregator;
import datawave.query.util.TypeMetadata;

/**
 * Integration test for several field index related items
 */
public class FieldIndexIntegrationTest {

    private static AccumuloClient client;

    private static final Value EMPTY_VALUE = new Value();
    private static final UIDBuilder<UID> builder = HashUID.builder();

    private final EventKey eventKeyParser = new EventKey();
    private final FieldIndexKey fiKeyParser = new FieldIndexKey();

    // stuff for document aggregation
    private final Set<String> allFields = Sets.newHashSet("FIELD_A", "FIELD_B", "FIELD_C", "TLD_FIELD_A", "TLD_FIELD_B", "TLD_FIELD_C", "FIELD_X", "FIELD_Y",
                    "FIELD_Z");
    private final Map<String,Integer> fieldDocumentCounts = new HashMap<>() {
        {
            put("FIELD_A", 2);
            put("FIELD_B", 2);
            put("FIELD_C", 2);
            put("TLD_FIELD_A", 2);
            put("TLD_FIELD_B", 2);
            put("TLD_FIELD_C", 2);
            put("FIELD_X", 2);
            put("FIELD_Y", 2);
            put("FIELD_Z", 2);
        }
    };
    private final Map<String,Integer> fieldTldDocumentCounts = new HashMap<>() {
        {
            put("FIELD_A", 2);
            put("FIELD_B", 2);
            put("FIELD_C", 2);
            put("TLD_FIELD_A", 6);
            put("TLD_FIELD_B", 26);
            put("TLD_FIELD_C", 8);
            put("FIELD_X", 2);
            put("FIELD_Y", 2);
            put("FIELD_Z", 2);
        }
    };

    private final Map<String,Integer> fieldTldDocumentSeekingCounts = new HashMap<>() {
        {
            put("FIELD_A", 2);
            put("FIELD_B", 2);
            put("FIELD_C", 2);
            put("TLD_FIELD_A", 2);
            put("TLD_FIELD_B", 2);
            put("TLD_FIELD_C", 2);
            put("FIELD_X", 2);
            put("FIELD_Y", 2);
            put("FIELD_Z", 2);
        }
    };

    //  @formatter:off
    private final Multimap<String,String> expectedTopKeys = new ImmutableMultimap.Builder<String, String>()
                    //  normal top keys
                    .put("FIELD_A", "row datatype\0-4dlopf.-6pcfp5.-1luwf4 FIELD_A\0value")
                    .put("FIELD_A", "row datatype\0qzzaju.bdkxzi.-ggrsmj FIELD_A\0value")
                    .put("FIELD_B", "row datatype\0-4dlopf.-6pcfp5.-1luwf4 FIELD_B\0value")
                    .put("FIELD_B", "row datatype\0dvejjd.ntqve1.qqru8x FIELD_B\0value")
                    .put("FIELD_B", "row datatype\0qzzaju.bdkxzi.-ggrsmj FIELD_B\0value")
                    .put("FIELD_C", "row datatype\0-4dlopf.-6pcfp5.-1luwf4 FIELD_C\0value")
                    .put("FIELD_C", "row datatype\0-pvmerz.-fnxapi.5qm80s FIELD_C\0value")
                    .put("FIELD_C", "row datatype\0dvejjd.ntqve1.qqru8x FIELD_C\0value")
                    .put("FIELD_C", "row datatype\0qzzaju.bdkxzi.-ggrsmj FIELD_C\0value")
                    //  tld top keys
                    .put("TLD_FIELD_A", "row datatype\0-4dlopf.-6pcfp5.-1luwf4 TLD_FIELD_A\0value")
                    .put("TLD_FIELD_A", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.1 TLD_FIELD_A\0value")
                    .put("TLD_FIELD_A", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.2 TLD_FIELD_A\0value")
                    .put("TLD_FIELD_A", "row datatype\0dvejjd.ntqve1.qqru8x TLD_FIELD_A\0value")
                    .put("TLD_FIELD_A", "row datatype\0dvejjd.ntqve1.qqru8x.1 TLD_FIELD_A\0value")
                    .put("TLD_FIELD_A", "row datatype\0dvejjd.ntqve1.qqru8x.2 TLD_FIELD_A\0value")
                    .put("TLD_FIELD_A", "row datatype\0qzzaju.bdkxzi.-ggrsmj TLD_FIELD_A\0value")
                    .put("TLD_FIELD_A", "row datatype\0qzzaju.bdkxzi.-ggrsmj.1 TLD_FIELD_A\0value")
                    .put("TLD_FIELD_A", "row datatype\0qzzaju.bdkxzi.-ggrsmj.2 TLD_FIELD_A\0value")

                    .put("TLD_FIELD_B", "row datatype\0-4dlopf.-6pcfp5.-1luwf4 TLD_FIELD_B\0value")
                    .put("TLD_FIELD_B", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.1 TLD_FIELD_B\0value")
                    .put("TLD_FIELD_B", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.2 TLD_FIELD_B\0value")
                    .put("TLD_FIELD_B", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.3 TLD_FIELD_B\0value")
                    .put("TLD_FIELD_B", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.4 TLD_FIELD_B\0value")
                    .put("TLD_FIELD_B", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.5 TLD_FIELD_B\0value")
                    .put("TLD_FIELD_B", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.6 TLD_FIELD_B\0value")
                    .put("TLD_FIELD_B", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.7 TLD_FIELD_B\0value")
                    .put("TLD_FIELD_B", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.8 TLD_FIELD_B\0value")
                    .put("TLD_FIELD_B", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.9 TLD_FIELD_B\0value")
                    .put("TLD_FIELD_B", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.10 TLD_FIELD_B\0value")
                    .put("TLD_FIELD_B", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.11 TLD_FIELD_B\0value")
                    .put("TLD_FIELD_B", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.12 TLD_FIELD_B\0value")

                    .put("TLD_FIELD_C", "row datatype\0-4dlopf.-6pcfp5.-1luwf4 TLD_FIELD_C\0value")
                    .put("TLD_FIELD_C", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.1 TLD_FIELD_C\0value")
                    .put("TLD_FIELD_C", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.2 TLD_FIELD_C\0value")
                    .put("TLD_FIELD_C", "row datatype\0-4dlopf.-6pcfp5.-1luwf4.3 TLD_FIELD_C\0value")

                    .put("TLD_FIELD_C", "row datatype\0dvejjd.ntqve1.qqru8x TLD_FIELD_C\0value")
                    .put("TLD_FIELD_C", "row datatype\0dvejjd.ntqve1.qqru8x.1 TLD_FIELD_C\0value")
                    .put("TLD_FIELD_C", "row datatype\0dvejjd.ntqve1.qqru8x.2 TLD_FIELD_C\0value")
                    .put("TLD_FIELD_C", "row datatype\0dvejjd.ntqve1.qqru8x.3 TLD_FIELD_C\0value")

                    .put("TLD_FIELD_C", "row datatype\0qzzaju.bdkxzi.-ggrsmj TLD_FIELD_C\0value")
                    .put("TLD_FIELD_C", "row datatype\0qzzaju.bdkxzi.-ggrsmj.1 TLD_FIELD_C\0value")
                    .put("TLD_FIELD_C", "row datatype\0qzzaju.bdkxzi.-ggrsmj.2 TLD_FIELD_C\0value")
                    .put("TLD_FIELD_C", "row datatype\0qzzaju.bdkxzi.-ggrsmj.3 TLD_FIELD_C\0value")

                    //  extra top keys
                    .put("FIELD_X", "row datatype\0-4dlopf.-6pcfp5.-1luwf4 FIELD_X\0value")
                    .put("FIELD_Y", "row datatype\0-4dlopf.-6pcfp5.-1luwf4 FIELD_Y\0value")
                    .put("FIELD_Z", "row datatype\0-4dlopf.-6pcfp5.-1luwf4 FIELD_Z\0value")
                    .build();
    //  @formatter:on

    private final AttributeFactory attributeFactory = new AttributeFactory(new TypeMetadata());

    @BeforeClass
    public static void setup() throws Exception {
        client = new InMemoryAccumuloClient("", new InMemoryInstance());
        client.tableOperations().create(SHARD);

        loadFieldIndexData();
    }

    private static void loadFieldIndexData() throws Exception {
        BatchWriterConfig cfg = new BatchWriterConfig();
        cfg.setMaxLatency(10, TimeUnit.SECONDS);
        cfg.setMaxMemory(100000L);
        cfg.setMaxWriteThreads(1);
        try (BatchWriter bw = client.createBatchWriter(SHARD, cfg)) {

            writeEventData(bw, "FIELD_A", "value", 2);
            writeEventData(bw, "FIELD_B", "value", 3);
            writeEventData(bw, "FIELD_C", "value", 4);

            writeTldData(bw, "TLD_FIELD_A", "value", 3, 2);
            writeTldData(bw, "TLD_FIELD_B", "value", 1, 12);
            writeTldData(bw, "TLD_FIELD_C", "value", 3, 3);

            writeEventData(bw, "FIELD_X", "value");
            writeEventData(bw, "FIELD_Y", "value");
            writeEventData(bw, "FIELD_Z", "value");

            bw.flush();
        }
    }

    private static void writeEventData(BatchWriter bw, String field, String value) throws Exception {
        writeEventData(bw, field, value, 1);
    }

    private static void writeEventData(BatchWriter bw, String field, String value, int uidsPerValue) throws Exception {
        UID next;
        Mutation m = new Mutation("row");
        for (int i = 0; i < uidsPerValue; i++) {
            next = builder.newId((value + "_" + (i + 1)).getBytes());
            m.put(new Text("fi\0" + field), new Text(value + '\u0000' + "datatype" + '\u0000' + next), EMPTY_VALUE);
        }
        bw.addMutation(m);
    }

    private static void writeTldData(BatchWriter bw, String field, String value, int uidsPerValue) throws Exception {
        writeTldData(bw, field, value, uidsPerValue, 0);
    }

    private static void writeTldData(BatchWriter bw, String field, String value, int uidsPerValue, int numberOfChildren) throws Exception {
        UID next;
        Mutation m = new Mutation("row");
        for (int i = 0; i < uidsPerValue; i++) {

            // write tld first
            next = builder.newId((value + "_" + (i + 1)).getBytes());
            m.put(new Text("fi\0" + field), new Text(value + '\u0000' + "datatype" + '\u0000' + next), EMPTY_VALUE);

            // write any child uids, if any
            for (int j = 0; j < numberOfChildren; j++) {
                next = builder.newId((value + "_" + (i + 1)).getBytes(), String.valueOf(j + 1));
                m.put(new Text("fi\0" + field), new Text(value + '\u0000' + "datatype" + '\u0000' + next), EMPTY_VALUE);
            }
        }
        bw.addMutation(m);
    }

    @Test
    public void verifyKeys() throws Exception {
        Key start = new Key("row", "fi\0FIELD_A");
        Key stop = new Key("row", "fi\0\uffff");
        Range range = new Range(start, true, stop, true);

        BatchScanner scanner = client.createBatchScanner(SHARD);
        scanner.setRanges(Collections.singleton(range));

        int count = 0;
        for (Map.Entry<Key,Value> entry : scanner) {
            // System.out.println(entry.getKey().toStringNoTime());
            count++;
        }

        assertEquals(46, count);
    }

    @Test
    public void testPerFieldCountsAndUids() throws Exception {
        Set<String> expectedUids = Sets.newHashSet("-4dlopf.-6pcfp5.-1luwf4", "qzzaju.bdkxzi.-ggrsmj");
        driveIterator("FIELD_A", 2, expectedUids);
    }

    /**
     * Test iterates across a series of field ranges
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testPerFieldCountsWithoutAggregation() throws Exception {
        driveIterator("FIELD_A", 2);
        driveIterator("FIELD_B", 3);
        driveIterator("FIELD_C", 4);

        driveIterator("TLD_FIELD_A", 9);
        driveIterator("TLD_FIELD_B", 13);
        driveIterator("TLD_FIELD_C", 12);

        driveIterator("FIELD_X", 1);
        driveIterator("FIELD_Y", 1);
        driveIterator("FIELD_Z", 1);
    }

    /**
     * Test aggregation using an {@link IdentityAggregator} with no additional configuration
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testPerFieldCountsWithIdentityAggregator() throws Exception {
        IdentityAggregator aggregator = new IdentityAggregator();
        driveAggregator(aggregator, "FIELD_A", 2);
        driveAggregator(aggregator, "FIELD_B", 3);
        driveAggregator(aggregator, "FIELD_C", 4);

        driveAggregator(aggregator, "TLD_FIELD_A", 9);
        driveAggregator(aggregator, "TLD_FIELD_B", 13);
        driveAggregator(aggregator, "TLD_FIELD_C", 12);

        driveAggregator(aggregator, "FIELD_X", 1);
        driveAggregator(aggregator, "FIELD_Y", 1);
        driveAggregator(aggregator, "FIELD_Z", 1);
    }

    /**
     * Tests aggregation using an {@link IdentityAggregator} that is configured to seek
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testPerFieldCountsWithSeekingIdentityAggregator() throws Exception {
        IdentityAggregator aggregator = new IdentityAggregator(allFields, null, 1);
        driveAggregator(aggregator, "FIELD_A", 2);
        driveAggregator(aggregator, "FIELD_B", 3);
        driveAggregator(aggregator, "FIELD_C", 4);

        driveAggregator(aggregator, "TLD_FIELD_A", 9);
        driveAggregator(aggregator, "TLD_FIELD_B", 13);
        driveAggregator(aggregator, "TLD_FIELD_C", 12);

        driveAggregator(aggregator, "FIELD_X", 1);
        driveAggregator(aggregator, "FIELD_Y", 1);
        driveAggregator(aggregator, "FIELD_Z", 1);
    }

    /**
     * Test aggregation using a {@link TLDFieldIndexAggregator} with minimal configuration
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testPerFieldCountsWithTLDFieldIndexAggregator() throws Exception {
        TLDFieldIndexAggregator aggregator = new TLDFieldIndexAggregator(allFields, null);
        driveAggregator(aggregator, "FIELD_A", 2);
        driveAggregator(aggregator, "FIELD_B", 3);
        driveAggregator(aggregator, "FIELD_C", 4);

        // TLD aggregator treats all child uids as part of the parent, thus a lower count
        driveAggregator(aggregator, "TLD_FIELD_A", 3);
        driveAggregator(aggregator, "TLD_FIELD_B", 1);
        driveAggregator(aggregator, "TLD_FIELD_C", 3);

        driveAggregator(aggregator, "FIELD_X", 1);
        driveAggregator(aggregator, "FIELD_Y", 1);
        driveAggregator(aggregator, "FIELD_Z", 1);
    }

    /**
     * Test aggregating using a {@link TLDFieldIndexAggregator} configured to seek
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testPerFieldCountsWithTLDFieldIndexAggregatorWithSeeking() throws Exception {
        TLDFieldIndexAggregator aggregator = new TLDFieldIndexAggregator(allFields, null, 1);
        driveAggregator(aggregator, "FIELD_A", 2);
        driveAggregator(aggregator, "FIELD_B", 3);
        driveAggregator(aggregator, "FIELD_C", 4);

        // TLD aggregator treats all child uids as part of the parent, thus a lower count
        driveAggregator(aggregator, "TLD_FIELD_A", 3);
        driveAggregator(aggregator, "TLD_FIELD_B", 1);
        driveAggregator(aggregator, "TLD_FIELD_C", 3);

        driveAggregator(aggregator, "FIELD_X", 1);
        driveAggregator(aggregator, "FIELD_Y", 1);
        driveAggregator(aggregator, "FIELD_Z", 1);
    }

    /**
     * Test document aggregation using an {@link IdentityAggregator} with no configuration
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testDocumentAggregationWithIdentityAggregator() throws Exception {
        IdentityAggregator aggregator = new IdentityAggregator();
        driveDocumentAggregation(aggregator, "FIELD_A", 2);
        driveDocumentAggregation(aggregator, "FIELD_B", 3);
        driveDocumentAggregation(aggregator, "FIELD_C", 4);

        driveDocumentAggregation(aggregator, "TLD_FIELD_A", 9);
        driveDocumentAggregation(aggregator, "TLD_FIELD_B", 13);
        driveDocumentAggregation(aggregator, "TLD_FIELD_C", 12);

        driveDocumentAggregation(aggregator, "FIELD_X", 1);
        driveDocumentAggregation(aggregator, "FIELD_Y", 1);
        driveDocumentAggregation(aggregator, "FIELD_Z", 1);
    }

    /**
     * Test document aggregation using an {@link IdentityAggregator} configured to seek
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testDocumentAggregationWithSeekingIdentityAggregator() throws Exception {
        IdentityAggregator aggregator = new IdentityAggregator(allFields, null, 1);
        driveDocumentAggregation(aggregator, "FIELD_A", 2);
        driveDocumentAggregation(aggregator, "FIELD_B", 3);
        driveDocumentAggregation(aggregator, "FIELD_C", 4);

        driveDocumentAggregation(aggregator, "TLD_FIELD_A", 9);
        driveDocumentAggregation(aggregator, "TLD_FIELD_B", 13);
        driveDocumentAggregation(aggregator, "TLD_FIELD_C", 12);

        driveDocumentAggregation(aggregator, "FIELD_X", 1);
        driveDocumentAggregation(aggregator, "FIELD_Y", 1);
        driveDocumentAggregation(aggregator, "FIELD_Z", 1);
    }

    /**
     * Test document aggregation using a {@link TLDFieldIndexAggregator} with no configuration
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testDocumentAggregationWithTLDFieldIndexAggregator() throws Exception {
        TLDFieldIndexAggregator aggregator = new TLDFieldIndexAggregator(allFields, null);
        driveDocumentAggregation(aggregator, "FIELD_A", 2);
        driveDocumentAggregation(aggregator, "FIELD_B", 3);
        driveDocumentAggregation(aggregator, "FIELD_C", 4);

        driveDocumentAggregation(aggregator, "TLD_FIELD_A", 3);
        driveDocumentAggregation(aggregator, "TLD_FIELD_B", 1);
        driveDocumentAggregation(aggregator, "TLD_FIELD_C", 3);

        driveDocumentAggregation(aggregator, "FIELD_X", 1);
        driveDocumentAggregation(aggregator, "FIELD_Y", 1);
        driveDocumentAggregation(aggregator, "FIELD_Z", 1);
    }

    /**
     * Test document aggregation using {@link TLDFieldIndexAggregator} configured to seek
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testDocumentAggregationWithTLDFieldIndexAggregatorWithSeeking() throws Exception {
        TLDFieldIndexAggregator aggregator = new TLDFieldIndexAggregator(allFields, null, 1);
        driveDocumentAggregation(aggregator, "FIELD_A", 2);
        driveDocumentAggregation(aggregator, "FIELD_B", 3);
        driveDocumentAggregation(aggregator, "FIELD_C", 4);

        driveDocumentAggregation(aggregator, "TLD_FIELD_A", 3);
        driveDocumentAggregation(aggregator, "TLD_FIELD_B", 1);
        driveDocumentAggregation(aggregator, "TLD_FIELD_C", 3);

        driveDocumentAggregation(aggregator, "FIELD_X", 1);
        driveDocumentAggregation(aggregator, "FIELD_Y", 1);
        driveDocumentAggregation(aggregator, "FIELD_Z", 1);
    }

    /**
     * Test aggregation using an {@link IdentityAggregator} with no additional configuration
     * <p>
     * This aggregation is not configured to seek
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testSeekingAggregationWithIdentityAggregator() throws Exception {
        IdentityAggregator aggregator = new IdentityAggregator();
        driveSeekingAggregation(aggregator, "FIELD_A", 2);
        driveSeekingAggregation(aggregator, "FIELD_B", 3);
        driveSeekingAggregation(aggregator, "FIELD_C", 4);

        driveSeekingAggregation(aggregator, "TLD_FIELD_A", 9);
        driveSeekingAggregation(aggregator, "TLD_FIELD_B", 13);
        driveSeekingAggregation(aggregator, "TLD_FIELD_C", 12);

        driveSeekingAggregation(aggregator, "FIELD_X", 1);
        driveSeekingAggregation(aggregator, "FIELD_Y", 1);
        driveSeekingAggregation(aggregator, "FIELD_Z", 1);
    }

    /**
     * Test aggregation using an {@link IdentityAggregator} with no additional configuration
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testSeekingAggregationWithSeekingIdentityAggregator() throws Exception {
        IdentityAggregator aggregator = new IdentityAggregator(allFields, null, 1);
        driveSeekingAggregation(aggregator, "FIELD_A", 2);
        driveSeekingAggregation(aggregator, "FIELD_B", 3);
        driveSeekingAggregation(aggregator, "FIELD_C", 4);

        driveSeekingAggregation(aggregator, "TLD_FIELD_A", 9);
        driveSeekingAggregation(aggregator, "TLD_FIELD_B", 13);
        driveSeekingAggregation(aggregator, "TLD_FIELD_C", 12);

        driveSeekingAggregation(aggregator, "FIELD_X", 1);
        driveSeekingAggregation(aggregator, "FIELD_Y", 1);
        driveSeekingAggregation(aggregator, "FIELD_Z", 1);
    }

    /**
     * Test aggregation using a {@link TLDFieldIndexAggregator} with minimal configuration
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testSeekingAggregationTLDFieldIndexAggregator() throws Exception {
        TLDFieldIndexAggregator aggregator = new TLDFieldIndexAggregator(allFields, null);
        driveSeekingAggregation(aggregator, "FIELD_A", 2);
        driveSeekingAggregation(aggregator, "FIELD_B", 3);
        driveSeekingAggregation(aggregator, "FIELD_C", 4);

        // TLD aggregator treats all child uids as part of the parent, thus a lower count
        driveSeekingAggregation(aggregator, "TLD_FIELD_A", 3);
        driveSeekingAggregation(aggregator, "TLD_FIELD_B", 1);
        driveSeekingAggregation(aggregator, "TLD_FIELD_C", 3);

        driveSeekingAggregation(aggregator, "FIELD_X", 1);
        driveSeekingAggregation(aggregator, "FIELD_Y", 1);
        driveSeekingAggregation(aggregator, "FIELD_Z", 1);
    }

    /**
     * Test aggregation using a {@link TLDFieldIndexAggregator} configured to seek
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testSeekingAggregationTLDFieldIndexAggregatorWithSeeking() throws Exception {
        TLDFieldIndexAggregator aggregator = new TLDFieldIndexAggregator(allFields, null, 1);
        driveSeekingAggregation(aggregator, "FIELD_A", 2);
        driveSeekingAggregation(aggregator, "FIELD_B", 3);
        driveSeekingAggregation(aggregator, "FIELD_C", 4);

        // TLD aggregator treats all child uids as part of the parent, thus a lower count
        driveSeekingAggregation(aggregator, "TLD_FIELD_A", 3);
        driveSeekingAggregation(aggregator, "TLD_FIELD_B", 1);
        driveSeekingAggregation(aggregator, "TLD_FIELD_C", 3);

        driveSeekingAggregation(aggregator, "FIELD_X", 1);
        driveSeekingAggregation(aggregator, "FIELD_Y", 1);
        driveSeekingAggregation(aggregator, "FIELD_Z", 1);
    }

    // tests to verify that the FiAggregator functions just like the IdentityAggregator

    @Test
    public void testFiAggregator() throws Exception {
        FiAggregator aggregator = new FiAggregator();
        driveAggregator(aggregator, "FIELD_A", 2);
        driveAggregator(aggregator, "FIELD_B", 3);
        driveAggregator(aggregator, "FIELD_C", 4);

        driveAggregator(aggregator, "TLD_FIELD_A", 9);
        driveAggregator(aggregator, "TLD_FIELD_B", 13);
        driveAggregator(aggregator, "TLD_FIELD_C", 12);

        driveAggregator(aggregator, "FIELD_X", 1);
        driveAggregator(aggregator, "FIELD_Y", 1);
        driveAggregator(aggregator, "FIELD_Z", 1);
    }

    @Test
    public void testFiAggregator_withSeeking() throws Exception {
        FiAggregator aggregator = new FiAggregator();
        aggregator.withMaxNextCount(1);

        driveAggregator(aggregator, "FIELD_A", 2);
        driveAggregator(aggregator, "FIELD_B", 3);
        driveAggregator(aggregator, "FIELD_C", 4);

        driveAggregator(aggregator, "TLD_FIELD_A", 9);
        driveAggregator(aggregator, "TLD_FIELD_B", 13);
        driveAggregator(aggregator, "TLD_FIELD_C", 12);

        driveAggregator(aggregator, "FIELD_X", 1);
        driveAggregator(aggregator, "FIELD_Y", 1);
        driveAggregator(aggregator, "FIELD_Z", 1);
    }

    @Test
    public void testFiAggregator_aggregateDocuments() throws Exception {
        FiAggregator aggregator = new FiAggregator();
        driveDocumentAggregation(aggregator, "FIELD_A", 2);
        driveDocumentAggregation(aggregator, "FIELD_B", 3);
        driveDocumentAggregation(aggregator, "FIELD_C", 4);

        driveDocumentAggregation(aggregator, "TLD_FIELD_A", 9);
        driveDocumentAggregation(aggregator, "TLD_FIELD_B", 13);
        driveDocumentAggregation(aggregator, "TLD_FIELD_C", 12);

        driveDocumentAggregation(aggregator, "FIELD_X", 1);
        driveDocumentAggregation(aggregator, "FIELD_Y", 1);
        driveDocumentAggregation(aggregator, "FIELD_Z", 1);
    }

    @Test
    public void testFiAggregator_aggregateDocuments_withSeeking() throws Exception {
        FieldMetadata fieldMetadata = new FieldMetadata(allFields, allFields, allFields);
        FiAggregator aggregator = new FiAggregator();
        aggregator.withMaxNextCount(1);
        aggregator.withFieldMetadata(fieldMetadata);

        driveDocumentAggregation(aggregator, "FIELD_A", 2);
        driveDocumentAggregation(aggregator, "FIELD_B", 3);
        driveDocumentAggregation(aggregator, "FIELD_C", 4);

        driveDocumentAggregation(aggregator, "TLD_FIELD_A", 3); // was 9
        driveDocumentAggregation(aggregator, "TLD_FIELD_B", 1); // was 13
        driveDocumentAggregation(aggregator, "TLD_FIELD_C", 3); // was 12

        driveDocumentAggregation(aggregator, "FIELD_X", 1);
        driveDocumentAggregation(aggregator, "FIELD_Y", 1);
        driveDocumentAggregation(aggregator, "FIELD_Z", 1);
    }

    @Test
    public void testFiAggregator_seekingAggregation() throws Exception {
        FiAggregator aggregator = new FiAggregator();
        driveSeekingAggregation(aggregator, "FIELD_A", 2);
        driveSeekingAggregation(aggregator, "FIELD_B", 3);
        driveSeekingAggregation(aggregator, "FIELD_C", 4);

        driveSeekingAggregation(aggregator, "TLD_FIELD_A", 9);
        driveSeekingAggregation(aggregator, "TLD_FIELD_B", 13);
        driveSeekingAggregation(aggregator, "TLD_FIELD_C", 12);

        driveSeekingAggregation(aggregator, "FIELD_X", 1);
        driveSeekingAggregation(aggregator, "FIELD_Y", 1);
        driveSeekingAggregation(aggregator, "FIELD_Z", 1);
    }

    @Test
    public void testFiAggregator_seekingAggregation_withSeeking() throws Exception {
        FiAggregator aggregator = new FiAggregator();
        aggregator.withMaxNextCount(1);

        driveSeekingAggregation(aggregator, "FIELD_A", 2);
        driveSeekingAggregation(aggregator, "FIELD_B", 3);
        driveSeekingAggregation(aggregator, "FIELD_C", 4);

        driveSeekingAggregation(aggregator, "TLD_FIELD_A", 9);
        driveSeekingAggregation(aggregator, "TLD_FIELD_B", 13);
        driveSeekingAggregation(aggregator, "TLD_FIELD_C", 12);

        driveSeekingAggregation(aggregator, "FIELD_X", 1);
        driveSeekingAggregation(aggregator, "FIELD_Y", 1);
        driveSeekingAggregation(aggregator, "FIELD_Z", 1);
    }

    // now test the TLDFiAggregator

    @Test
    public void testTLDFiAggregator() throws Exception {
        TLDFiAggregator aggregator = new TLDFiAggregator();

        driveAggregator(aggregator, "FIELD_A", 2);
        driveAggregator(aggregator, "FIELD_B", 3);
        driveAggregator(aggregator, "FIELD_C", 4);

        // TLD aggregator treats all child uids as part of the parent, thus a lower count
        driveAggregator(aggregator, "TLD_FIELD_A", 3);
        driveAggregator(aggregator, "TLD_FIELD_B", 1);
        driveAggregator(aggregator, "TLD_FIELD_C", 3);

        driveAggregator(aggregator, "FIELD_X", 1);
        driveAggregator(aggregator, "FIELD_Y", 1);
        driveAggregator(aggregator, "FIELD_Z", 1);
    }

    @Test
    public void testTLDFiAggregator_withSeeking() throws Exception {
        TLDFiAggregator aggregator = new TLDFiAggregator();
        aggregator.withMaxNextCount(1);

        driveAggregator(aggregator, "FIELD_A", 2);
        driveAggregator(aggregator, "FIELD_B", 3);
        driveAggregator(aggregator, "FIELD_C", 4);

        // TLD aggregator treats all child uids as part of the parent, thus a lower count
        driveAggregator(aggregator, "TLD_FIELD_A", 3);
        driveAggregator(aggregator, "TLD_FIELD_B", 1);
        driveAggregator(aggregator, "TLD_FIELD_C", 3);

        driveAggregator(aggregator, "FIELD_X", 1);
        driveAggregator(aggregator, "FIELD_Y", 1);
        driveAggregator(aggregator, "FIELD_Z", 1);
    }

    @Test
    public void testTLDFiAggregator_aggregateDocuments() throws Exception {
        TLDFiAggregator aggregator = new TLDFiAggregator();
        driveDocumentAggregation(aggregator, "FIELD_A", 2);
        driveDocumentAggregation(aggregator, "FIELD_B", 3);
        driveDocumentAggregation(aggregator, "FIELD_C", 4);

        driveDocumentAggregation(aggregator, "TLD_FIELD_A", 3);
        driveDocumentAggregation(aggregator, "TLD_FIELD_B", 1);
        driveDocumentAggregation(aggregator, "TLD_FIELD_C", 3);

        driveDocumentAggregation(aggregator, "FIELD_X", 1);
        driveDocumentAggregation(aggregator, "FIELD_Y", 1);
        driveDocumentAggregation(aggregator, "FIELD_Z", 1);
    }

    @Test
    public void testTLDFiAggregator_aggregateDocuments_withSeeking() throws Exception {
        FieldMetadata fieldMetadata = new FieldMetadata(allFields, allFields, allFields);
        TLDFiAggregator aggregator = new TLDFiAggregator();
        aggregator.withMaxNextCount(1);
        aggregator.withFieldMetadata(fieldMetadata);

        driveDocumentAggregation(aggregator, "FIELD_A", 2);
        driveDocumentAggregation(aggregator, "FIELD_B", 3);
        driveDocumentAggregation(aggregator, "FIELD_C", 4);

        driveDocumentAggregation(aggregator, "TLD_FIELD_A", 3); // was 6
        driveDocumentAggregation(aggregator, "TLD_FIELD_B", 1);
        driveDocumentAggregation(aggregator, "TLD_FIELD_C", 3);

        driveDocumentAggregation(aggregator, "FIELD_X", 1);
        driveDocumentAggregation(aggregator, "FIELD_Y", 1);
        driveDocumentAggregation(aggregator, "FIELD_Z", 1);
    }

    @Test
    public void testTLDFiAggregator_seekingAggregation() throws Exception {
        TLDFiAggregator aggregator = new TLDFiAggregator();
        driveSeekingAggregation(aggregator, "FIELD_A", 2);
        driveSeekingAggregation(aggregator, "FIELD_B", 3);
        driveSeekingAggregation(aggregator, "FIELD_C", 4);

        // TLD aggregator treats all child uids as part of the parent, thus a lower count
        driveSeekingAggregation(aggregator, "TLD_FIELD_A", 3);
        driveSeekingAggregation(aggregator, "TLD_FIELD_B", 1);
        driveSeekingAggregation(aggregator, "TLD_FIELD_C", 3);

        driveSeekingAggregation(aggregator, "FIELD_X", 1);
        driveSeekingAggregation(aggregator, "FIELD_Y", 1);
        driveSeekingAggregation(aggregator, "FIELD_Z", 1);
    }

    @Test
    public void testTLDFiAggregator_seekingAggregation_withSeeking() throws Exception {
        TLDFiAggregator aggregator = new TLDFiAggregator();
        aggregator.withMaxNextCount(1);

        driveSeekingAggregation(aggregator, "FIELD_A", 2);
        driveSeekingAggregation(aggregator, "FIELD_B", 3);
        driveSeekingAggregation(aggregator, "FIELD_C", 4);

        // TLD aggregator treats all child uids as part of the parent, thus a lower count
        driveSeekingAggregation(aggregator, "TLD_FIELD_A", 3);
        driveSeekingAggregation(aggregator, "TLD_FIELD_B", 1);
        driveSeekingAggregation(aggregator, "TLD_FIELD_C", 3);

        driveSeekingAggregation(aggregator, "FIELD_X", 1);
        driveSeekingAggregation(aggregator, "FIELD_Y", 1);
        driveSeekingAggregation(aggregator, "FIELD_Z", 1);
    }

    /**
     * Constructs an iterator for the specified field
     *
     * @param field
     *            the field
     * @return an iterator ready to go!
     * @throws Exception
     *             if something goes wrong
     */
    private SortedKeyValueIterator<Key,Value> createIteratorForRange(Range range) throws Exception {
        Scanner scanner = client.createScanner(SHARD);
        scanner.setRange(range);
        Iterator<Map.Entry<Key,Value>> iter = scanner.iterator();

        SortedListKeyValueIterator skvi = new SortedListKeyValueIterator(iter);
        skvi.seek(range, Collections.emptySet(), true);

        return skvi;
    }

    private Range createRangeForField(String field) {
        Key start = new Key("row", "fi\0" + field);
        Key stop = new Key("row", "fi\0" + field + '\uffff');
        return new Range(start, true, stop, true);
    }

    /**
     * Method to drive an iterator across a field-bounded fi range.
     * <p>
     * Asserts expected number of uids
     *
     * @param field
     *            the field
     * @param expectedUidCount
     *            the expected number of uids
     * @throws Exception
     *             if something goes wrong
     */
    private void driveIterator(String field, int expectedUidCount) throws Exception {
        driveIterator(field, expectedUidCount, Collections.emptySet());
    }

    /**
     * Method to drive an iterator across a field-bounded fi range.
     * <p>
     * Asserts uid count and optionally the uids themselves
     *
     * @param field
     *            a field
     * @param expectedUidCount
     *            the expected number of uids
     * @param expectedUids
     *            optionally, the expected uids
     * @throws Exception
     *             if something goes wrong
     */
    private void driveIterator(String field, int expectedUidCount, Set<String> expectedUids) throws Exception {
        int uidCount = 0;
        Set<String> uids = new HashSet<>();

        Range range = createRangeForField(field);
        SortedKeyValueIterator<Key,Value> iter = createIteratorForRange(range);
        while (iter.hasTop()) {
            Key k = iter.getTopKey();

            // increment count and parse uid
            uidCount++;
            parseUid(k, fiKeyParser, uids);
            iter.next();
        }

        assertEquals(expectedUidCount, uidCount);
        assertEquals(expectedUidCount, uids.size());

        if (!expectedUids.isEmpty()) {
            assertEquals(expectedUids, uids);
        }
    }

    /**
     * Method drives an iterator with an aggregator
     * <p>
     * Asserts uid count
     *
     * @param aggregator
     *            a FieldIndexAggregator
     * @param field
     *            a field
     * @param expectedUidCount
     *            the expected uid count
     * @throws Exception
     *             if something goes wrong
     */
    private void driveAggregator(FieldIndexAggregator aggregator, String field, int expectedUidCount) throws Exception {
        driveAggregator(aggregator, field, expectedUidCount, Collections.emptySet());
    }

    /**
     * Method drives an iterator with an aggregator
     * <p>
     * Asserts uid count and optionally the uids themselves
     *
     * @param aggregator
     *            a FieldIndexAggregator
     * @param field
     *            a field
     * @param expectedUidCount
     *            the expected uid count
     * @param expectedUids
     *            optionally, the expected uids
     * @throws Exception
     *             if something goes wrong
     */
    private void driveAggregator(FieldIndexAggregator aggregator, String field, int expectedUidCount, Set<String> expectedUids) throws Exception {
        int uidCount = 0;
        Set<String> uids = new HashSet<>();

        Range range = createRangeForField(field);
        SortedKeyValueIterator<Key,Value> iter = createIteratorForRange(range);
        while (iter.hasTop()) {
            Key k = aggregator.apply(iter);

            // increment count and parse uid
            uidCount++;
            assertTopKeyForField(field, k);
            parseUid(k, eventKeyParser, uids);
        }

        assertEquals(expectedUidCount, uidCount);
        assertEquals(expectedUidCount, uids.size());

        if (!expectedUids.isEmpty()) {
            assertEquals(expectedUids, uids);
        }
    }

    /**
     * Method drives an iterator with an aggregator that aggregates keys into a document
     * <p>
     * Assert uid count and possibly some document stuff
     *
     * @param aggregator
     *            a FieldIndexAggregator
     * @param field
     *            a field
     * @param expectedUidCount
     *            the expected uid count
     * @throws Exception
     *             if something goes wrong
     */
    private void driveDocumentAggregation(FieldIndexAggregator aggregator, String field, int expectedUidCount) throws Exception {
        int uidCount = 0;

        Range range = createRangeForField(field);
        if (aggregator instanceof FiAggregator) {
            FiAggregator fiAgg = (FiAggregator) aggregator;
            fiAgg.setSeekRange(range);
            fiAgg.setColumnFamilies(Collections.emptySet());
        }

        SortedKeyValueIterator<Key,Value> iter = createIteratorForRange(range);
        while (iter.hasTop()) {
            Document d = new Document();
            Key k = aggregator.apply(iter, d, attributeFactory);
            assertTopKeyForField(field, k);

            uidCount++;
            assertTopKeyForField(field, k);
            assertDocumentSize(aggregator, field, d);
        }

        assertEquals(expectedUidCount, uidCount);
    }

    /**
     * Method drives an iterator with an aggregator, no document is aggregated but the source may be seeked
     *
     * @param aggregator
     *            a FieldIndexAggregator
     * @param field
     *            a field
     * @param expectedUidCount
     *            the expected uid count
     * @throws Exception
     *             if something goes wrong
     */
    private void driveSeekingAggregation(FieldIndexAggregator aggregator, String field, int expectedUidCount) throws Exception {
        int uidCount = 0;
        Set<String> uids = new HashSet<>();

        Range range = createRangeForField(field);
        SortedKeyValueIterator<Key,Value> iter = createIteratorForRange(range);
        while (iter.hasTop()) {
            Key k = aggregator.apply(iter, range, Collections.emptySet(), true);
            assertTopKeyForField(field, k);

            uidCount++;
            parseUid(k, eventKeyParser, uids);
        }

        assertEquals(expectedUidCount, uidCount);
        assertEquals(expectedUidCount, uids.size());
    }

    private void parseUid(Key k, KeyParser parser, Set<String> uids) {
        parser.parse(k);
        uids.add(parser.getUid());
    }

    private void assertDocumentSize(FieldIndexAggregator aggregator, String field, Document d) {
        if (aggregator instanceof TLDFiAggregator && ((TLDFiAggregator) aggregator).getFieldMetadata() != null) {
            assertEquals(fieldTldDocumentSeekingCounts.get(field).intValue(), d.size());
        } else if (aggregator instanceof TLDFieldIndexAggregator || aggregator instanceof TLDFiAggregator) {
            assertEquals(fieldTldDocumentCounts.get(field).intValue(), d.size());
        } else {
            assertEquals(fieldDocumentCounts.get(field).intValue(), d.size());
        }
    }

    /**
     * Asserts that the provided top key is present in the expectedTopKeys set
     *
     * @param field
     *            the field
     * @param key
     *            the top key
     */
    private void assertTopKeyForField(String field, Key key) {

        String searchKey = key.getRow().toString() + " " + key.getColumnFamily().toString() + " " + key.getColumnQualifier().toString();

        boolean found = expectedTopKeys.get(field).contains(searchKey);
        if (!found) {
            System.out.println("key: " + searchKey);
            System.out.println("not found in");
            System.out.println(expectedTopKeys.get(field));
        }
        assertTrue(found);
    }

}
