package datawave.ingest.mapreduce.handler.facet;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import datawave.data.hash.UID;
import datawave.data.type.LcNoDiacriticsType;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandlerTest.TestContentBaseIngestHelper;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandlerTest.TestContentIndexingColumnBasedHandler;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandlerTest.TestEventRecordReader;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.AbstractContextWriter;
import datawave.ingest.test.StandaloneStatusReporter;
import datawave.ingest.test.StandaloneTaskAttemptContext;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class FacetHandlerTest {
    
    private static final String TEST_TYPE = "test";
    private static final UID TEST_UID = UID.builder().newId();
    
    private static final Logger log = Logger.getLogger(FacetHandlerTest.class);
    public static final String DATAWAVE_FACETS = "datawave.facets";
    public static final String DATAWAVE_FACET_METADATA = "datawave.facetMetadata";
    public static final String DATAWAVE_FACET_HASHES = "datawave.facetHashes";
    
    private TaskAttemptContext ctx = null;
    
    private final RawRecordContainer event = EasyMock.createMock(RawRecordContainer.class);
    private ContentBaseIngestHelper helper;
    private ColumnVisibility colVis;
    
    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("config/all-config.xml");
        conf.addResource("config/facet-config.xml");
        
        ctx = new TaskAttemptContextImpl(conf, new org.apache.hadoop.mapred.TaskAttemptID());
        ctx.getConfiguration().setInt(ContentIndexingColumnBasedHandler.NUM_SHARDS, 1);
        ctx.getConfiguration().set(ContentIndexingColumnBasedHandler.SHARD_TNAME, "shard");
        ctx.getConfiguration().set(ContentIndexingColumnBasedHandler.SHARD_GIDX_TNAME, "shardIndex");
        ctx.getConfiguration().set(ContentIndexingColumnBasedHandler.SHARD_GRIDX_TNAME, "shardIndex");
        
        ctx.getConfiguration().set("data.name", TEST_TYPE);
        ctx.getConfiguration().set("test.data.auth.id.mode", "NEVER");
        ctx.getConfiguration().set("test" + BaseIngestHelper.DEFAULT_TYPE, LcNoDiacriticsType.class.getName());
        
        ctx.getConfiguration().set("test" + TypeRegistry.HANDLER_CLASSES, TestContentIndexingColumnBasedHandler.class.getName());
        ctx.getConfiguration().set("test" + TypeRegistry.RAW_READER, TestEventRecordReader.class.getName());
        ctx.getConfiguration().set("test" + TypeRegistry.INGEST_HELPER, TestContentBaseIngestHelper.class.getName());
        ctx.getConfiguration().set(TypeRegistry.EXCLUDED_HANDLER_CLASSES, "FAKE_HANDLER_CLASS"); // it will die if this field is not faked
        
        colVis = new ColumnVisibility("");
        helper = new TestContentBaseIngestHelper();
        
        TypeRegistry.reset();
        TypeRegistry.getInstance(ctx.getConfiguration());
        
        setupMocks();
        
        configureFacets(ctx.getConfiguration());
        
        // log.setLevel(Level.DEBUG);
    }
    
    private void setupMocks() {
        try {
            long timestamp = Instant.parse("2020-04-01T08:00:00.0z").toEpochMilli();
            EasyMock.expect(event.getVisibility()).andReturn(colVis).anyTimes();
            EasyMock.expect(event.getDataType()).andReturn(TypeRegistry.getType(TEST_TYPE)).anyTimes();
            EasyMock.expect(event.getId()).andReturn(TEST_UID).anyTimes();
            EasyMock.expect(event.getDate()).andReturn(timestamp).anyTimes();
            EasyMock.expect(event.getRawFileName()).andReturn("dummy_filename.txt").anyTimes();
            EasyMock.replay(event);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public void configureFacets(Configuration conf) {
        conf.set("test.facet.category.name.make", "MAKE;STYLE,MODEL,COLOR");
        conf.set("test.facet.category.name.style", "STYLE;MODEL,COLOR");
        conf.set("test.facet.category.name.color", "COLOR;MODEL,STYLE,THISFIELDDOESNTEXIST");
    }
    
    @Test
    public void testSingleEvent() {
        ExtendedDataTypeHandler<Text,BulkIngestKey,Value> handler = new FacetHandler<>();
        handler.setup(ctx);
        
        helper.setup(ctx.getConfiguration());
        
        Multimap<String,NormalizedContentInterface> fields = TestData.getDataItem(1);
        setupTaskAttemptContext();
        processEvent(event, fields, handler);
        Multimap<String,FacetResult> keysByTable = collectResults();
        
        Set<String> expectedFacets = Stream.of(TestData.getExpectedFacetData(1)).collect(Collectors.toSet());
        Set<String> expectedFacetMetadata = Stream.of(TestData.getExpectedFacetMetadataData(1)).collect(Collectors.toSet());
        
        evaluateSingleEventResults(keysByTable, expectedFacets, expectedFacetMetadata);
    }
    
    @Test
    public void testSingleEventWithPivotPredicate() {
        testSingleEventWithPredicate("STYLE");
    }
    
    @Test
    public void testSingleEventWithFacetPredicate() {
        testSingleEventWithPredicate("MODEL");
    }
    
    public void testSingleEventWithPredicate(final String filterField) {
        FacetHandler<Text,BulkIngestKey,Value> handler = new FacetHandler<>();
        handler.setup(ctx);
        handler.setFieldSelectionPredicate(s -> !filterField.equalsIgnoreCase(s));
        
        helper.setup(ctx.getConfiguration());
        
        Multimap<String,NormalizedContentInterface> fields = TestData.getDataItem(1);
        setupTaskAttemptContext();
        processEvent(event, fields, handler);
        Multimap<String,FacetResult> keysByTable = collectResults();
        
        Set<String> expectedFacets = Stream.of(TestData.getExpectedFacetData(1)).filter(s -> !s.contains(filterField)).collect(Collectors.toSet());
        Set<String> expectedFacetMetadata = Stream.of(TestData.getExpectedFacetMetadataData(1)).filter(s -> !s.contains(filterField))
                        .collect(Collectors.toSet());
        
        evaluateSingleEventResults(keysByTable, expectedFacets, expectedFacetMetadata);
    }
    
    @Test
    public void testMultipleEvents() {
        ExtendedDataTypeHandler<Text,BulkIngestKey,Value> handler = new FacetHandler<>();
        handler.setup(ctx);
        
        helper.setup(ctx.getConfiguration());
        
        Collection<Multimap<String,NormalizedContentInterface>> items = TestData.getDataItems();
        int size = items.size();
        
        setupTaskAttemptContext();
        items.forEach(f -> processEvent(event, f, handler));
        Multimap<String,FacetResult> keysByTable = collectResults();
        
        Object2IntMap<String> expectedFacetKeyCounts = TestData.getExpectedFacetCounts();
        evaluateMultipleEventResults(keysByTable, size, expectedFacetKeyCounts);
    }
    
    @Test
    public void testMultiValueHashing() {
        
        ctx.getConfiguration().set("test.facet.hash.threshold", "2");
        
        ExtendedDataTypeHandler<Text,BulkIngestKey,Value> handler = new FacetHandler<>();
        handler.setup(ctx);
        
        Collection<Multimap<String,NormalizedContentInterface>> items = TestData.generateMultivaluedColorData();
        
        setupTaskAttemptContext();
        items.forEach(f -> processEvent(event, f, handler));
        Multimap<String,FacetResult> keysByTable = collectResults();
        
        Object2IntMap<String> expectedFacetKeyCounts = TestData.getExpectedHashedFacetCounts();
        evaluateMultiValueResults(keysByTable, TestData.getExpectedFacetHashes(), expectedFacetKeyCounts);
    }
    
    StandaloneTaskAttemptContext<Text,RawRecordContainerImpl,BulkIngestKey,Value> context;
    CachingContextWriter contextWriter;
    
    private void setupTaskAttemptContext() {
        context = new StandaloneTaskAttemptContext<>(ctx.getConfiguration(), new StandaloneStatusReporter());
        contextWriter = new CachingContextWriter();
        try {
            contextWriter.setup(context.getConfiguration(), false);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Error setting up context writer", e);
        }
    }
    
    /**
     * Process a single event using the handler
     *
     * @param event
     *            the event we're processing
     * @param eventFields
     *            the fields from the event to process
     * @param handler
     *            the handler to do the processing
     */
    private void processEvent(RawRecordContainer event, Multimap<String,NormalizedContentInterface> eventFields,
                    ExtendedDataTypeHandler<Text,BulkIngestKey,Value> handler) {
        
        assertNotNull("Event was null", event);
        
        try {
            handler.process(null, event, eventFields, context, contextWriter);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Error processing event data", e);
        }
    }
    
    /**
     * @return a reference to the keysByTable structure passed in to the method
     */
    private Multimap<String,FacetResult> collectResults() {
        try {
            contextWriter.commit(context);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        
        final Multimap<String,FacetResult> keysByTable = LinkedListMultimap.create();
        
        for (Map.Entry<BulkIngestKey,Value> entry : contextWriter.getCache().entries()) {
            keysByTable.put(entry.getKey().getTableName().toString(), new FacetResult(entry.getKey(), entry.getValue()));
        }
        
        return keysByTable;
    }
    
    /** evaluate for the single event test */
    protected void evaluateSingleEventResults(final Multimap<String,FacetResult> keysByTable, Set<String> expectedFacets, Set<String> expectedFacetMetadata) {
        evaluateResults(keysByTable, expectedFacets, expectedFacetMetadata, null, 1, null);
    }
    
    /** evaluate for the multiple event test */
    protected void evaluateMultipleEventResults(final Multimap<String,FacetResult> keysByTable, int metadataItemCount,
                    Object2IntMap<String> expectedFacetKeyCounts) {
        evaluateResults(keysByTable, null, null, null, metadataItemCount, expectedFacetKeyCounts);
    }
    
    /** evaluate for the multi value hashing test */
    protected void evaluateMultiValueResults(final Multimap<String,FacetResult> keysByTable, Set<String> expectedFacetHashes,
                    Object2IntMap<String> expectedFacetKeyCounts) {
        evaluateResults(keysByTable, null, null, expectedFacetHashes, 0, expectedFacetKeyCounts);
    }
    
    protected void evaluateResults(final Multimap<String,FacetResult> keysByTable, Set<String> expectedFacets, Set<String> expectedFacetMetadata,
                    Set<String> expectedFacetHashes, int metadataItemCount, Object2IntMap<String> expectedFacetKeyCounts) {
        if (expectedFacetMetadata == null)
            expectedFacetMetadata = new TreeSet<>();
        boolean evaluateExpectedFacetMetadata = !expectedFacetMetadata.isEmpty();
        Set<String> unexpectedFacetMetadata = new TreeSet<>();
        
        if (expectedFacets == null)
            expectedFacets = new TreeSet<>();
        boolean evaluateExpectedFacets = !expectedFacets.isEmpty();
        Set<String> unexpectedFacets = new TreeSet<>();
        
        if (expectedFacetHashes == null)
            expectedFacetHashes = new TreeSet<>();
        boolean evaluateExpectedFacetHashes = !expectedFacetHashes.isEmpty();
        Set<String> unexpectedFacetHashes = new TreeSet<>();
        
        if (expectedFacetKeyCounts == null)
            expectedFacetKeyCounts = new Object2IntOpenHashMap<>();
        boolean evaluateExpectedFacetKeyCounts = !expectedFacetKeyCounts.isEmpty();
        
        Object2IntMap<String> facetKeyCounts = new Object2IntOpenHashMap<>();
        Object2IntMap<String> facetMetadataKeyCounts = new Object2IntOpenHashMap<>();
        
        Set<String> totallyUnexpected = new TreeSet<>();
        
        for (String tableName : keysByTable.keySet()) {
            // sort the results
            List<FacetResult> results = new ArrayList<>(keysByTable.get(tableName));
            Collections.sort(results);
            for (FacetResult pair : results) {
                switch (tableName) {
                    case DATAWAVE_FACETS:
                        String facet = pair.getFirst().getKey().toString() + " " + FacetHandler.extractCardinality(pair.getSecond());
                        log.debug(tableName + " " + facet);
                        if (!expectedFacets.remove(facet)) {
                            unexpectedFacets.add(facet);
                        }
                        facetKeyCounts.compute(facet, (k, v) -> 1 + (v == null ? 0 : v));
                        break;
                    case DATAWAVE_FACET_METADATA:
                        String facetMetadata = pair.getFirst().getKey().toString();
                        log.debug(tableName + " " + facetMetadata);
                        if (!expectedFacetMetadata.remove(facetMetadata)) {
                            unexpectedFacetMetadata.add(facetMetadata);
                        }
                        facetMetadataKeyCounts.compute(facetMetadata, (k, v) -> 1 + (v == null ? 0 : v));
                        break;
                    case DATAWAVE_FACET_HASHES:
                        String facetHash = pair.getFirst().getKey().toString();
                        log.debug(tableName + " " + facetHash);
                        if (!expectedFacetHashes.remove(facetHash)) {
                            unexpectedFacetHashes.add(facetHash);
                        }
                        break;
                    default:
                        String item = tableName + " " + pair.getFirst().getKey().toString();
                        log.warn("Unexpected table name/key: " + item);
                        totallyUnexpected.add(item);
                        break;
                }
            }
        }
        
        if (evaluateExpectedFacetMetadata) {
            assertTrue("Did not observe expected facet metadata: " + expectedFacetMetadata, expectedFacetMetadata.isEmpty());
            assertTrue("Observed unexpected facet metadata: " + unexpectedFacetMetadata, unexpectedFacetMetadata.isEmpty());
        }
        
        if (evaluateExpectedFacets) {
            assertTrue("Did not observe expected facets: " + expectedFacets, expectedFacets.isEmpty());
            assertTrue("Observed unexpected facets: " + unexpectedFacets, unexpectedFacets.isEmpty());
        }
        
        if (evaluateExpectedFacetHashes) {
            assertTrue("Did not observe expected facet hashes: " + expectedFacetHashes, expectedFacetHashes.isEmpty());
            assertTrue("Observed unexpected facet hashes: " + unexpectedFacetHashes, unexpectedFacetHashes.isEmpty());
        }
        
        if (!totallyUnexpected.isEmpty()) {
            fail("Observed unexpected keys for unexpected table: " + totallyUnexpected);
        }
        
        // dump and/or metadata item count
        facetMetadataKeyCounts.forEach((k, v) -> {
            log.debug(DATAWAVE_FACET_METADATA + " " + k + " " + v);
            if (metadataItemCount > 0) {
                assertEquals("Metadata count equals itemCount " + metadataItemCount, metadataItemCount, v.intValue());
            }
        });
        
        // dump and/or evaluateExpectedFacetKeyCounts
        List<String> sortedKeys = new ArrayList<>(facetKeyCounts.keySet());
        Collections.sort(sortedKeys);
        
        final Object2IntMap<String> finalExpectedFacetKeyCounts = expectedFacetKeyCounts;
        sortedKeys.forEach((k) -> {
            int found = facetKeyCounts.getOrDefault(k, 0);
            log.debug(DATAWAVE_FACETS + " " + k + " " + found);
            
            if (evaluateExpectedFacetKeyCounts) {
                int expected = finalExpectedFacetKeyCounts.getOrDefault(k, 0);
                if (found == 0 || expected == 0) {
                    fail("Neither found or expected key count should be zero for: " + k + "; expected: " + expected + ", found: " + found);
                }
                assertEquals("Facet key count mismatch: expected for: " + k + "; expected: " + expected + ", found: " + found, expected, found);
                finalExpectedFacetKeyCounts.remove(k, expected);
            }
        });
        
        assertEquals("Did not observe expected facets: " + finalExpectedFacetKeyCounts, 0, finalExpectedFacetKeyCounts.size());
        
    }
    
    private static class CachingContextWriter extends AbstractContextWriter<BulkIngestKey,Value> {
        private final Multimap<BulkIngestKey,Value> cache = LinkedListMultimap.create();
        
        @Override
        protected void flush(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,BulkIngestKey,Value> context) {
            for (Map.Entry<BulkIngestKey,Value> entry : entries.entries()) {
                cache.put(entry.getKey(), entry.getValue());
            }
        }
        
        public Multimap<BulkIngestKey,Value> getCache() {
            return cache;
        }
    }
    
    private static final class TestData {
        // @formatter:off
        private static final String[] schema = {"ID", "MAKE", "MODEL", "VARIANT", "STYLE", "COLOR", "YEAR", "MILEAGE", "PRICE"};
        
        private static final String[][] data = {
                {"8343", "nissan", "skyline", "gtr r43 v=spec", "sports", "blue", "1999", "40921", "100000"},
                {"8342", "nissan", "skyline", "gtr r33 v-spec", "sports", "silver", "1995", "50740", "79000"},
                {"8340", "nissan", "skyline", "gtr r33 v-spec", "sports", "black",},
                {"8338", "nissan", "skyline", "gtr r33", "sports", "black", "1995", "91159", "48000"},
                {"8341", "toyota", "supra", "", "sports", "red", "1993", "132419", "43750"},
                {"8339", "toyota", "mr2", "", "sports", "white", "1989", "128442", "13500"},
                {"8337", "subaru", "forester", "sti", "wagon", "white", "2000", "118060", "7750"},
                {"8336", "mitsubishi", "lancer", "evo", "sports", "red", "2000", "96654", "55000"},
                {"8335", "nissan", "skyline", "gtr v", "sports", "black", "1994", "66030", "37500"},
                {"8334", "nissan", "skyline", "gtr r32", "sports", "silver", "1995", "149242", "46300"},
                {"8333", "toyota", "supra", "", "sports", "white", "1996", "106782", "27600"},
                {"8332", "nissan", "skyline", "gtr tr32", "sports", "black", "1992", "154982", "37500"},
                {"8331", "lancia", "delta", "hf integrale", "hatchback", "yellow", "1993", "105330", "85000"},
                {"8330", "lancia", "delta", "hf integrale", "hatchback", "red", "1993", "58852", "85000"},
                {"8329", "honda", "civic", "sir eg6", "sports", "white", "0000", "156109", "18800"},
                {"8328", "toyota", "supra", "", "sports", "white", "1993", "148842", "18500"},
                {"8323", "mazda", "rx7", "type r bathurst", "sports", "blue", "2001", "60024", "43500"},
                {"8038", "subaru", "impreza", "wrx sti", "sports", "blue", "1994", "131267", "11000"},
                {"8323", "mazda", "az-1", "speed", "sports", "blue", "1992", "69200", "18000"},};
        
        private static final String[][] expectedFacets = {
                {},
                {
                    "silver%00;silver%00;test COLOR%00;COLOR:20200401 [] 1585699200000 false 1",
                    "nissan%00;silver%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1",
                    "silver%00;skyline%00;test COLOR%00;MODEL:20200401 [] 1585699200000 false 1",
                    "silver%00;sports%00;test COLOR%00;STYLE:20200401 [] 1585699200000 false 1",
                    "sports%00;skyline%00;test STYLE%00;MODEL:20200401 [] 1585699200000 false 1",
                    "nissan%00;skyline%00;test MAKE%00;MODEL:20200401 [] 1585699200000 false 1",
                    "sports%00;sports%00;test STYLE%00;STYLE:20200401 [] 1585699200000 false 1",
                    "nissan%00;sports%00;test MAKE%00;STYLE:20200401 [] 1585699200000 false 1",
                    "nissan%00;nissan%00;test MAKE%00;MAKE:20200401 [] 1585699200000 false 1",
                    "sports%00;silver%00;test STYLE%00;COLOR:20200401 [] 1585699200000 false 1"
                }
        };
        
        private static final String[][] expectedFacetMetadata = {
                {},
                {
                        "MAKE%00;MAKE pv: [] 1585699200000 false",
                        "MAKE%00;STYLE pv: [] 1585699200000 false",
                        "MAKE%00;MODEL pv: [] 1585699200000 false",
                        "MAKE%00;COLOR pv: [] 1585699200000 false",
                        "STYLE%00;STYLE pv: [] 1585699200000 false",
                        "STYLE%00;MODEL pv: [] 1585699200000 false",
                        "STYLE%00;COLOR pv: [] 1585699200000 false",
                        "COLOR%00;COLOR pv: [] 1585699200000 false",
                        "COLOR%00;MODEL pv: [] 1585699200000 false",
                        "COLOR%00;STYLE pv: [] 1585699200000 false",
                }
        };
        
        
        private static final String[][] expectedUniqueFacets = {
                {"black%00;black%00;test COLOR%00;COLOR:20200401 [] 1585699200000 false 1","4"},
                {"black%00;skyline%00;test COLOR%00;MODEL:20200401 [] 1585699200000 false 1","4"},
                {"black%00;sports%00;test COLOR%00;STYLE:20200401 [] 1585699200000 false 1","4"},
                {"blue%00;az-1%00;test COLOR%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"blue%00;blue%00;test COLOR%00;COLOR:20200401 [] 1585699200000 false 1","4"},
                {"blue%00;impreza%00;test COLOR%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"blue%00;rx7%00;test COLOR%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"blue%00;skyline%00;test COLOR%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"blue%00;sports%00;test COLOR%00;STYLE:20200401 [] 1585699200000 false 1","4"},
                {"hatchback%00;delta%00;test STYLE%00;MODEL:20200401 [] 1585699200000 false 1","2"},
                {"hatchback%00;hatchback%00;test STYLE%00;STYLE:20200401 [] 1585699200000 false 1","2"},
                {"hatchback%00;red%00;test STYLE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
                {"hatchback%00;yellow%00;test STYLE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
                {"honda%00;civic%00;test MAKE%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"honda%00;honda%00;test MAKE%00;MAKE:20200401 [] 1585699200000 false 1","1"},
                {"honda%00;sports%00;test MAKE%00;STYLE:20200401 [] 1585699200000 false 1","1"},
                {"honda%00;white%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
                {"lancia%00;delta%00;test MAKE%00;MODEL:20200401 [] 1585699200000 false 1","2"},
                {"lancia%00;hatchback%00;test MAKE%00;STYLE:20200401 [] 1585699200000 false 1","2"},
                {"lancia%00;lancia%00;test MAKE%00;MAKE:20200401 [] 1585699200000 false 1","2"},
                {"lancia%00;red%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
                {"lancia%00;yellow%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
                {"mazda%00;az-1%00;test MAKE%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"mazda%00;blue%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","2"},
                {"mazda%00;mazda%00;test MAKE%00;MAKE:20200401 [] 1585699200000 false 1","2"},
                {"mazda%00;rx7%00;test MAKE%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"mazda%00;sports%00;test MAKE%00;STYLE:20200401 [] 1585699200000 false 1","2"},
                {"mitsubishi%00;lancer%00;test MAKE%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"mitsubishi%00;mitsubishi%00;test MAKE%00;MAKE:20200401 [] 1585699200000 false 1","1"},
                {"mitsubishi%00;red%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
                {"mitsubishi%00;sports%00;test MAKE%00;STYLE:20200401 [] 1585699200000 false 1","1"},
                {"nissan%00;black%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","4"},
                {"nissan%00;blue%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
                {"nissan%00;nissan%00;test MAKE%00;MAKE:20200401 [] 1585699200000 false 1","7"},
                {"nissan%00;silver%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","2"},
                {"nissan%00;skyline%00;test MAKE%00;MODEL:20200401 [] 1585699200000 false 1","7"},
                {"nissan%00;sports%00;test MAKE%00;STYLE:20200401 [] 1585699200000 false 1","7"},
                {"red%00;delta%00;test COLOR%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"red%00;hatchback%00;test COLOR%00;STYLE:20200401 [] 1585699200000 false 1","1"},
                {"red%00;lancer%00;test COLOR%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"red%00;red%00;test COLOR%00;COLOR:20200401 [] 1585699200000 false 1","3"},
                {"red%00;sports%00;test COLOR%00;STYLE:20200401 [] 1585699200000 false 1","2"},
                {"red%00;supra%00;test COLOR%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"silver%00;silver%00;test COLOR%00;COLOR:20200401 [] 1585699200000 false 1","2"},
                {"silver%00;skyline%00;test COLOR%00;MODEL:20200401 [] 1585699200000 false 1","2"},
                {"silver%00;sports%00;test COLOR%00;STYLE:20200401 [] 1585699200000 false 1","2"},
                {"sports%00;az-1%00;test STYLE%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"sports%00;black%00;test STYLE%00;COLOR:20200401 [] 1585699200000 false 1","4"},
                {"sports%00;blue%00;test STYLE%00;COLOR:20200401 [] 1585699200000 false 1","4"},
                {"sports%00;civic%00;test STYLE%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"sports%00;impreza%00;test STYLE%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"sports%00;lancer%00;test STYLE%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"sports%00;mr2%00;test STYLE%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"sports%00;red%00;test STYLE%00;COLOR:20200401 [] 1585699200000 false 1","2"},
                {"sports%00;rx7%00;test STYLE%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"sports%00;silver%00;test STYLE%00;COLOR:20200401 [] 1585699200000 false 1","2"},
                {"sports%00;skyline%00;test STYLE%00;MODEL:20200401 [] 1585699200000 false 1","7"},
                {"sports%00;sports%00;test STYLE%00;STYLE:20200401 [] 1585699200000 false 1","16"},
                {"sports%00;supra%00;test STYLE%00;MODEL:20200401 [] 1585699200000 false 1","3"},
                {"sports%00;white%00;test STYLE%00;COLOR:20200401 [] 1585699200000 false 1","4"},
                {"subaru%00;blue%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
                {"subaru%00;forester%00;test MAKE%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"subaru%00;impreza%00;test MAKE%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"subaru%00;sports%00;test MAKE%00;STYLE:20200401 [] 1585699200000 false 1","1"},
                {"subaru%00;subaru%00;test MAKE%00;MAKE:20200401 [] 1585699200000 false 1","2"},
                {"subaru%00;wagon%00;test MAKE%00;STYLE:20200401 [] 1585699200000 false 1","1"},
                {"subaru%00;white%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
                {"toyota%00;mr2%00;test MAKE%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"toyota%00;red%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
                {"toyota%00;sports%00;test MAKE%00;STYLE:20200401 [] 1585699200000 false 1","4"},
                {"toyota%00;supra%00;test MAKE%00;MODEL:20200401 [] 1585699200000 false 1","3"},
                {"toyota%00;toyota%00;test MAKE%00;MAKE:20200401 [] 1585699200000 false 1","4"},
                {"toyota%00;white%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","3"},
                {"wagon%00;forester%00;test STYLE%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"wagon%00;wagon%00;test STYLE%00;STYLE:20200401 [] 1585699200000 false 1","1"},
                {"wagon%00;white%00;test STYLE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
                {"white%00;civic%00;test COLOR%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"white%00;forester%00;test COLOR%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"white%00;mr2%00;test COLOR%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"white%00;sports%00;test COLOR%00;STYLE:20200401 [] 1585699200000 false 1","4"},
                {"white%00;supra%00;test COLOR%00;MODEL:20200401 [] 1585699200000 false 1","2"},
                {"white%00;wagon%00;test COLOR%00;STYLE:20200401 [] 1585699200000 false 1","1"},
                {"white%00;white%00;test COLOR%00;COLOR:20200401 [] 1585699200000 false 1","5"},
                {"yellow%00;delta%00;test COLOR%00;MODEL:20200401 [] 1585699200000 false 1","1"},
                {"yellow%00;hatchback%00;test COLOR%00;STYLE:20200401 [] 1585699200000 false 1","1"},
                {"yellow%00;yellow%00;test COLOR%00;COLOR:20200401 [] 1585699200000 false 1","1"},
        };

        public static String[][] expectedHashedFacets = {
            {"blue%00;blue%00;test COLOR%00;COLOR:20200401 [] 1585699200000 false 1","2"},
            {"honda%00;honda%00;test MAKE%00;MAKE:20200401 [] 1585699200000 false 1","1"},
            {"honda%00;white%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
            {"lancia%00;lancia%00;test MAKE%00;MAKE:20200401 [] 1585699200000 false 1","1"},
            {"lancia%00;red%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
            {"lancia%00;yellow%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
            {"mazda%00;blue%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
            {"mazda%00;mazda%00;test MAKE%00;MAKE:20200401 [] 1585699200000 false 1","1"},
            {"mitsubishi%00;mitsubishi%00;test MAKE%00;MAKE:20200401 [] 1585699200000 false 1","1"},
            {"mitsubishi%00;red%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
            {"nissan%00;b65828a2da1e34d09c9ebe3506a017c9a8b6605a%00;test MAKE%00;COLOR.hash:20200401 [] 1585699200000 false 1","1"},
            {"nissan%00;nissan%00;test MAKE%00;MAKE:20200401 [] 1585699200000 false 1","1"},
            {"red%00;red%00;test COLOR%00;COLOR:20200401 [] 1585699200000 false 1","3"},
            {"subaru%00;blue%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
            {"subaru%00;subaru%00;test MAKE%00;MAKE:20200401 [] 1585699200000 false 1","1"},
            {"subaru%00;white%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
            {"toyota%00;red%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
            {"toyota%00;toyota%00;test MAKE%00;MAKE:20200401 [] 1585699200000 false 1","1"},
            {"toyota%00;white%00;test MAKE%00;COLOR:20200401 [] 1585699200000 false 1","1"},
            {"white%00;white%00;test COLOR%00;COLOR:20200401 [] 1585699200000 false 1","3"},
            {"yellow%00;yellow%00;test COLOR%00;COLOR:20200401 [] 1585699200000 false 1","1"}
        };

        public static String[] expectedFacetHashes = {
            "b65828a2da1e34d09c9e black: [] 1585699200000 false",
            "b65828a2da1e34d09c9e blue: [] 1585699200000 false",
            "b65828a2da1e34d09c9e silver: [] 1585699200000 false"
        };
        // @formatter:on
        
        static String[] getExpectedFacetData(int index) {
            return expectedFacets[index];
        }
        
        static String[] getExpectedFacetMetadataData(int index) {
            return expectedFacetMetadata[index];
        }
        
        static Object2IntMap<String> getExpectedHashedFacetCounts() {
            Object2IntMap<String> countMap = new Object2IntOpenHashMap<>();
            Stream.of(expectedHashedFacets).forEach(k -> countMap.put(k[0], Integer.parseInt(k[1])));
            return countMap;
        }
        
        static Set<String> getExpectedFacetHashes() {
            return Stream.of(expectedFacetHashes).collect(Collectors.toSet());
        }
        
        static Object2IntMap<String> getExpectedFacetCounts() {
            Object2IntMap<String> countMap = new Object2IntOpenHashMap<>();
            Stream.of(expectedUniqueFacets).forEach(k -> countMap.put(k[0], Integer.parseInt(k[1])));
            return countMap;
        }
        
        static Multimap<String,NormalizedContentInterface> getDataItem(int index) {
            return toEventFields(data[index]);
        }
        
        static Collection<Multimap<String,NormalizedContentInterface>> getDataItems() {
            return Stream.of(data).map(TestData::toEventFields).collect(Collectors.toSet());
        }
        
        /**
         * Transform a collection of fields into an event
         *
         * @param data
         *            An array of fields, less than or equal in length to the schema.
         * @return Multimap<String,NormalizedContentInterface>
         */
        private static Multimap<String,NormalizedContentInterface> toEventFields(String[] data) {
            if (schema.length < data.length) {
                throw new RuntimeException("Data length mismatch, expected at most " + schema.length + " fields, but got " + data.length);
            }
            
            Multimap<String,NormalizedContentInterface> fields = HashMultimap.create();
            for (int i = 0; i < data.length; i++) {
                fields.put(schema[i], new NormalizedFieldAndValue(schema[i], data[i]));
            }
            return fields;
        }
        
        /** generate one event per make where each event has the full list of colors for that make. */
        public static Collection<Multimap<String,NormalizedContentInterface>> generateMultivaluedColorData() {
            // index 1 == MAKE, index 5 == COLOR
            Multimap<String,String> makeColors = HashMultimap.create();
            Stream.of(data).forEach(item -> makeColors.put(item[1], item[5]));
            return makeColors.keySet().stream().map(make -> {
                Multimap<String,NormalizedContentInterface> singleMakeColor = HashMultimap.create();
                singleMakeColor.put(schema[1], new NormalizedFieldAndValue(schema[1], make));
                makeColors.get(make).forEach(color -> singleMakeColor.put(schema[5], new NormalizedFieldAndValue(schema[5], color)));
                return singleMakeColor;
            }).collect(Collectors.toSet());
        }
    }
    
    private static class FacetResult extends Pair<BulkIngestKey,Value> implements Comparable<Pair<BulkIngestKey,Value>> {
        public FacetResult(BulkIngestKey f, Value s) {
            super(f, s);
        }
        
        @Override
        public int compareTo(Pair<BulkIngestKey,Value> o) {
            return getFirst().compareTo(o.getFirst());
        }
    }
}
