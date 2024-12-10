package datawave.query.jexl.visitors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.lookups.IndexLookup;
import datawave.query.jexl.lookups.cache.LookupFailureCache;
import datawave.query.jexl.lookups.cache.RegexLookupCache;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.util.TableName;
import datawave.util.time.DateHelper;

public class RegexIndexExpansionVisitorTest {

    private static AccumuloClient client;
    private ShardQueryConfiguration config;
    private ScannerFactory scannerFactory;
    private MetadataHelper metadataHelper;

    private static final String SHARD_INDEX = TableName.SHARD_INDEX;
    private static final Value VALUE = new Value();

    private String startDate;
    private String endDate;
    private LookupFailureCache lookupCache;

    @BeforeClass
    public static void setup() throws Exception {
        client = new InMemoryAccumuloClient("", new InMemoryInstance());

        writeData(client);
    }

    @Before
    public void beforeEach() {
        config = new ShardQueryConfiguration();
        config.setClient(client);
        config.setLimitTermExpansionToModel(false);

        // default date range
        startDate = "20241105";
        endDate = "20241107";
        config.setBeginDate(DateHelper.parse(startDate));
        config.setEndDate(DateHelper.parse(endDate));

        scannerFactory = new ScannerFactory(client);

        metadataHelper = new MockMetadataHelper();
        ((MockMetadataHelper) metadataHelper).setIndexedFields(Set.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D"));
        Multimap<String,Type<?>> queryFieldDatatype = HashMultimap.create();
        queryFieldDatatype.put("FIELD_A", new LcNoDiacriticsType());
        queryFieldDatatype.put("FIELD_B", new LcNoDiacriticsType());
        queryFieldDatatype.put("FIELD_C", new LcNoDiacriticsType());
        queryFieldDatatype.put("FIELD_D", new LcNoDiacriticsType());
        queryFieldDatatype.put("FIELD_E", new LcNoDiacriticsType());
        ((MockMetadataHelper) metadataHelper).setDataTypes(queryFieldDatatype);
        // TODO -- non event fields
        // TODO -- not indexed fields

        config.setQueryFieldsDatatypes(queryFieldDatatype);

        lookupCache = null;
    }

    private static void writeData(AccumuloClient client) throws Exception {

        client.tableOperations().create(SHARD_INDEX);

        try (BatchWriter bw = client.createBatchWriter(SHARD_INDEX)) {

            // row for testing fields
            Mutation m = new Mutation("bar");
            m.put("FIELD_A", "20241105_0\0datatype-a", VALUE);
            m.put("FIELD_B", "20241105_0\0datatype-a", VALUE);
            bw.addMutation(m);

            m = new Mutation("baz");
            m.put("FIELD_A", "20241105_0\0datatype-a", VALUE);
            m.put("FIELD_B", "20241105_0\0datatype-a", VALUE);
            m.put("FIELD_C", "20241105_0\0datatype-a", VALUE);
            bw.addMutation(m);

            m = new Mutation("buzz");
            m.put("FIELD_A", "20241105_0\0datatype-a", VALUE);
            m.put("FIELD_C", "20241105_0\0datatype-a", VALUE);
            m.put("FIELD_D", "20241105_0\0datatype-a", VALUE);
            bw.addMutation(m);

            // row for testing date ranges
            m = new Mutation("date");
            m.put("FIELD_A", "20241105_0\0datatype-a", VALUE);
            m.put("FIELD_B", "20241106_0\0datatype-a", VALUE);
            m.put("FIELD_C", "20241107_0\0datatype-a", VALUE);
            m.put("FIELD_D", "20241108_0\0datatype-a", VALUE);
            bw.addMutation(m);

            // row for testing datatype filtering
            m = new Mutation("type");
            m.put("FIELD_A", "20241105_0\0datatype-a", VALUE);
            m.put("FIELD_A", "20241106_0\0datatype-b", VALUE);
            m.put("FIELD_A", "20241107_0\0datatype-c", VALUE);
            bw.addMutation(m);
        }
    }

    @Test
    public void testSmallExpansionRestrictedByValue() {
        String query = "FIELD_A =~ 'ba.*'";
        String expected = "FIELD_A == 'bar' || FIELD_A == 'baz'";
        drive(query, expected);
    }

    @Test
    public void testLargeExpansionRestrictedByValue() {
        String query = "FIELD_A =~ 'b.*'";
        String expected = "FIELD_A == 'bar' || FIELD_A == 'baz' || FIELD_A == 'buzz'";
        drive(query, expected);
    }

    @Test
    public void testExpansionRestrictedByField() {
        String query = "FIELD_B =~ 'b.*'";
        String expected = "FIELD_B == 'bar' || FIELD_B == 'baz'";
        drive(query, expected);

        query = "FIELD_C =~ 'b.*'";
        expected = "FIELD_C == 'baz' || FIELD_C == 'buzz'";
        drive(query, expected);

        query = "FIELD_D =~ 'b.*'";
        expected = "FIELD_D == 'buzz'";
        drive(query, expected);
    }

    @Test
    public void testExpansionRestrictedByDate() {
        String query = "FIELD_A =~ 'da.*'";
        String expected = "FIELD_A == 'date'";
        drive(query, expected);

        query = "FIELD_A =~ 'da.*'";
        expected = "FIELD_A == 'date'";
        withDateRange("20241105", "20241106");
        drive(query, expected);

        query = "FIELD_A =~ 'da.*'"; // no change when data not found in date range
        withDateRange("20241106", "20241107");
        drive(query, query);
    }

    @Test
    public void testExpansionRestrictedByDatatype() {
        String query = "FIELD_A =~ 'ty.*'";
        String expected = "FIELD_A == 'type'";
        config.setDatatypeFilter(Set.of("datatype-a"));
        drive(query, expected);

        config.setDatatypeFilter(Set.of("datatype-z"));
        drive(query, query);
    }

    @Test
    public void testFieldedValueDoesNotExist() {
        String query = "FIELD_A =~ 'xyz.*'";
        String expected = "FIELD_A =~ 'xyz.*'";
        drive(query, expected);
    }

    @Test
    public void testAnyfieldValueDoesNotExist() {
        String query = "_ANYFIELD_ =~ 'xyz.*'";
        String expected = "_ANYFIELD_ =~ 'xyz.*'";
        drive(query, expected);
    }

    @Test
    public void testExpansionSuccessWithCache() {
        String query = "FIELD_A =~ 'ty.*'";
        String expected = "FIELD_A == 'type'";

        withLookupCache();
        for (int i = 0; i < 5; i++) {
            drive(query, expected);
        }
    }

    @Test
    public void testExpansionFailureWithCache() {
        String query = "FIELD_A =~ 'b.*'";
        String expected = "((_Value_ = true) && (FIELD_A =~ 'b.*'))";

        withLookupCache();

        int maxValueExpansionThreshold = config.getMaxValueExpansionThreshold();
        try {
            // lower threshold to trigger expansion failures
            config.setMaxValueExpansionThreshold(1);
            for (int i = 0; i < 5; i++) {
                drive(query, expected);
            }
        } finally {
            config.setMaxValueExpansionThreshold(maxValueExpansionThreshold);
        }
    }

    private void drive(String query, String expected) {
        ASTJexlScript script = parse(query);
        ASTJexlScript expanded = expand(script);
        String result = JexlStringBuildingVisitor.buildQuery(expanded);
        assertEquals(expected, result);
    }

    private ASTJexlScript expand(ASTJexlScript script) {
        try {
            Map<String,IndexLookup> lookupMap = new HashMap<>();
            return RegexIndexExpansionVisitor.expandRegex(config, scannerFactory, metadataHelper, lookupMap, script, lookupCache);
        } catch (Exception e) {
            fail("Failed to execute test: " + e.getMessage());
        }
        return null;
    }

    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            Assert.fail("Failed to parse query: " + query);
            throw new RuntimeException(e);
        }
    }

    private void withDateRange(String start, String end) {
        startDate = start;
        endDate = end;
        config.setBeginDate(DateHelper.parse(startDate));
        config.setEndDate(DateHelper.parse(endDate));
    }

    private void withLookupCache() {
        lookupCache = new RegexLookupCache(5, 1, 1);
    }
}
