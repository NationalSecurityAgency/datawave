package datawave.ingest.data.config;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.sun.net.httpserver.HttpServer;

import datawave.TestBaseIngestHelper;
import datawave.data.type.DateType;
import datawave.data.type.HexStringType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.VirtualIngest;
import datawave.ingest.mapreduce.SimpleDataTypeHandler;
import datawave.policy.IngestPolicyEnforcer;

public class XMLFieldConfigHelperTest {

    private final BaseIngestHelper ingestHelper = new TestBaseIngestHelper();
    private Configuration conf = new Configuration();

    @Before
    public void setUp() {

        conf.set(DataTypeHelper.Properties.DATA_NAME, "test");
        conf.set("test" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        conf.set("test" + BaseIngestHelper.DEFAULT_TYPE, NoOpType.class.getName());

        datawave.ingest.data.Type type = new datawave.ingest.data.Type("test", null, null, new String[] {SimpleDataTypeHandler.class.getName()}, 10, null);
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf).put("test", type);

        ingestHelper.setup(conf);
    }

    @Test
    public void shouldReadConfigOverHttp() throws Exception {
        int port = 28080;
        String requestUrl = "http://localhost:" + port + "/";
        HttpServer server = createFileServer("config/sample-field-config.xml", port);
        server.start();

        try {
            FieldConfigHelper helper = XMLFieldConfigHelper.load(requestUrl, ingestHelper);

            assertThat(helper.isIndexedField("A"), is(true));
            assertThat(helper.isIndexedField("B"), is(false));

        } finally {
            server.stop(0);

        }
    }

    private HttpServer createFileServer(String path, int port) throws Exception {
        final String resp = readFile(path);
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

        server.setExecutor(null);
        server.createContext("/", e -> {
            e.sendResponseHeaders(200, 0);
            e.getResponseHeaders().set("Content-Type", "text/xml");

            OutputStream responseBody = e.getResponseBody();
            responseBody.write(resp.getBytes());
            responseBody.close();
        });

        return server;
    }

    private String readFile(String path) {
        StringBuilder sb = new StringBuilder();
        InputStream istream = getClass().getClassLoader().getResourceAsStream(path);
        try (Scanner scanner = new Scanner(istream)) {
            while (scanner.hasNext()) {
                sb.append(scanner.nextLine() + "\n");
            }
        }

        return sb.toString();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBadTag() throws Exception {
        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <default stored=\"true\" indexed=\"false\" reverseIndexed=\"false\" tokenized=\"true\" reverseTokenized=\"true\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                        + "    <nomatch stored=\"true\" indexed=\"true\" reverseIndexed=\"true\" tokenized=\"true\"  reverseTokenized=\"true\" indexType=\"datawave.data.type.HexStringType\"/>\n"
                        + "    <fieldPattern pattern=\"*J\" indexed=\"true\" indexType=\"datawave.data.type.MacAddressType\"/>\n"
                        + "    <field name=\"H\" indexType=\"datawave.data.type.DateType\"/>\n"
                        + "    <orange name=\"H\" indexType=\"datawave.data.type.DateType\"/>\n" + "</fieldConfig>";

        XMLFieldConfigHelper helper = new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDuplicateField() throws Exception {
        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <default stored=\"true\" indexed=\"false\" reverseIndexed=\"false\" tokenized=\"true\" reverseTokenized=\"true\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                        + "    <nomatch stored=\"true\" indexed=\"true\" reverseIndexed=\"true\" tokenized=\"true\"  reverseTokenized=\"true\" indexType=\"datawave.data.type.HexStringType\"/>\n"
                        + "    <fieldPattern pattern=\"*J\" indexed=\"true\" indexType=\"datawave.data.type.MacAddressType\"/>\n"
                        + "    <field name=\"H\" indexType=\"datawave.data.type.DateType\"/>\n"
                        + "    <field name=\"H\" indexType=\"datawave.data.type.HexStringType\"/>\n" + "</fieldConfig>";

        FieldConfigHelper helper = new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper);
    }

    @Test(expected = IllegalStateException.class)
    public void testMissingDefault() throws Exception {
        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <nomatch stored=\"true\" indexed=\"true\" reverseIndexed=\"true\" tokenized=\"true\"  reverseTokenized=\"true\" indexType=\"datawave.data.type.HexStringType\"/>\n"
                        + "    <field name=\"A\" indexed=\"true\"/>\n" + "</fieldConfig>";

        FieldConfigHelper helper = new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIncompleteDefault() throws Exception {
        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <default stored=\"true\" reverseIndexed=\"false\" tokenized=\"true\" reverseTokenized=\"true\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                        + "    <nomatch stored=\"true\" indexed=\"true\" reverseIndexed=\"true\" tokenized=\"true\"  reverseTokenized=\"true\" indexType=\"datawave.data.type.HexStringType\"/>\n"
                        + "    <fieldPattern pattern=\"*J\" indexed=\"true\" indexType=\"datawave.data.type.MacAddressType\"/>\n"
                        + "    <field name=\"A\" indexed=\"true\"/>\n" +

                        "</fieldConfig>";

        FieldConfigHelper helper = new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper);
    }

    @Test
    public void testMissingNomatch() throws Exception {
        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <default stored=\"true\" indexed=\"false\" reverseIndexed=\"false\" tokenized=\"true\" reverseTokenized=\"true\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                        + "    <fieldPattern pattern=\"*J\" indexed=\"true\" indexType=\"datawave.data.type.MacAddressType\"/>\n"
                        + "    <field name=\"H\" indexType=\"datawave.data.type.DateType\"/>\n" + "</fieldConfig>";

        FieldConfigHelper helper = new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper);
        // ok.
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIncompleteNomatch() throws Exception {
        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <default stored=\"true\" indexed=\"false\" reverseIndexed=\"false\" tokenized=\"true\" reverseTokenized=\"true\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                        + "    <nomatch stored=\"true\" reverseIndexed=\"true\" tokenized=\"true\"  reverseTokenized=\"true\" indexType=\"datawave.data.type.HexStringType\"/>\n"
                        + "    <fieldPattern pattern=\"*J\" indexed=\"true\" indexType=\"datawave.data.type.MacAddressType\"/>\n"
                        + "    <field name=\"H\" indexType=\"datawave.data.type.DateType\"/>\n" + "</fieldConfig>";

        FieldConfigHelper helper = new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper);
    }

    @Test
    public void testMultiType() throws Exception {
        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <default stored=\"true\" indexed=\"false\" reverseIndexed=\"false\" tokenized=\"true\" reverseTokenized=\"true\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                        + "    <fieldPattern pattern=\"*J\" indexed=\"true\" indexType=\"datawave.data.type.MacAddressType\"/>\n"
                        + "    <field name=\"H\" indexType=\"datawave.data.type.DateType,datawave.data.type.HexStringType\"/>\n" + "</fieldConfig>";

        FieldConfigHelper helper = new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper);

        List<Type<?>> types = ingestHelper.getDataTypes("H");
        assertEquals(2, types.size());
    }

    @Test
    public void testOverlappingRegex() throws Exception {
        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <default stored=\"true\" indexed=\"false\" reverseIndexed=\"false\" tokenized=\"true\" reverseTokenized=\"true\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                        + "    <fieldPattern pattern=\"B*\" indexed=\"true\" indexType=\"datawave.data.type.MacAddressType\"/>\n"
                        + "    <fieldPattern pattern=\"BA*\"  indexType=\"datawave.data.type.HexStringType\"/>\n"
                        + "    <fieldPattern pattern=\"BANAN*\"  indexType=\"datawave.data.type.DateType\"/>\n" + "</fieldConfig>";

        FieldConfigHelper helper = new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper);

        List<Type<?>> types = ingestHelper.getDataTypes("BANANA");
        assertEquals(3, types.size());
    }

    @Test
    public void testOverlappingRegexPrecise() throws Exception {
        conf.setBoolean(BaseIngestHelper.USE_MOST_PRECISE_FIELD_TYPE_REGEX, true);
        ingestHelper.setup(conf);

        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <default stored=\"true\" indexed=\"false\" reverseIndexed=\"false\" tokenized=\"true\" reverseTokenized=\"true\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                        + "    <fieldPattern pattern=\"B*\" indexed=\"true\" indexType=\"datawave.data.type.MacAddressType\"/>\n"
                        + "    <fieldPattern pattern=\"BANAN*\" indexType=\"datawave.data.type.DateType\"/>\n"
                        + "    <fieldPattern pattern=\"BA*\" indexed=\"true\" indexType=\"datawave.data.type.HexStringType\"/>\n" + "</fieldConfig>";

        FieldConfigHelper helper = new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper);

        List<Type<?>> types = ingestHelper.getDataTypes("BANANA");
        assertEquals(1, types.size());
        assertTrue(types.get(0) instanceof datawave.data.type.DateType);
        assertFalse(helper.isIndexedField("BANANA"));
    }

    @Test
    public void testSameLengthOverlappingRegexPrecise() throws Exception {
        conf.setBoolean(BaseIngestHelper.USE_MOST_PRECISE_FIELD_TYPE_REGEX, true);
        ingestHelper.setup(conf);

        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <default stored=\"true\" indexed=\"false\" reverseIndexed=\"false\" tokenized=\"true\" reverseTokenized=\"true\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                        + "    <fieldPattern pattern=\"B*\" indexType=\"datawave.data.type.HexStringType\"/>\n"
                        + "    <fieldPattern pattern=\"*A\" indexed=\"true\" indexType=\"datawave.data.type.MacAddressType\"/>\n" + "</fieldConfig>";

        FieldConfigHelper helper = new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper);

        List<Type<?>> types = ingestHelper.getDataTypes("BANANA");
        assertEquals(1, types.size());
        // B* should sort after *A and hence should be the one used.
        assertTrue(types.get(0) instanceof datawave.data.type.HexStringType);
        assertFalse(helper.isIndexedField("BANANA"));
    }

    @Test
    public void testCombineFile() throws Exception {
        String fileName = "datawave/ingest/test-combined-list.xml";
        URL file = ClassLoader.getSystemResource(fileName);

        String field1 = "A";
        String field2 = "D";

        HashMap<String,String[]> expectedMap = new HashMap<>();
        expectedMap.put(field1, datawave.util.StringUtils.split("B.C", '.'));
        expectedMap.put(field2, datawave.util.StringUtils.split("E.F", '.'));

        conf.addResource(file);
        conf.set("test" + BaseIngestHelper.FIELD_CONFIG_FILE, fileName);
        conf.set("test" + VirtualIngest.VirtualFieldNormalizer.VIRTUAL_FIELD_VALUE_START_SEPATATOR, "(");
        conf.set("test" + VirtualIngest.VirtualFieldNormalizer.VIRTUAL_FIELD_VALUE_END_SEPATATOR, ")");
        ingestHelper.setup(conf);

        assertTrue(ingestHelper.getVirtualFieldDefinitions().size() == 2);
        assertTrue(Arrays.equals(expectedMap.get(field1), ingestHelper.getVirtualFieldDefinitions().get(field1)));
        assertTrue(Arrays.equals(expectedMap.get(field2), ingestHelper.getVirtualFieldDefinitions().get(field2)));

        Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
        eventFields.put("B", new NormalizedFieldAndValue("B", "banana"));
        eventFields.put("C", new NormalizedFieldAndValue("C", "cantaloupe"));
        eventFields.put("E", new NormalizedFieldAndValue("E", "elderberry"));
        eventFields.put("F", new NormalizedFieldAndValue("F", "fig"));

        Multimap<String,NormalizedContentInterface> results = ingestHelper.getVirtualFields(eventFields);

        assertTrue(results.containsKey("A"));
        assertTrue(results.containsKey("D"));
        assertEquals("banana(cantaloupe)", results.get("A").iterator().next().getEventFieldValue());
        assertEquals("elderberry(fig)", results.get("D").iterator().next().getEventFieldValue());
    }

    @Test
    public void testFieldConfigHelperAllowlist() throws Exception {
        InputStream in = ClassLoader.getSystemResourceAsStream("datawave/ingest/test-field-allowlist.xml");
        XMLFieldConfigHelper helper = new XMLFieldConfigHelper(in, ingestHelper);

        // this is allowlist behavior
        assertFalse(helper.isNoMatchStored());
        assertFalse(helper.isNoMatchIndexed());
        assertFalse(helper.isNoMatchReverseIndexed());
        assertFalse(helper.isNoMatchTokenized());
        assertFalse(helper.isNoMatchReverseTokenized());

        assertTrue(helper.isStoredField("A"));
        assertTrue(helper.isStoredField("B"));
        assertTrue(helper.isStoredField("C"));
        assertTrue(helper.isStoredField("D"));
        assertTrue(helper.isStoredField("E"));
        assertFalse(helper.isStoredField("F"));
        assertFalse(helper.isStoredField("G"));
        assertTrue(helper.isStoredField("H"));

        assertFalse(helper.isIndexedField("A"));
        assertTrue(helper.isIndexedField("B"));
        assertTrue(helper.isIndexedField("C"));
        assertTrue(helper.isIndexedField("D"));
        assertFalse(helper.isIndexedField("E"));
        assertTrue(helper.isIndexedField("F"));
        assertFalse(helper.isIndexedField("G"));
        assertTrue(helper.isIndexedField("H"));

        assertTrue(helper.isReverseIndexedField("A"));
        assertFalse(helper.isReverseIndexedField("B"));
        assertTrue(helper.isReverseIndexedField("C"));
        assertTrue(helper.isReverseIndexedField("D"));
        assertFalse(helper.isReverseIndexedField("E"));
        assertTrue(helper.isReverseIndexedField("F"));
        assertFalse(helper.isReverseIndexedField("G"));
        assertTrue(helper.isReverseIndexedField("H"));

        assertFalse(helper.isTokenizedField("A"));
        assertFalse(helper.isTokenizedField("B"));
        assertTrue(helper.isTokenizedField("C"));
        assertFalse(helper.isTokenizedField("D"));
        assertFalse(helper.isTokenizedField("E"));
        assertTrue(helper.isTokenizedField("F"));
        assertFalse(helper.isTokenizedField("G"));
        assertFalse(helper.isTokenizedField("H"));

        assertFalse(helper.isReverseTokenizedField("A"));
        assertFalse(helper.isReverseTokenizedField("B"));
        assertFalse(helper.isReverseTokenizedField("C"));
        assertTrue(helper.isReverseTokenizedField("D"));
        assertFalse(helper.isReverseTokenizedField("E"));
        assertTrue(helper.isReverseTokenizedField("F"));
        assertFalse(helper.isReverseTokenizedField("G"));
        assertFalse(helper.isReverseTokenizedField("H"));

        assertFalse(helper.isIndexOnlyField("A"));
        assertFalse(helper.isIndexOnlyField("B"));
        assertFalse(helper.isIndexOnlyField("C"));
        assertFalse(helper.isIndexOnlyField("D"));
        assertFalse(helper.isIndexOnlyField("E"));
        assertTrue(helper.isIndexOnlyField("F"));
        assertFalse(helper.isIndexOnlyField("G"));
        assertFalse(helper.isIndexOnlyField("H"));

        assertType(LcNoDiacriticsType.class, ingestHelper.getDataTypes("A"));
        assertType(LcNoDiacriticsType.class, ingestHelper.getDataTypes("B"));
        assertType(LcNoDiacriticsType.class, ingestHelper.getDataTypes("C"));
        assertType(LcNoDiacriticsType.class, ingestHelper.getDataTypes("D"));
        assertType(LcNoDiacriticsType.class, ingestHelper.getDataTypes("E"));
        assertType(DateType.class, ingestHelper.getDataTypes("F"));
        assertType(HexStringType.class, ingestHelper.getDataTypes("G"));
        assertType(LcNoDiacriticsType.class, ingestHelper.getDataTypes("H"));
    }

    public static void assertType(Class<?> expected, List<Type<?>> observedList) {
        int count = 0;
        for (Type<?> observed : observedList) {

            if (expected.isAssignableFrom(observed.getClass())) {
                count++;
            }
        }
        assertEquals("Expected a single type to match " + expected.getName() + ", but " + count + " types matched; List was: " + observedList, 1, count);
    }

    @Test
    public void testFieldConfigHelperDisallowlist() throws Exception {
        InputStream in = ClassLoader.getSystemResourceAsStream("datawave/ingest/test-field-disallowlist.xml");
        XMLFieldConfigHelper helper = new XMLFieldConfigHelper(in, ingestHelper);

        // this is disallowlist behavior
        assertTrue(helper.isNoMatchStored());
        assertTrue(helper.isNoMatchIndexed());
        assertTrue(helper.isNoMatchReverseIndexed());
        assertTrue(helper.isNoMatchTokenized());
        assertTrue(helper.isNoMatchReverseTokenized());

        assertFalse(helper.isStoredField("A"));
        assertFalse(helper.isStoredField("B"));
        assertFalse(helper.isStoredField("C"));
        assertFalse(helper.isStoredField("D"));
        assertFalse(helper.isStoredField("E"));
        assertFalse(helper.isStoredField("F"));
        assertTrue(helper.isStoredField("G"));
        assertTrue(helper.isStoredField("H"));

        assertTrue(helper.isIndexedField("A"));
        assertFalse(helper.isIndexedField("B"));
        assertFalse(helper.isIndexedField("C"));
        assertFalse(helper.isIndexedField("D"));
        assertTrue(helper.isIndexedField("E"));
        assertFalse(helper.isIndexedField("F"));
        assertTrue(helper.isIndexedField("G"));
        assertFalse(helper.isIndexedField("H"));

        assertFalse(helper.isReverseIndexedField("A"));
        assertTrue(helper.isReverseIndexedField("B"));
        assertFalse(helper.isReverseIndexedField("C"));
        assertFalse(helper.isReverseIndexedField("D"));
        assertFalse(helper.isReverseIndexedField("E"));
        assertFalse(helper.isReverseIndexedField("F"));
        assertTrue(helper.isReverseIndexedField("G"));
        assertFalse(helper.isReverseIndexedField("H"));

        assertTrue(helper.isTokenizedField("A"));
        assertTrue(helper.isTokenizedField("B"));
        assertFalse(helper.isTokenizedField("C"));
        assertTrue(helper.isTokenizedField("D"));
        assertTrue(helper.isTokenizedField("E"));
        assertFalse(helper.isTokenizedField("F"));
        assertTrue(helper.isTokenizedField("G"));
        assertTrue(helper.isTokenizedField("H"));

        assertTrue(helper.isReverseTokenizedField("A"));
        assertTrue(helper.isReverseTokenizedField("B"));
        assertTrue(helper.isReverseTokenizedField("C"));
        assertFalse(helper.isReverseTokenizedField("D"));
        assertTrue(helper.isReverseTokenizedField("E"));
        assertFalse(helper.isReverseTokenizedField("F"));
        assertTrue(helper.isReverseTokenizedField("G"));
        assertTrue(helper.isReverseTokenizedField("H"));

        assertTrue(helper.isIndexOnlyField("A"));
        assertFalse(helper.isIndexOnlyField("B"));
        assertFalse(helper.isIndexOnlyField("C"));
        assertFalse(helper.isIndexOnlyField("D"));
        assertTrue(helper.isIndexOnlyField("E"));
        assertFalse(helper.isIndexOnlyField("F"));
        assertFalse(helper.isIndexOnlyField("G"));
        assertFalse(helper.isIndexOnlyField("H"));

        assertType(LcNoDiacriticsType.class, ingestHelper.getDataTypes("A"));
        assertType(LcNoDiacriticsType.class, ingestHelper.getDataTypes("B"));
        assertType(LcNoDiacriticsType.class, ingestHelper.getDataTypes("C"));
        assertType(LcNoDiacriticsType.class, ingestHelper.getDataTypes("D"));
        assertType(LcNoDiacriticsType.class, ingestHelper.getDataTypes("E"));
        assertType(LcNoDiacriticsType.class, ingestHelper.getDataTypes("F"));
        assertType(HexStringType.class, ingestHelper.getDataTypes("G"));
        assertType(DateType.class, ingestHelper.getDataTypes("H"));
    }
}
