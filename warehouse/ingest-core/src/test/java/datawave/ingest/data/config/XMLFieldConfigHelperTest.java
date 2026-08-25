package datawave.ingest.data.config;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import datawave.TestBaseIngestHelper;
import datawave.data.type.DateType;
import datawave.data.type.HexStringType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.mapreduce.SimpleDataTypeHandler;
import datawave.policy.IngestPolicyEnforcer;

public class XMLFieldConfigHelperTest {

    private final BaseIngestHelper ingestHelper = new TestBaseIngestHelper();
    private final Configuration conf = new Configuration();

    @BeforeEach
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

            assertTrue(helper.isIndexedField("A"));
            assertFalse(helper.isIndexedField("B"));

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
        InputStream is = getClass().getClassLoader().getResourceAsStream(path);
        assertNotNull(is);
        try (Scanner scanner = new Scanner(is)) {
            while (scanner.hasNext()) {
                sb.append(scanner.nextLine()).append("\n");
            }
        }

        return sb.toString();
    }

    @Test
    public void testBadTag() {
        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <default stored=\"true\" indexed=\"false\" reverseIndexed=\"false\" tokenized=\"true\" reverseTokenized=\"true\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                        + "    <nomatch stored=\"true\" indexed=\"true\" reverseIndexed=\"true\" tokenized=\"true\"  reverseTokenized=\"true\" indexType=\"datawave.data.type.HexStringType\"/>\n"
                        + "    <fieldPattern pattern=\"*J\" indexed=\"true\" indexType=\"datawave.data.type.MacAddressType\"/>\n"
                        + "    <field name=\"H\" indexType=\"datawave.data.type.DateType\"/>\n"
                        + "    <orange name=\"H\" indexType=\"datawave.data.type.DateType\"/>\n" + "</fieldConfig>";

        assertThrows(IllegalArgumentException.class, () -> new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper));
    }

    @Test
    public void testDuplicateField() {
        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <default stored=\"true\" indexed=\"false\" reverseIndexed=\"false\" tokenized=\"true\" reverseTokenized=\"true\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                        + "    <nomatch stored=\"true\" indexed=\"true\" reverseIndexed=\"true\" tokenized=\"true\"  reverseTokenized=\"true\" indexType=\"datawave.data.type.HexStringType\"/>\n"
                        + "    <fieldPattern pattern=\"*J\" indexed=\"true\" indexType=\"datawave.data.type.MacAddressType\"/>\n"
                        + "    <field name=\"H\" indexType=\"datawave.data.type.DateType\"/>\n"
                        + "    <field name=\"H\" indexType=\"datawave.data.type.HexStringType\"/>\n" + "</fieldConfig>";

        assertThrows(IllegalArgumentException.class, () -> new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper));
    }

    @Test
    public void testMissingDefault() {
        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <nomatch stored=\"true\" indexed=\"true\" reverseIndexed=\"true\" tokenized=\"true\"  reverseTokenized=\"true\" indexType=\"datawave.data.type.HexStringType\"/>\n"
                        + "    <field name=\"A\" indexed=\"true\"/>\n" + "</fieldConfig>";

        assertThrows(IllegalStateException.class, () -> new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper));
    }

    @Test
    public void testIncompleteDefault() {
        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <default stored=\"true\" reverseIndexed=\"false\" tokenized=\"true\" reverseTokenized=\"true\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                        + "    <nomatch stored=\"true\" indexed=\"true\" reverseIndexed=\"true\" tokenized=\"true\"  reverseTokenized=\"true\" indexType=\"datawave.data.type.HexStringType\"/>\n"
                        + "    <fieldPattern pattern=\"*J\" indexed=\"true\" indexType=\"datawave.data.type.MacAddressType\"/>\n"
                        + "    <field name=\"A\" indexed=\"true\"/>\n" +

                        "</fieldConfig>";

        assertThrows(IllegalArgumentException.class, () -> new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper));
    }

    @Test
    public void testMissingNomatch() throws Exception {
        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <default stored=\"true\" indexed=\"false\" reverseIndexed=\"false\" tokenized=\"true\" reverseTokenized=\"true\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                        + "    <fieldPattern pattern=\"*J\" indexed=\"true\" indexType=\"datawave.data.type.MacAddressType\"/>\n"
                        + "    <field name=\"H\" indexType=\"datawave.data.type.DateType\"/>\n" + "</fieldConfig>";

        FieldConfigHelper helper = new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper);
        assertNotNull(helper, "Assertion to prevent unused warning on the helper");
        // ok.
    }

    @Test
    public void testIncompleteNomatch() {
        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <default stored=\"true\" indexed=\"false\" reverseIndexed=\"false\" tokenized=\"true\" reverseTokenized=\"true\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                        + "    <nomatch stored=\"true\" reverseIndexed=\"true\" tokenized=\"true\"  reverseTokenized=\"true\" indexType=\"datawave.data.type.HexStringType\"/>\n"
                        + "    <fieldPattern pattern=\"*J\" indexed=\"true\" indexType=\"datawave.data.type.MacAddressType\"/>\n"
                        + "    <field name=\"H\" indexType=\"datawave.data.type.DateType\"/>\n" + "</fieldConfig>";

        assertThrows(IllegalArgumentException.class, () -> new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper));
    }

    @Test
    public void testMultiType() throws Exception {
        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <default stored=\"true\" indexed=\"false\" reverseIndexed=\"false\" tokenized=\"true\" reverseTokenized=\"true\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                        + "    <fieldPattern pattern=\"*J\" indexed=\"true\" indexType=\"datawave.data.type.MacAddressType\"/>\n"
                        + "    <field name=\"H\" indexType=\"datawave.data.type.DateType,datawave.data.type.HexStringType\"/>\n" + "</fieldConfig>";

        FieldConfigHelper helper = new XMLFieldConfigHelper(new ByteArrayInputStream(input.getBytes()), ingestHelper);
        assertNotNull(helper, "Assertion to prevent unused warning on the helper");

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
        assertNotNull(helper, "Assertion to prevent unused warning on the helper");

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
        assertInstanceOf(DateType.class, types.get(0));
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
        assertInstanceOf(HexStringType.class, types.get(0));
        assertFalse(helper.isIndexedField("BANANA"));
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
        assertEquals(1, count, "Expected a single type to match " + expected.getName() + ", but " + count + " types matched; List was: " + observedList);
    }

    @Test
    public void testFieldConfigHelperDisallowList() throws Exception {
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

    @Test
    void testCachingBehaviorWillCallBaseMethods() throws Exception {
        // test intent is to verify each is*Field accessor dispatches to the matching FieldInfo attribute
        // (i.e. no copy/paste error) and that a single lookup memoizes the fully-resolved FieldInfo

        String input = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                        + "    <default stored=\"true\" indexed=\"false\" reverseIndexed=\"false\" tokenized=\"false\" reverseTokenized=\"false\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                        + "    <nomatch stored=\"true\" indexed=\"true\" reverseIndexed=\"true\" tokenized=\"true\"  reverseTokenized=\"true\" indexType=\"datawave.data.type.HexStringType\"/>\n"
                        + "    <field name=\"A\" stored=\"true\" indexed=\"true\" reverseIndexed=\"false\" tokenized=\"true\" reverseTokenized=\"false\"/>\n"
                        + "    <field name=\"B\" stored=\"true\" indexed=\"false\" reverseIndexed=\"true\" tokenized=\"true\" reverseTokenized=\"false\"/>\n"
                        + "    <field name=\"C\" stored=\"false\" indexed=\"true\" reverseIndexed=\"true\" tokenized=\"true\" reverseTokenized=\"false\"/>\n"
                        + "</fieldConfig>";

        String field = "A";
        XMLFieldConfigHelper helper = new XMLFieldConfigHelper(IOUtils.toInputStream(input, UTF_8), ingestHelper);
        Map<String,XMLFieldConfigHelper.FieldInfo> cache = helper.getResolvedFields();

        // a single lookup resolves and memoizes the whole FieldInfo for the field
        assertTrue(cache.isEmpty());
        helper.isStoredField(field);
        assertEquals(1, cache.size());

        XMLFieldConfigHelper.FieldInfo info = cache.get(field);
        assertNotNull(info);
        assertTrue(info.stored);
        assertTrue(info.indexed);
        assertFalse(info.reverseIndexed);
        assertTrue(info.tokenized);
        assertFalse(info.reverseTokenized);

        // each accessor returns the matching flag on the resolved FieldInfo
        assertEquals(info.stored, helper.isStoredField(field));
        assertEquals(info.indexed, helper.isIndexedField(field));
        assertEquals(info.reverseIndexed, helper.isReverseIndexedField(field));
        assertEquals(info.tokenized, helper.isTokenizedField(field));
        assertEquals(info.reverseTokenized, helper.isReverseTokenizedField(field));
        assertEquals(info.indexed && !info.stored, helper.isIndexOnlyField(field));

        // flags across A/B/C give every attribute a distinct value signature, so an accessor
        // dispatching to the wrong attribute fails on at least one of the three fields
        assertTrue(helper.isStoredField("B"));
        assertFalse(helper.isIndexedField("B"));
        assertTrue(helper.isReverseIndexedField("B"));
        assertTrue(helper.isTokenizedField("B"));
        assertFalse(helper.isReverseTokenizedField("B"));
        assertFalse(helper.isIndexOnlyField("B"));

        assertFalse(helper.isStoredField("C"));
        assertTrue(helper.isIndexedField("C"));
        assertTrue(helper.isReverseIndexedField("C"));
        assertTrue(helper.isTokenizedField("C"));
        assertFalse(helper.isReverseTokenizedField("C"));
        assertTrue(helper.isIndexOnlyField("C"));

        // repeated lookups of a known field return the same cached FieldInfo instance
        assertSame(info, cache.get(field));

        // two different unknown fields resolve to the same shared no-match FieldInfo instance
        helper.isStoredField("UNKNOWNONE");
        helper.isStoredField("UNKNOWNTWO");
        XMLFieldConfigHelper.FieldInfo noMatchOne = cache.get("UNKNOWNONE");
        XMLFieldConfigHelper.FieldInfo noMatchTwo = cache.get("UNKNOWNTWO");
        assertNotNull(noMatchOne);
        assertSame(noMatchOne, noMatchTwo);
    }
}
