package datawave.query.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import datawave.query.analysis.QueryAnalyzer.Syntax;

public class QueryTuningJsonTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SAMPLE = "{\"query\":\"A == 1\",\"syntax\":\"JEXL\",\"bucket\":\"a\"}";
    private static final String SAMPLES = "{\"schemaVersion\":1,\"samples\":[" + SAMPLE + "]}";
    private static final String CONFIGURATION = "{\"schemaVersion\":1,\"fingerprintVersion\":2,\"clustering\":{\"threshold\":0.7,"
                    + "\"featureLimit\":4,\"postingLimit\":32,\"candidateLimit\":64,\"weights\":{\"bindings\":0.35,\"counts\":0.3,\"topology\":0.2,\"complexity\":0.15}}}";

    @Test
    public void readsMixedSyntaxAndPreservesStrings() throws Exception {
        List<QueryTuningSample> samples = readSamples("{\"schemaVersion\":1,\"samples\":["
                        + "{\"id\":\" first \",\"query\":\"  NAME:alice  \",\"syntax\":\"LUCENE\",\"bucket\":\" identity \"}," + SAMPLE + "]}");
        assertEquals(2, samples.size());
        assertEquals(" first ", samples.get(0).getId());
        assertEquals("  NAME:alice  ", samples.get(0).getQuery());
        assertEquals(" identity ", samples.get(0).getBucket());
        assertEquals(Syntax.LUCENE, samples.get(0).getSyntax());
        assertNull(samples.get(1).getId());
        assertEquals(Syntax.JEXL, samples.get(1).getSyntax());
        assertThrows(UnsupportedOperationException.class, () -> samples.clear());
    }

    @Test
    public void rejectsInvalidSampleDocuments() throws Exception {
        for (String document : new String[] {"", "null", "[]", "{}", "{\"schemaVersion\":2,\"samples\":[" + SAMPLE + "]}",
                SAMPLES.replace("\"schemaVersion\":1", "\"schemaVersion\":\"1\""), SAMPLES.replace("\"schemaVersion\":1", "\"schemaVersion\":true"),
                SAMPLES.replace("\"schemaVersion\":1", "\"schemaVersion\":1.0000000000000001"),
                SAMPLES.replace("\"samples\":[" + SAMPLE + "]", "\"samples\":[]"), SAMPLES.replace("\"samples\":[" + SAMPLE + "]", "\"samples\":{}"),
                SAMPLES.replace("\"samples\":", "\"extra\":1,\"samples\":"), SAMPLES.replace("\"schemaVersion\":1", "\"schemaVersion\":1,\"schemaVersion\":1"),
                SAMPLES + " {}", SAMPLES + " true", SAMPLES + " garbage"}) {
            assertThrows(document, IOException.class, () -> readSamples(document));
        }
    }

    @Test
    public void rejectsInvalidSampleFieldsWithIndexContext() throws Exception {
        for (String sample : new String[] {"null", "[]", "{}", SAMPLE.replace("\"query\":\"A == 1\"", "\"query\":null"),
                SAMPLE.replace("\"query\":\"A == 1\"", "\"query\":42"), SAMPLE.replace("\"query\":\"A == 1\"", "\"query\":\" \\t\""),
                SAMPLE.replace("\"bucket\":\"a\"", "\"bucket\":true"), SAMPLE.replace("\"bucket\":\"a\"", "\"bucket\":\"\\u2003\""),
                SAMPLE.replace("\"syntax\":\"JEXL\"", "\"syntax\":\"jexl\""), SAMPLE.replace("\"syntax\":\"JEXL\"", "\"syntax\":0"),
                SAMPLE.replace("\"query\":", "\"id\":null,\"query\":"), SAMPLE.replace("\"query\":", "\"id\":\" \",\"query\":"),
                SAMPLE.replace("\"query\":", "\"other\":\"value\",\"query\":")}) {
            IOException failure = assertThrows(IOException.class, () -> readSamples("{\"schemaVersion\":1,\"samples\":[" + sample + "]}"));
            assertTrue(failure.getMessage(), failure.getMessage().contains("samples[0]"));
        }
        assertThrows(IOException.class, () -> readSamples(SAMPLES.replace("\"query\":", "\"bucket\":\"b\",\"query\":")));
    }

    @Test
    public void requiresUniqueProvidedIdsButAllowsDuplicateOccurrencesAndOmittedIds() throws Exception {
        assertEquals(2, readSamples("{\"schemaVersion\":1,\"samples\":[" + SAMPLE + "," + SAMPLE + "]}").size());
        String identified = SAMPLE.replace("\"query\":", "\"id\":\"same\",\"query\":");
        assertThrows(IOException.class, () -> readSamples("{\"schemaVersion\":1,\"samples\":[" + identified + "," + identified + "]}"));
        assertEquals(2, readSamples("{\"schemaVersion\":1,\"samples\":[" + identified + "," + identified.replace("same", " same ") + "]}").size());
    }

    @Test
    public void enforcesSampleCountBoundary() throws Exception {
        String many = String.join(",", java.util.Collections.nCopies(QueryTuningJson.MAX_SAMPLES, SAMPLE));
        assertEquals(9999, readSamples("{\"schemaVersion\":1,\"samples\":[" + many + "]}").size());
        assertThrows(IOException.class, () -> readSamples("{\"schemaVersion\":1,\"samples\":[" + many + "," + SAMPLE + "]}"));
    }

    @Test
    public void configurationRoundTripsAllOptionsExactly() throws Exception {
        for (QuerySimilarity.Weights weights : new QuerySimilarity.Weights[] {QuerySimilarity.Weights.DEFAULT, new QuerySimilarity.Weights(1, 1, 1, 0),
                new QuerySimilarity.Weights(8, 3, 2, 1), new QuerySimilarity.Weights(0, 0, 0, 1)}) {
            QueryClusterer.Options options = new QueryClusterer.Options(0.81234567890123, 8, 96, 128, weights);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            QueryTuningJson.writeConfiguration(output, new QueryTuningConfiguration(options));
            QueryTuningConfiguration decoded = QueryTuningJson.readConfiguration(new ByteArrayInputStream(output.toByteArray()));
            assertOptions(options, decoded.toClusteringOptions());
            assertEquals(1, decoded.getSchemaVersion());
            assertEquals(2, decoded.getFingerprintVersion());
        }
    }

    @Test
    public void acceptsMathematicalIntegersAndNormalizesRelativeWeights() throws Exception {
        assertEquals(1, readSamples(SAMPLES.replace("\"schemaVersion\":1", "\"schemaVersion\":1.0")).size());
        QueryClusterer.Options options = readConfiguration(CONFIGURATION.replace("\"schemaVersion\":1", "\"schemaVersion\":1e0")
                        .replace("\"fingerprintVersion\":2", "\"fingerprintVersion\":2.0").replace("\"featureLimit\":4", "\"featureLimit\":4.00")
                        .replace("\"bindings\":0.35", "\"bindings\":3.5").replace("\"counts\":0.3", "\"counts\":3")
                        .replace("\"topology\":0.2", "\"topology\":2").replace("\"complexity\":0.15", "\"complexity\":1.5")).toClusteringOptions();
        assertEquals(4, options.getFeatureLimit());
        assertEquals(0.35, options.getWeights().getBindings(), 0);
        assertEquals(0.30, options.getWeights().getCounts(), 0);
        assertEquals(0.20, options.getWeights().getTopology(), 0);
        assertEquals(0.15, options.getWeights().getComplexity(), 0);
    }

    @Test
    public void rejectsConfigurationVersionsPropertiesAndWrongTypes() throws Exception {
        for (String invalid : new String[] {CONFIGURATION + " {}", CONFIGURATION.replace("\"schemaVersion\":1", "\"schemaVersion\":2"),
                CONFIGURATION.replace("\"fingerprintVersion\":2", "\"fingerprintVersion\":1"),
                CONFIGURATION.replace("\"schemaVersion\":1", "\"schemaVersion\":1.0000000000000001"),
                CONFIGURATION.replace("\"schemaVersion\":1", "\"schemaVersion\":\"1\""),
                CONFIGURATION.replace("\"clustering\":", "\"other\":1,\"clustering\":"), CONFIGURATION.replace("\"threshold\":", "\"other\":1,\"threshold\":"),
                CONFIGURATION.replace("\"bindings\":", "\"other\":1,\"bindings\":"), CONFIGURATION.replace("\"threshold\":0.7", "\"threshold\":\"0.7\""),
                CONFIGURATION.replace("\"threshold\":0.7", "\"threshold\":true"), CONFIGURATION.replace("\"weights\":{", "\"weights\":{\"bindings\":1,"),
                CONFIGURATION.replace("\"threshold\":0.7", "\"threshold\":null"), CONFIGURATION.replace("\"featureLimit\":4,", "")}) {
            assertThrows(invalid, IOException.class, () -> readConfiguration(invalid));
        }
    }

    @Test
    public void rejectsUnsafeNumericOptionsWithoutTruncatingOrRounding() throws Exception {
        for (String field : new String[] {"featureLimit", "postingLimit", "candidateLimit"}) {
            ObjectNode root = (ObjectNode) MAPPER.readTree(CONFIGURATION);
            ObjectNode options = (ObjectNode) root.get("clustering");
            for (String number : new String[] {"0", "-1", "1.5", "2147483648", "1e999", "4.0000000000000001", "\"4\"", "true"}) {
                options.set(field, MAPPER.readTree(number));
                String document = root.toString();
                // Use the original decimal text for near-integer checks rather than the test mapper's rounded double tree.
                if (number.equals("4.0000000000000001")) {
                    options.put(field, 4);
                    document = root.toString().replace("\"" + field + "\":4", "\"" + field + "\":" + number);
                }
                final String invalid = document;
                assertThrows(invalid, IOException.class, () -> readConfiguration(invalid));
            }
        }
        for (String number : new String[] {"-0.01", "1.01", "1e999", "1.00000000000000001", "-1e-999"}) {
            assertThrows(IOException.class, () -> readConfiguration(CONFIGURATION.replace("\"threshold\":0.7", "\"threshold\":" + number)));
        }
        for (String number : new String[] {"-1", "-1e-999", "1e999", "\"0.35\"", "null", "true"}) {
            assertThrows(IOException.class, () -> readConfiguration(CONFIGURATION.replace("\"bindings\":0.35", "\"bindings\":" + number)));
        }
        assertThrows(IOException.class,
                        () -> readConfiguration(CONFIGURATION.replace("0.35", "0").replace("0.3", "0").replace("0.2", "0").replace("0.15", "0")));
    }

    @Test
    public void callerOwnedStreamsStayOpenOnSuccessAndFailure() throws Exception {
        TrackingInput samples = new TrackingInput(SAMPLES);
        QueryTuningJson.readSamples(samples);
        assertFalse(samples.closed);
        TrackingInput configuration = new TrackingInput(CONFIGURATION);
        QueryTuningJson.readConfiguration(configuration);
        assertFalse(configuration.closed);
        TrackingInput invalid = new TrackingInput("{");
        assertThrows(IOException.class, () -> QueryTuningJson.readSamples(invalid));
        assertFalse(invalid.closed);
        TrackingOutput output = new TrackingOutput();
        QueryTuningJson.writeConfiguration(output, new QueryTuningConfiguration(new QueryClusterer.Options()));
        assertFalse(output.closed);
        assertOptions(new QueryClusterer.Options(), QueryTuningJson.readConfiguration(new ByteArrayInputStream(output.toByteArray())).toClusteringOptions());
    }

    @Test
    public void pathHelpersReadSamplesAndRoundTripConfiguration() throws Exception {
        Path path = Files.createTempFile("query-tuning", ".json");
        try {
            Files.writeString(path, SAMPLES);
            assertEquals(1, QueryTuningJson.readSamples(path).size());
            QueryClusterer.Options options = new QueryClusterer.Options();
            QueryTuningJson.writeConfiguration(path, new QueryTuningConfiguration(options));
            assertOptions(options, QueryTuningJson.readConfiguration(path).toClusteringOptions());
        } finally {
            Files.deleteIfExists(path);
        }
    }

    @Test
    public void publishedExamplesAndSchemasAreAvailable() throws Exception {
        try (InputStream samples = resource("query-tuning-samples-example.json");
                        InputStream configuration = resource("query-tuning-configuration-example.json")) {
            assertEquals(8, QueryTuningJson.readSamples(samples).size());
            assertOptions(new QueryClusterer.Options(), QueryTuningJson.readConfiguration(configuration).toClusteringOptions());
        }
        for (String name : new String[] {"query-tuning-samples.schema.json", "query-tuning-configuration.schema.json"}) {
            try (InputStream schema = resource(name)) {
                JsonNode document = MAPPER.readTree(schema);
                assertEquals("https://json-schema.org/draft/2020-12/schema", document.get("$schema").textValue());
                assertFalse(document.get("additionalProperties").booleanValue());
            }
        }
    }

    private static InputStream resource(String name) throws IOException {
        InputStream stream = QueryTuningJsonTest.class.getResourceAsStream("/datawave/query/analysis/" + name);
        if (stream == null) {
            throw new IOException("Missing resource: " + name);
        }
        return stream;
    }

    private static List<QueryTuningSample> readSamples(String document) throws IOException {
        return QueryTuningJson.readSamples(new ByteArrayInputStream(document.getBytes(StandardCharsets.UTF_8)));
    }

    private static QueryTuningConfiguration readConfiguration(String document) throws IOException {
        return QueryTuningJson.readConfiguration(new ByteArrayInputStream(document.getBytes(StandardCharsets.UTF_8)));
    }

    private static void assertOptions(QueryClusterer.Options expected, QueryClusterer.Options actual) {
        assertEquals(expected.getThreshold(), actual.getThreshold(), 0);
        assertEquals(expected.getFeatureLimit(), actual.getFeatureLimit());
        assertEquals(expected.getPostingLimit(), actual.getPostingLimit());
        assertEquals(expected.getCandidateLimit(), actual.getCandidateLimit());
        assertEquals(expected.getWeights().getBindings(), actual.getWeights().getBindings(), 0);
        assertEquals(expected.getWeights().getCounts(), actual.getWeights().getCounts(), 0);
        assertEquals(expected.getWeights().getTopology(), actual.getWeights().getTopology(), 0);
        assertEquals(expected.getWeights().getComplexity(), actual.getWeights().getComplexity(), 0);
    }

    private static final class TrackingInput extends ByteArrayInputStream {
        private boolean closed;

        private TrackingInput(String document) {
            super(document.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }

    private static final class TrackingOutput extends ByteArrayOutputStream {
        private boolean closed;

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }
}
