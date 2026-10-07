package datawave.query.analysis;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import datawave.query.analysis.QueryAnalyzer.Syntax;

/**
 * Strict JSON readers and configuration writer. The stream overloads leave caller-owned streams open; path overloads close the streams they create. Validation
 * failures, including incompatible versions, are reported as {@link IOException}. Query parsing belongs to the configured analyzer, not this reader. The
 * accompanying draft-2020-12 schemas document the format without requiring a schema-validator dependency.
 */
public final class QueryTuningJson {
    public static final int MAX_SAMPLES = 9999;

    private static final ObjectMapper MAPPER = new ObjectMapper(new JsonFactory().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .disable(JsonParser.Feature.AUTO_CLOSE_SOURCE).disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET))
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private QueryTuningJson() {}

    public static List<QueryTuningSample> readSamples(Path path) throws IOException {
        try (InputStream input = Files.newInputStream(Objects.requireNonNull(path, "path"))) {
            return readSamples(input);
        }
    }

    public static List<QueryTuningSample> readSamples(InputStream input) throws IOException {
        JsonNode root = readDocument(input);
        object(root, "samples document", Set.of("schemaVersion", "samples"));
        version(root, "schemaVersion", QueryTuningConfiguration.SCHEMA_VERSION);
        JsonNode samples = required(root, "samples");
        if (!samples.isArray() || samples.size() < 1 || samples.size() > MAX_SAMPLES) {
            throw invalid("samples must be an array containing 1 to " + MAX_SAMPLES + " entries");
        }
        List<QueryTuningSample> result = new ArrayList<>(samples.size());
        Set<String> ids = new HashSet<>();
        for (int index = 0; index < samples.size(); index++) {
            try {
                JsonNode sample = samples.get(index);
                object(sample, "sample", Set.of("id", "query", "syntax", "bucket"));
                String id = sample.has("id") ? text(sample, "id") : null;
                if (id != null && !ids.add(id)) {
                    throw invalid("Duplicate sample id");
                }
                String query = text(sample, "query");
                String syntax = text(sample, "syntax");
                String bucket = text(sample, "bucket");
                result.add(new QueryTuningSample(id, query, Syntax.valueOf(syntax), bucket));
            } catch (IOException e) {
                throw invalid("samples[" + index + "]: " + e.getMessage(), e);
            } catch (IllegalArgumentException e) {
                throw invalid("samples[" + index + "]: syntax must be LUCENE or JEXL", e);
            }
        }
        return Collections.unmodifiableList(result);
    }

    public static QueryTuningConfiguration readConfiguration(Path path) throws IOException {
        try (InputStream input = Files.newInputStream(Objects.requireNonNull(path, "path"))) {
            return readConfiguration(input);
        }
    }

    public static QueryTuningConfiguration readConfiguration(InputStream input) throws IOException {
        JsonNode root = readDocument(input);
        object(root, "configuration", Set.of("schemaVersion", "fingerprintVersion", "clustering"));
        version(root, "schemaVersion", QueryTuningConfiguration.SCHEMA_VERSION);
        version(root, "fingerprintVersion", QueryTuningConfiguration.FINGERPRINT_VERSION);
        JsonNode clustering = required(root, "clustering");
        object(clustering, "clustering", Set.of("threshold", "featureLimit", "postingLimit", "candidateLimit", "weights"));
        JsonNode weights = required(clustering, "weights");
        object(weights, "weights", Set.of("bindings", "counts", "topology", "complexity"));
        double threshold = number(clustering, "threshold");
        if (clustering.get("threshold").decimalValue().compareTo(BigDecimal.ONE) > 0) {
            throw invalid("threshold must be in [0,1]");
        }
        int features = integer(clustering, "featureLimit");
        int postings = integer(clustering, "postingLimit");
        int candidates = integer(clustering, "candidateLimit");
        try {
            QuerySimilarity.Weights similarity = new QuerySimilarity.Weights(number(weights, "bindings"), number(weights, "counts"),
                            number(weights, "topology"), number(weights, "complexity"));
            return new QueryTuningConfiguration(new QueryClusterer.Options(threshold, features, postings, candidates, similarity));
        } catch (IllegalArgumentException e) {
            throw invalid("Invalid clustering options: " + e.getMessage(), e);
        }
    }

    public static void writeConfiguration(Path path, QueryTuningConfiguration configuration) throws IOException {
        Objects.requireNonNull(configuration, "configuration");
        try (OutputStream output = Files.newOutputStream(Objects.requireNonNull(path, "path"))) {
            writeConfiguration(output, configuration);
        }
    }

    public static void writeConfiguration(OutputStream output, QueryTuningConfiguration configuration) throws IOException {
        Objects.requireNonNull(configuration, "configuration");
        QueryClusterer.Options options = configuration.toClusteringOptions();
        QuerySimilarity.Weights weights = options.getWeights();
        try (JsonGenerator json = MAPPER.getFactory().createGenerator(Objects.requireNonNull(output, "output"))) {
            json.useDefaultPrettyPrinter();
            json.writeStartObject();
            json.writeNumberField("schemaVersion", configuration.getSchemaVersion());
            json.writeNumberField("fingerprintVersion", configuration.getFingerprintVersion());
            json.writeObjectFieldStart("clustering");
            json.writeNumberField("threshold", options.getThreshold());
            json.writeNumberField("featureLimit", options.getFeatureLimit());
            json.writeNumberField("postingLimit", options.getPostingLimit());
            json.writeNumberField("candidateLimit", options.getCandidateLimit());
            json.writeObjectFieldStart("weights");
            json.writeNumberField("bindings", weights.getBindings());
            json.writeNumberField("counts", weights.getCounts());
            json.writeNumberField("topology", weights.getTopology());
            json.writeNumberField("complexity", weights.getComplexity());
            json.writeEndObject();
            json.writeEndObject();
            json.writeEndObject();
        }
    }

    private static JsonNode readDocument(InputStream input) throws IOException {
        try (JsonParser parser = MAPPER.getFactory().createParser(Objects.requireNonNull(input, "input"))) {
            JsonNode root = MAPPER.readTree(parser);
            if (root == null || parser.nextToken() != null) {
                throw invalid("Expected exactly one JSON document");
            }
            return root;
        }
    }

    private static void object(JsonNode node, String name, Set<String> properties) throws IOException {
        if (!node.isObject()) {
            throw invalid(name + " must be an object");
        }
        Iterator<String> fields = node.fieldNames();
        while (fields.hasNext()) {
            String field = fields.next();
            if (!properties.contains(field)) {
                throw invalid("Unknown property in " + name + ": " + field);
            }
        }
    }

    private static JsonNode required(JsonNode node, String name) throws IOException {
        JsonNode value = node.get(name);
        if (value == null || value.isNull()) {
            throw invalid("Required property: " + name);
        }
        return value;
    }

    private static String text(JsonNode node, String name) throws IOException {
        JsonNode value = required(node, name);
        if (!value.isTextual() || value.textValue().isBlank()) {
            throw invalid(name + " must be a nonblank string");
        }
        return value.textValue();
    }

    private static int integer(JsonNode node, String name) throws IOException {
        JsonNode value = required(node, name);
        if (!value.isNumber()) {
            throw invalid(name + " must be an integer in the Java int range");
        }
        try {
            return value.decimalValue().intValueExact();
        } catch (ArithmeticException e) {
            throw invalid(name + " must be an integer in the Java int range", e);
        }
    }

    private static double number(JsonNode node, String name) throws IOException {
        JsonNode value = required(node, name);
        if (!value.isNumber() || !Double.isFinite(value.doubleValue()) || value.decimalValue().signum() < 0
                        || value.decimalValue().compareTo(BigDecimal.valueOf(Double.MAX_VALUE)) > 0) {
            throw invalid(name + " must be a finite nonnegative JSON number within the Java double range");
        }
        return value.doubleValue();
    }

    private static void version(JsonNode node, String name, int expected) throws IOException {
        if (integer(node, name) != expected) {
            throw invalid("Unsupported " + name + "; expected " + expected);
        }
    }

    private static IOException invalid(String message) {
        return new IOException(message);
    }

    private static IOException invalid(String message, Exception cause) {
        return new IOException(message, cause);
    }
}
