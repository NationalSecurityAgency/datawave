package datawave.ingest.json.config.helper;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.json.util.JsonObjectFlattener;
import datawave.ingest.json.util.JsonObjectFlattenerImpl;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * <p>
 * Custom {@link JsonObjectFlattener} implementation that has a {@link JsonDataTypeHelper} instance, in order to utilize some of its behaviors, e.g., its
 * key/value normalization behavior, allowlist/disallowlist options, etc
 *
 * <p>
 * Forces path delimiters and ordinal position delimiters to the appropriate values, based on the configured {@link JsonObjectFlattener.FlattenMode}, and based
 * on DataWave field name format requirements
 */
public class JsonIngestFlattener extends JsonObjectFlattenerImpl {

    private static final String EMPTY_STRING = "";
    private static final String UNDERSCORE = "_";
    private static final String PERIOD = ".";

    private static final String DELIMITER_WITH_ORDINAL_PATTERN = UNDERSCORE + "\\d+";
    private static final String PERIOD_LITERAL_PATTERN = "\\" + PERIOD;

    protected final JsonDataTypeHelper jsonDataTypeHelper;

    protected JsonIngestFlattener(Builder builder) {
        super(builder);
        this.jsonDataTypeHelper = builder.jsonDataTypeHelper;
    }

    @Override
    protected void mapPut(String currentPath, String currentValue, Multimap<String, String> map, Map<String, Integer> occurrenceCounts) {
        String key = this.keyValueNormalizer.normalizeMapKey(currentPath, currentValue);
        String value = this.keyValueNormalizer.normalizeMapValue(currentValue, key);
        if (!ignoreKeyValue(key, value)) {

            if (this.flattenMode == FlattenMode.GROUPED || this.flattenMode == FlattenMode.GROUPED_AND_NORMAL) {

                if (this.flattenMode == FlattenMode.GROUPED_AND_NORMAL) {
                    // Build typical GROUPED key suffix, but with NORMAL key prefix instead
                    key = getNormalKeyFromGroupedContext(key) + getFieldAndContextSuffix(key, value, incrementCount(key, occurrenceCounts));
                } else {
                    key = getFieldAndContextSuffix(key, value, incrementCount(key, occurrenceCounts));
                }
            }

            map.put(key, value);
        }
    }

    @Override
    protected String getFieldAndContextSuffix(String fieldPath, String value, int occurrence) {
        int fieldNameIndex = fieldPath.lastIndexOf(this.pathDelimiter) + 1;
        if (fieldNameIndex == 0) {
            /*
             * We avoid adding the context suffix to root-level fields here, since there's no nesting for those, and since there's little chance that it would
             * ever be useful, except *maybe* in root-level arrays where the position of its elements has some meaning to the end user...
             *
             * For example, root-level array keys would otherwise be surfaced here as FOO.FOO_0, FOO.FOO_1, FOO.FOO_2, etc. We drop that context here. If you
             * need it, then just override this method in a subclass
             */
            return fieldPath;
        }
        if (this.flattenMode == FlattenMode.GROUPED_AND_NORMAL) {
            // Only return the suffix
            return this.pathDelimiter + fieldPath + this.occurrenceDelimiter + occurrence;
        } else {
            return fieldPath.substring(fieldNameIndex) + this.pathDelimiter + fieldPath + this.occurrenceDelimiter + occurrence;
        }
    }

    @Override
    protected String getNormalKeyFromGroupedContext(String groupedKey) {
        Preconditions.checkArgument(this.flattenMode == FlattenMode.GROUPED_AND_NORMAL);
        // First, strip any context ordinals and associated delimiters
        String normalKey = groupedKey.replaceAll(DELIMITER_WITH_ORDINAL_PATTERN, EMPTY_STRING);
        // Replace periods with underscrores
        normalKey = normalKey.replaceAll(PERIOD_LITERAL_PATTERN, UNDERSCORE);
        if (normalKey.equals(groupedKey)) {
            // Root primitive. No path. No group context
            return EMPTY_STRING;
        }
        return normalKey;
    }

    public static class Builder extends JsonObjectFlattenerImpl.Builder {

        protected JsonDataTypeHelper jsonDataTypeHelper = null;

        public Builder jsonDataTypeHelper(JsonDataTypeHelper jsonDataTypeHelper) {
            this.jsonDataTypeHelper = jsonDataTypeHelper;
            return this;
        }

        @Override
        public JsonIngestFlattener build() {
            if (null == this.jsonDataTypeHelper) {
                this.jsonDataTypeHelper = new JsonDataTypeHelper();
                this.jsonDataTypeHelper.setJsonObjectFlattenMode(this.flattenMode);
            }

            if (null == this.nameNormalizer) {
                this.nameNormalizer = new DefaultJsonElementNameNormalizer(this.jsonDataTypeHelper);
            }

            if (null == this.keyValueNormalizer) {
                this.keyValueNormalizer = new DefaultMapKeyValueNormalizer(this.jsonDataTypeHelper);
            }

            if (this.flattenMode == FlattenMode.GROUPED || this.flattenMode == FlattenMode.GROUPED_AND_NORMAL) {
                // Force pathDelimiter and occurrenceDelimiter per DW's grouping requirements
                this.pathDelimiter = PERIOD;
                this.occurrenceDelimiter = UNDERSCORE;
                return new JsonIngestFlattener(this);
            }
            this.pathDelimiter = UNDERSCORE;
            return new JsonIngestFlattener(this);
        }
    }

    public static final class DefaultMapKeyValueNormalizer implements MapKeyValueNormalizer {

        private JsonDataTypeHelper jsonDataTypeHelper;

        private DefaultMapKeyValueNormalizer() {
        }

        public DefaultMapKeyValueNormalizer(JsonDataTypeHelper jsonDataTypeHelper) {
            this.jsonDataTypeHelper = jsonDataTypeHelper;
        }

        @Override
        public String normalizeMapKey(String key, String value) throws IllegalStateException {
            return key.toUpperCase();
        }

        @Override
        public String normalizeMapValue(String value, String key) throws IllegalStateException {
            return this.jsonDataTypeHelper.clean(key, value);
        }
    }

    public static final class DefaultJsonElementNameNormalizer implements JsonElementNameNormalizer {

        private JsonDataTypeHelper jsonDataTypeHelper;

        private DefaultJsonElementNameNormalizer() {
        }

        public DefaultJsonElementNameNormalizer(JsonDataTypeHelper jsonDataTypeHelper) {
            this.jsonDataTypeHelper = jsonDataTypeHelper;
        }

        @Override
        public String normalizeElementName(String elementName, String parentKey) throws IllegalStateException {

            // No periods allowed in DW base field names
            elementName = elementName.replaceAll(PERIOD_LITERAL_PATTERN, EMPTY_STRING);
            switch (this.jsonDataTypeHelper.getJsonObjectFlattenMode()) {
                case NORMAL:
                case GROUPED:
                case GROUPED_AND_NORMAL:
                    // Also strip underscores since that's our occurrence delimiter
                    elementName = elementName.replaceAll(UNDERSCORE, EMPTY_STRING);
                    break;
                // Other normalizations perhaps?
            }

            return elementName;
        }
    }

    /**
     * <p>
     * This class allows you to easily evaluate {@link JsonIngestFlattener} on your raw data, as configured via your ingest config. The displayed output
     * represents the very same flattened {@link Multimap} that would be parsed and presented to DataWave Ingest as input during the map phase of M/R job
     *
     * <p>
     * Usage... <blockquote> $ cd warehouse/ingest-json </blockquote> <blockquote> $ java -cp lib/*:target/datawave-ingest-json-version.jar
     * datawave.ingest.json.config.helper.JsonIngestFlattener\$Test -f test.json -c test-ingest-config.xml </blockquote>
     */
    public static final class Test {

        private File jsonInputFile = null;
        private Configuration jsonIngestConfig = null;
        private JsonObjectFlattener jsonIngestFlattener = null;
        private Iterator<JsonElement> jsonIterator;
        private TreeMultimap<String, String> sortedMap = TreeMultimap.create();
        private int objectCounter = 0;

        /**
         * <p>
         * The specified input file (--file arg) may contain one json object or many json objects concatenated together
         *
         * <p>
         * You must also specify at least one ingest config file (--config arg) for configuring a {@link JsonDataTypeHelper} instance, which will ultimately be
         * used to create and configure the {@link JsonIngestFlattener}. See {@link JsonDataTypeHelper#newFlattener()}. Config file paths should be
         * comma-delimited
         *
         * @param args --file(-f) /path/to/json/file; --config(-c) /path/to/ingest/config/file1,/path/to/ingest/config/file2,...; --help(-h,?)
         * @throws IOException on IO error
         */
        public static void main(String[] args) throws IOException {
            (new Test(args)).run();
        }

        private Test() {
        }

        private Test(String[] args) throws MalformedURLException {
            processArgs(args);
            configureFlattener();
        }

        private void run() throws IOException {

            JsonReader reader = new JsonReader(new FileReader(jsonInputFile));
            reader.setLenient(true);
            setupIterator(reader);

            try {
                while (true) {

                    if (!jsonIterator.hasNext()) {

                        if (reader.peek() == JsonToken.END_DOCUMENT) {
                            // If we're here, then we're done. No more objects left in the file
                            return;
                        }
                        setupIterator(reader);
                    }

                    if (jsonIterator.hasNext()) {
                        flattenAndPrint(jsonIterator.next().getAsJsonObject());
                    }
                }
            } finally {
                reader.close();
            }
        }

        private void flattenAndPrint(JsonObject jsonObject) {

            sortedMap.clear();
            jsonIngestFlattener.flatten(jsonObject, sortedMap);

            System.out.println("*********************** BEGIN JSON OBJECT #" + (++objectCounter) + " - FLATTEN MODE: " + jsonIngestFlattener.getFlattenMode()
                    + " ***********************");
            System.out.println();

            for (String key : sortedMap.keySet()) {
                System.out.print(key + ": ");
                Collection<String> values = sortedMap.get(key);
                for (String value : values) {
                    System.out.print("[" + value + "]");
                }
                System.out.println();
            }

            System.out.println();
            System.out.println("*********************** END JSON OBJECT #" + objectCounter + " - FLATTEN MODE: " + jsonIngestFlattener.getFlattenMode()
                    + " *************************");
            System.out.println();
        }

        private void setupIterator(JsonReader reader) {
            JsonParser parser = new JsonParser();
            JsonElement root = parser.parse(reader);

            if (root.isJsonArray()) {
                // Currently positioned to read a set of objects
                jsonIterator = root.getAsJsonArray().iterator();
            } else {
                // Currently positioned to read a single object
                jsonIterator = IteratorUtils.singletonIterator(root);
            }
        }

        private void processArgs(String[] args) throws MalformedURLException {

            if (args != null) {

                for (int i = 0; i < args.length; i++) {

                    if (args[i].equals("-f") || args[i].equals("--file")) {

                        Path p = Paths.get(args[++i]);
                        Preconditions.checkState(Files.exists(p), "Json input file does not exist: " + p);
                        jsonInputFile = p.toFile();

                    } else if (args[i].equals("-c") || args[i].equals("--config")) {

                        String[] configs = args[++i].split(",");
                        jsonIngestConfig = new Configuration();
                        for (String configFile : configs) {
                            Path p = Paths.get(configFile.trim());
                            Preconditions.checkState(Files.exists(p), "Config file does not exist: " + p);
                            jsonIngestConfig.addResource(p.toUri().toURL());
                        }

                    } else if (args[i].equals("-h") || args[i].equals("--help") || args[i].equals("?")) {

                        printUsage();
                        System.exit(0);

                    } else {

                        System.err.println("Unrecognized argument: " + args[i]);
                        printUsage();
                        System.exit(-1);
                    }
                }
            }

            Preconditions.checkState(jsonInputFile != null, "Json input file is required. Use the --help (-h,?) flag for usage");
            Preconditions.checkState(jsonIngestConfig != null, "At least one ingest config file is required. Use the --help (-h,?) flag for usage");
        }

        /**
         * Uses the specified ingest config(s) to configure a {@link JsonDataTypeHelper}, which in turn creates and configures the {@link JsonIngestFlattener}
         */
        private void configureFlattener() {

            // The configured IngestPolicyEnforcer class is employed later in the ingest processing flow and doesn't
            // affect the behavior of the flattener in any way. We care about it here only because the base DataTypeHelper
            // will complain during setup below if none is set. User's test config likely won't have one, as it's typically
            // configured in all-config.xml as a global setting for all data types...

            jsonIngestConfig.set("all" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS,
                    "datawave.policy.IngestPolicyEnforcer$NoOpIngestPolicyEnforcer");

            JsonDataTypeHelper helper = new JsonDataTypeHelper();
            helper.setup(jsonIngestConfig);

            jsonIngestFlattener = helper.newFlattener();
        }

        private void printUsage() {
            System.out.println("Usage:");
            System.out.println("   " + getClass().getName()
                    + " --file (-f) /path/to/input/json/file --config (-c) /path/to/ingest/config/file1,...,/path/to/ingest/config/fileN");
            System.out.println("Optional args:");
            System.out.println("   --help (-h, ?)");
        }
    }
}
