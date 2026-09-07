package datawave.query.iterator;

import static datawave.query.iterator.QueryOptions.DOC_AGGREGATION_THRESHOLD_MS;
import static datawave.query.iterator.QueryOptions.EVENT_FIELD_SEEK;
import static datawave.query.iterator.QueryOptions.EVENT_NEXT_SEEK;
import static datawave.query.iterator.QueryOptions.FI_FIELD_SEEK;
import static datawave.query.iterator.QueryOptions.FI_NEXT_SEEK;
import static datawave.query.iterator.QueryOptions.QUERY;
import static datawave.query.iterator.QueryOptions.SEEKING_EVENT_AGGREGATION;
import static datawave.query.iterator.QueryOptions.TERM_FREQUENCY_AGGREGATION_THRESHOLD_MS;
import static datawave.query.iterator.QueryOptions.TF_FIELD_SEEK;
import static datawave.query.iterator.QueryOptions.TF_NEXT_SEEK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import datawave.query.Constants;
import datawave.query.function.Equality;
import datawave.query.function.PrefixEquality;
import datawave.query.iterator.filter.EntryKeyIdentity;
import datawave.query.iterator.filter.FieldIndexKeyDataTypeFilter;
import datawave.query.util.TypeMetadata;
import datawave.query.util.TypeMetadataSerializer;

public class QueryOptionsTest {

    @BeforeClass
    public static void setupClass() {
        Logger.getLogger(QueryOptions.class).setLevel(Level.TRACE);
    }

    @Test
    public void testBuildFieldDataTypeMapFromSingleValueString() {
        String singleValueData = "k:v;";
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        expectedDataTypeMap.put("k", Sets.newHashSet("v"));
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(singleValueData);
        assertEquals("Failed to parse single value option string", expectedDataTypeMap, fieldDataTypeMap);
    }

    @Test
    public void testBuildFieldDataTypeMapFromMultiValueString() {
        String multiValueData = "k:v;key:value";
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        expectedDataTypeMap.put("k", Sets.newHashSet("v"));
        expectedDataTypeMap.put("key", Sets.newHashSet("value"));
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(multiValueData);
        assertEquals("Failed to parse multi-value option string", expectedDataTypeMap, fieldDataTypeMap);
    }

    @Test
    public void testBuildFieldDataTypeMapFromEmptyString() {
        String emptyData = "";
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(emptyData);
        assertEquals("Failed to parse empty option string", expectedDataTypeMap, fieldDataTypeMap);
    }

    @Test
    public void testBuildFieldDataTypeMapFromNullString() {
        String nulldata = null;
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(nulldata);
        assertEquals("Failed to parse null option string", expectedDataTypeMap, fieldDataTypeMap);
    }

    @Test
    public void testBuildFieldDataTypeMapFromBadString() {
        String badData = "k:k2:k3:v;";
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(badData);
        assertEquals("Failed to parse bad option string", expectedDataTypeMap, fieldDataTypeMap);
    }

    @Test
    public void testFetchDataTypeKeysFromSingleValueString() {
        String data = "k:v;";
        Set<String> expectedDataTypeKeys = Sets.newHashSet("k");
        Set<String> dataTypeKeys = QueryOptions.fetchDataTypeKeys(data);
        assertEquals("Failed to parse single value option string", expectedDataTypeKeys, dataTypeKeys);
    }

    @Test
    public void testFetchDataTypeKeysFromMultiValueString() {
        String data = "k:v;key:value";
        Set<String> expectedDataTypeKeys = Sets.newHashSet("k", "key");
        Set<String> dataTypeKeys = QueryOptions.fetchDataTypeKeys(data);
        assertEquals("Failed to parse multi-value option string", expectedDataTypeKeys, dataTypeKeys);
    }

    @Test
    public void testFetchDataTypeKeysFromEmptyString() {
        String data = "";
        Set<String> expectedDataTypeKeys = Sets.newHashSet();
        Set<String> dataTypeKeys = QueryOptions.fetchDataTypeKeys(data);
        assertEquals("Failed to parse empty option string", expectedDataTypeKeys, dataTypeKeys);
    }

    @Test
    public void testFetchDataTypeKeysFromNullString() {
        String data = null;
        Set<String> expectedDataTypeKeys = Sets.newHashSet();
        Set<String> dataTypeKeys = QueryOptions.fetchDataTypeKeys(data);
        assertEquals("Failed to parse null option string", expectedDataTypeKeys, dataTypeKeys);
    }

    @Test
    public void testSeekingConfiguration() {
        Map<String,String> optionsMap = new HashMap<>();
        optionsMap.put(QUERY, "set to avoid early return");
        optionsMap.put(FI_FIELD_SEEK, "10");
        optionsMap.put(FI_NEXT_SEEK, "11");
        optionsMap.put(EVENT_FIELD_SEEK, "12");
        optionsMap.put(EVENT_NEXT_SEEK, "13");
        optionsMap.put(TF_FIELD_SEEK, "14");
        optionsMap.put(TF_NEXT_SEEK, "15");
        optionsMap.put(SEEKING_EVENT_AGGREGATION, "true");

        QueryOptions options = new QueryOptions();

        // initial state
        assertEquals(-1, options.getFiFieldSeek());
        assertEquals(-1, options.getFiNextSeek());
        assertEquals(-1, options.getEventFieldSeek());
        assertEquals(-1, options.getEventNextSeek());
        assertEquals(-1, options.getTfFieldSeek());
        assertEquals(-1, options.getTfNextSeek());
        assertFalse(options.isSeekingEventAggregation());

        options.validateOptions(optionsMap);

        // expected state
        assertEquals(10, options.getFiFieldSeek());
        assertEquals(11, options.getFiNextSeek());
        assertEquals(12, options.getEventFieldSeek());
        assertEquals(13, options.getEventNextSeek());
        assertEquals(14, options.getTfFieldSeek());
        assertEquals(15, options.getTfNextSeek());
        assertTrue(options.isSeekingEventAggregation());
    }

    @Test
    public void testDocumentAndTermOffsetAggregationThresholds() {
        Map<String,String> optionsMap = new HashMap<>();
        optionsMap.put(QUERY, "query option required to validate");
        optionsMap.put(DOC_AGGREGATION_THRESHOLD_MS, "15000");
        optionsMap.put(TERM_FREQUENCY_AGGREGATION_THRESHOLD_MS, "10000");

        QueryOptions options = new QueryOptions();

        // initial state
        assertEquals(-1, options.getDocAggregationThresholdMs());
        assertEquals(-1, options.getTfAggregationThresholdMs());

        options.validateOptions(optionsMap);

        // expected state
        assertEquals(15000, options.getDocAggregationThresholdMs());
        assertEquals(10000, options.getTfAggregationThresholdMs());
    }

    @Test
    public void testGetEquality() {
        QueryOptions options = new QueryOptions();
        Equality equality = options.getEquality();
        assertEquals(PrefixEquality.class.getSimpleName(), equality.getClass().getSimpleName());
    }

    @Test
    public void testSimple() {
        Map<String,Set<String>> expected = new HashMap<>();

        expected.put("FIELD1", new HashSet<>(Arrays.asList("norm1", "norm2")));
        expected.put("FIELD2", new HashSet<>(Arrays.asList("norm1")));
        expected.put("FIELD3", new HashSet<>(Arrays.asList("norm1")));
        expected.put("FIELD4", new HashSet<>(Arrays.asList("norm1")));

        String data = QueryOptions.buildFieldNormalizerString(expected);
        Map<String,Set<String>> actual = QueryOptions.buildFieldDataTypeMap(data);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSingleField() {
        Map<String,Set<String>> expected = new HashMap<>();

        expected.put("FIELD1", new HashSet<>(Arrays.asList("norm1")));

        String data = QueryOptions.buildFieldNormalizerString(expected);
        Map<String,Set<String>> actual = QueryOptions.buildFieldDataTypeMap(data);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testEmptyMap() {
        Map<String,Set<String>> expected = new HashMap<>();

        String data = QueryOptions.buildFieldNormalizerString(expected);
        Map<String,Set<String>> actual = QueryOptions.buildFieldDataTypeMap(data);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNoDataTypeFilter() {
        Map<String,String> noOpts = Collections.emptyMap();
        QueryOptions opts = new QueryOptions();
        opts.validateOptions(noOpts);
        Assert.assertSame(EntryKeyIdentity.Function, opts.getFieldIndexKeyDataTypeFilter());
    }

    @Test
    public void testDataTypeFilter() {
        Map<String,String> opts = Maps.newHashMap();
        opts.put(QueryOptions.DATATYPE_FILTER, "foo,bar,meow");
        opts.put(QueryOptions.DISABLE_EVALUATION, "true");
        QueryOptions qopts = new QueryOptions();
        qopts.validateOptions(opts);
        Assert.assertTrue(qopts.getFieldIndexKeyDataTypeFilter() instanceof FieldIndexKeyDataTypeFilter);
    }

    @Test
    public void testTypeMetadataFromNativeStringFormat() {
        TypeMetadata expected = new TypeMetadata();
        expected.put("FIELD1", "ingestA", "LcNoDiacriticsType");
        expected.put("FIELD2", "ingestA", "NumberType");

        Map<String,String> optionsMap = new HashMap<>();
        optionsMap.put(QUERY, "set to avoid early return");
        optionsMap.put(QueryOptions.TYPE_METADATA, expected.toString());

        QueryOptions options = new QueryOptions();
        options.validateOptions(optionsMap);

        assertEquals(expected, options.getTypeMetadata());
    }

    @Test
    public void testTypeMetadataFromKryoFormat() {
        TypeMetadata expected = new TypeMetadata();
        expected.put("FIELD1", "ingestA", "LcNoDiacriticsType");
        expected.put("FIELD2", "ingestA", "NumberType");

        Map<String,String> optionsMap = new HashMap<>();
        optionsMap.put(QUERY, "set to avoid early return");
        optionsMap.put(QueryOptions.TYPE_METADATA, new TypeMetadataSerializer().serialize(expected));
        optionsMap.put(QueryOptions.TYPE_METADATA_KRYO, "true");

        QueryOptions options = new QueryOptions();
        options.validateOptions(optionsMap);

        assertEquals(expected, options.getTypeMetadata());
    }

    @Test
    public void testLargeOptionsBuffer() throws IOException {
        Random rand = new Random(System.currentTimeMillis());
        int maxSize = 2522222;
        byte[] buffer = new byte[maxSize];
        for (int i = 0; i < maxSize; i++) {
            buffer[i] = (byte) (rand.nextInt(25) + 97);
        }
        String initialString = new String(buffer);
        String compressedOptions = QueryOptions.compressOption(initialString, QueryOptions.UTF8);
        String decompressOptions = WrappedQueryOptions.decompressOption(compressedOptions, QueryOptions.UTF8);

        Assert.assertEquals(initialString.length(), decompressOptions.length());
        Assert.assertEquals(initialString, decompressOptions);
    }

    private static class WrappedQueryOptions extends QueryOptions {
        protected static String decompressOption(final String buffer, Charset characterSet) throws IOException {
            return QueryOptions.decompressOption(buffer, characterSet);
        }
    }

    @Test
    public void testDefaultOptions() {

        IteratorSetting iteratorSetting = new IteratorSetting(1, "foo", QueryIterator.class, new HashMap<>());
        Map<String,String> nonDefaults = new HashMap<>();
        nonDefaults.put(QueryOptions.INCLUDE_RECORD_ID, "false");
        nonDefaults.put(QueryOptions.INCLUDE_DATATYPE, "true");

        // Test all default
        QueryOptions.getDefaultOptions(QueryIterator.class.getName()).getDefaultValues().forEach((k, v) -> {
            QueryOptions.addOption(iteratorSetting, k, v, false);
        });

        Assert.assertEquals(0, iteratorSetting.getOptions().size());

        // Test non-defaults added after all defaults added
        iteratorSetting.clearOptions();
        QueryOptions.getDefaultOptions(QueryIterator.class.getName()).getDefaultValues().forEach((k, v) -> {
            QueryOptions.addOption(iteratorSetting, k, v, false);
        });

        iteratorSetting.addOptions(nonDefaults);
        assertEquals(2, iteratorSetting.getOptions().size());

        // Test defaults added after non-defaults added
        iteratorSetting.clearOptions();
        iteratorSetting.addOptions(nonDefaults);
        QueryOptions.getDefaultOptions(QueryIterator.class.getName()).getDefaultValues().forEach((k, v) -> {
            QueryOptions.addOption(iteratorSetting, k, v, false);
        });

        assertEquals(0, iteratorSetting.getOptions().size());

    }

    @Test
    public void testCollectionAsValueEquivalence() {

        IteratorSetting iteratorSetting = new IteratorSetting(1, "foo", QueryIterator.class, new HashMap<>());

        HashMap<String,Integer> limitFields = new HashMap<>();
        limitFields.put("field1", 1);
        limitFields.put("field2", 2);

        QueryOptions.addOption(iteratorSetting, QueryOptions.LIMIT_FIELDS, limitFields, false);

        Assert.assertTrue(QueryOptions.getDefaultOptions(QueryOptionsWithNonEmptyCollectionsAsDefaultValues.class.getName())
                        .equalsDefaultValue(QueryOptions.LIMIT_FIELDS, limitFields));

    }

    //@formatter:off
    private static final Map<String, String> propertiesToOptions = new ImmutableMap.Builder<String, String>()
            .put("disableEvaluation", QueryOptions.DISABLE_EVALUATION)
            .put("disableFiEval", QueryOptions.DISABLE_FIELD_INDEX_EVAL)
            .put("sourceLimit", QueryOptions.LIMIT_SOURCES)
            .put("disableIndexOnlyDocuments", QueryOptions.DISABLE_DOCUMENTS_WITHOUT_EVENTS)
            .put("query", QueryOptions.QUERY)
            .put("queryId", QueryOptions.QUERY_ID)
            .put("scanId", QueryOptions.SCAN_ID)
            .put("compressedMappings", QueryOptions.QUERY_MAPPING_COMPRESS)
            .put("compositeMetadata", QueryOptions.COMPOSITE_METADATA)
            .put("compositeSeekThreshold", QueryOptions.COMPOSITE_SEEK_THRESHOLD)
            .put("returnType", Constants.RETURN_TYPE)
            .put("reducedResponse", QueryOptions.REDUCED_RESPONSE)
            .put("fullTableScanOnly", QueryOptions.FULL_TABLE_SCAN_ONLY)
            .put("trackSizes", QueryOptions.TRACK_SIZES)
            .put("allowListedFields", QueryOptions.PROJECTION_FIELDS)
            .put("disallowListedFields", QueryOptions.DISALLOWLISTED_FIELDS)
            .put("fieldCounts", QueryOptions.FIELD_COUNTS)
            .put("termCounts", QueryOptions.TERM_COUNTS)
            .put("filterMaskedValues", QueryOptions.FILTER_MASKED_VALUES)
            .put("includeDatatype", QueryOptions.INCLUDE_DATATYPE)
            .put("includeRecordId", QueryOptions.INCLUDE_RECORD_ID)
            .put("collectTimingDetails", QueryOptions.COLLECT_TIMING_DETAILS)
            .put("statsdHostAndPort", QueryOptions.STATSD_HOST_COLON_PORT)
            .put("statsdMaxQueueSize", QueryOptions.STATSD_MAX_QUEUE_SIZE)
            .put("includeHierarchyFields", QueryOptions.INCLUDE_HIERARCHY_FIELDS)
            .put("fiFieldSeek", QueryOptions.FI_FIELD_SEEK)
            .put("fiNextSeek", QueryOptions.FI_NEXT_SEEK)
            .put("eventFieldSeek", QueryOptions.EVENT_FIELD_SEEK)
            .put("eventNextSeek", QueryOptions.EVENT_NEXT_SEEK)
            .put("tfFieldSeek", QueryOptions.TF_FIELD_SEEK)
            .put("tfNextSeek", QueryOptions.TF_NEXT_SEEK)
            .put("docAggregationThresholdMs", QueryOptions.DOC_AGGREGATION_THRESHOLD_MS)
            .put("tfAggregationThresholdMs", QueryOptions.TERM_FREQUENCY_AGGREGATION_THRESHOLD_MS)
            .put("fieldIndexKeyDataTypeFilter", QueryOptions.DATATYPE_FILTER)
            .put("indexOnlyFields", QueryOptions.INDEX_ONLY_FIELDS)
            .put("indexedFields", QueryOptions.INDEXED_FIELDS)
            .put("ignoreColumnFamilies", QueryOptions.IGNORE_COLUMN_FAMILIES)
            .put("startTime", QueryOptions.START_TIME)
            .put("endTime", QueryOptions.END_TIME)
            .put("includeGroupingContext", QueryOptions.INCLUDE_GROUPING_CONTEXT)
            .put("documentPermutationClasses", QueryOptions.DOCUMENT_PERMUTATION_CLASSES)
            .put("limitFieldsMap", QueryOptions.LIMIT_FIELDS)
            .put("matchingFieldSets", QueryOptions.MATCHING_FIELD_SETS)
            .put("limitFieldsPreQueryEvaluation", QueryOptions.LIMIT_FIELDS_PRE_QUERY_EVALUATION)
            .put("limitFieldsField", QueryOptions.LIMIT_FIELDS_FIELD)
            .put("groupFields", QueryOptions.GROUP_FIELDS)
            .put("groupFieldsBatchSize", QueryOptions.GROUP_FIELDS_BATCH_SIZE)
            .put("uniqueFields", QueryOptions.UNIQUE_FIELDS)
            .put("arithmetic", QueryOptions.HIT_LIST)
            .put("dateIndexTimeTravel", QueryOptions.DATE_INDEX_TIME_TRAVEL)
            .put("postProcessingFunctions", QueryOptions.POSTPROCESSING_CLASSES)
            .put("nonIndexedDataTypeMap", QueryOptions.NON_INDEXED_DATATYPES)
            .put("containsIndexOnlyTerms", QueryOptions.CONTAINS_INDEX_ONLY_TERMS)
            .put("allowFieldIndexEvaluation", QueryOptions.ALLOW_FIELD_INDEX_EVALUATION)
            .put("allowTermFrequencyLookup", QueryOptions.ALLOW_TERM_FREQUENCY_LOOKUP)
            .put("hdfsSiteConfigURLs", QueryOptions.HDFS_SITE_CONFIG_URLS)
            .put("hdfsFileCompressionCodec", QueryOptions.HDFS_FILE_COMPRESSION_CODEC)
            .put("zookeeperConfig", QueryOptions.ZOOKEEPER_CONFIG)
            .put("ivaratorCacheDirConfigs", QueryOptions.IVARATOR_CACHE_DIR_CONFIG)
            .put("ivaratorCacheBufferSize", QueryOptions.IVARATOR_CACHE_BUFFER_SIZE)
            .put("ivaratorCacheScanPersistThreshold", QueryOptions.IVARATOR_SCAN_PERSIST_THRESHOLD)
            .put("ivaratorCacheScanTimeout", QueryOptions.IVARATOR_SCAN_TIMEOUT)
            .put("resultTimeout", QueryOptions.RESULT_TIMEOUT)
            .put("maxIndexRangeSplit", QueryOptions.MAX_INDEX_RANGE_SPLIT)
            .put("ivaratorMaxOpenFiles", QueryOptions.MAX_IVARATOR_OPEN_FILES)
            .put("ivaratorNumRetries", QueryOptions.IVARATOR_NUM_RETRIES)
            .put("ivaratorPersistVerify", QueryOptions.IVARATOR_PERSIST_VERIFY)
            .put("ivaratorPersistVerifyCount", QueryOptions.IVARATOR_PERSIST_VERIFY_COUNT)
            .put("maxIvaratorSources", QueryOptions.MAX_IVARATOR_SOURCES)
            .put("maxIvaratorSourceWait", QueryOptions.MAX_IVARATOR_SOURCE_WAIT)
            .put("maxIvaratorResults", QueryOptions.MAX_IVARATOR_RESULTS)
            .put("yieldThresholdMs", QueryOptions.YIELD_THRESHOLD_MS)
            .put("compressResults", QueryOptions.COMPRESS_SERVER_SIDE_RESULTS)
            .put("maxEvaluationPipelines", QueryOptions.MAX_EVALUATION_PIPELINES)
            .put("serialEvaluationPipeline", QueryOptions.SERIAL_EVALUATION_PIPELINE)
            .put("maxPipelineCachedResults", QueryOptions.MAX_PIPELINE_CACHED_RESULTS)
            .put("termFrequenciesRequired", QueryOptions.TERM_FREQUENCIES_REQUIRED)
            .put("sortedUIDs", QueryOptions.SORTED_UIDS)
            .put("debugMultithreadedSources", QueryOptions.DEBUG_MULTITHREADED_SOURCES)
            .put("activeQueryLogName", QueryOptions.ACTIVE_QUERY_LOG_NAME)
            .put("excerptFields", QueryOptions.EXCERPT_FIELDS)
            .put("excerptFieldsNoHitCallout", QueryOptions.EXCERPT_FIELDS_NO_HIT_CALLOUT)
            .put("excerptIterator", QueryOptions.EXCERPT_ITERATOR)
            .put("seekingEventAggregation", QueryOptions.SEEKING_EVENT_AGGREGATION)
            .put("typeMetadata", QueryOptions.TYPE_METADATA)
            .put("typeMetadataAuths", QueryOptions.TYPE_METADATA_AUTHS)
            .put("metadataTableName", QueryOptions.METADATA_TABLE_NAME)
            .put("compositeFields", QueryOptions.COMPOSITE_FIELDS)
            .put("containsCompositeTerms", QueryOptions.CONTAINS_COMPOSITE_TERMS)
            .put("termFrequencyFields", QueryOptions.TERM_FREQUENCY_FIELDS)
            .put("contentExpansionFields", QueryOptions.CONTENT_EXPANSION_FIELDS)
            .put("mostRecentUnique", QueryOptions.MOST_RECENT_UNIQUE)
            .put("uniqueCacheBufferSize", QueryOptions.UNIQUE_CACHE_BUFFER_SIZE)
            .put("hitsOnly", QueryOptions.HITS_ONLY)
            .put("maxYields", QueryOptions.MAX_YIELDS)
            .put("logTimingDetails", QueryOptions.LOG_TIMING_DETAILS)
            .put("datatypeFieldname", QueryOptions.DATATYPE_FIELDNAME)
            .put("defaultDatatypeFieldname", QueryOptions.DEFAULT_DATATYPE_FIELDNAME)
            .put("defaultParentUidFieldname", QueryOptions.DEFAULT_PARENT_UID_FIELDNAME)
            .put("defaultChildCountFieldname", QueryOptions.DEFAULT_CHILD_COUNT_FIELDNAME)
            .put("defaultDescendantCountFieldname", QueryOptions.DEFAULT_DESCENDANT_COUNT_FIELDNAME)
            .put("defaultHasChildrenFieldname", QueryOptions.DEFAULT_HAS_CHILDREN_FIELDNAME)
            .put("childCountOutputImmediateChildren", QueryOptions.CHILD_COUNT_OUTPUT_IMMEDIATE_CHILDREN)
            .put("childCountOutputAllDescendants", QueryOptions.CHILD_COUNT_OUTPUT_ALL_DESCDENDANTS)
            .put("childCountOutputHasChildren", QueryOptions.CHILD_COUNT_OUTPUT_HASCHILDREN)
            .put("childCountIndexDelimiter", QueryOptions.CHILD_COUNT_INDEX_DELIMITER)
            .put("childCountIndexFieldname", QueryOptions.CHILD_COUNT_INDEX_FIELDNAME)
            .put("childCountIndexPattern", QueryOptions.CHILD_COUNT_INDEX_PATTERN)
            .put("childCountIndexSkipThreshold", QueryOptions.CHILD_COUNT_INDEX_SKIP_THRESHOLD)
            .put("postProcessingOptions", QueryOptions.POSTPROCESSING_OPTIONS)
            .put("ranges", QueryOptions.RANGES)
            .put("summaryOptions", QueryOptions.SUMMARY_OPTIONS)
            .put("summaryIterator", QueryOptions.SUMMARY_ITERATOR)
            .put("summaryFieldName", QueryOptions.SUMMARY_FIELD_NAME)
            .put("cardinalityThreshold", QueryOptions.CARDINALITY_THRESHOLD)
            .build();
    //@formatter:on

    @Test
    public void testAllOptionsHaveDefaultOptionSpecified() throws JsonProcessingException {
        ObjectMapper mapper = JsonMapper.builder().build();
        mapper.addMixIn(QueryOptions.class, QueryOptionsMixin.class);
        ObjectWriter writer = mapper.writer().withoutAttribute("script");
        JsonNode node = mapper.readTree(writer.writeValueAsString(new QueryOptions()));
        Set<String> properties = new HashSet<>();
        for (Iterator<String> it = node.fieldNames(); it.hasNext();) {
            properties.add(it.next());
        }

        QueryOptions.DefaultOptions defaultOptions = QueryOptions.getDefaultOptions(QueryOptions.class.getName());
        Set<String> optionsWithDefaults = defaultOptions.getDefaultValues().keySet();

        Set<String> unmappedProperties = new HashSet<>();
        Set<String> optionsMissingDefault = new HashSet<>();
        for (String field : properties) {
            String option = propertiesToOptions.get(field);
            if (option == null) {
                unmappedProperties.add(field);
            } else {
                if (!optionsWithDefaults.contains(option)) {
                    optionsMissingDefault.add(option);
                }
            }
        }

        if (!optionsMissingDefault.isEmpty()) {
            Assert.fail("The following options do not have default values configured: " + optionsMissingDefault);
        }

        if (!unmappedProperties.isEmpty()) {
            Assert.fail("The following properties do not have a mapped option. Add them to the propertiesToOptions map, or mark with @JsonIgnore: "
                            + unmappedProperties);
        }
    }

    public static class QueryOptionsWithNonEmptyCollectionsAsDefaultValues extends QueryOptions {

        @Override
        protected DefaultOptions createDefaultOptions() {

            HashMap<String,Integer> limitFields = new HashMap<>();
            limitFields.put("field1", 1);
            limitFields.put("field2", 2);

            // formatter:off
            DefaultOptions defaultOptions = DefaultOptions.builder()
                            .putDefaultValues(QueryOptions.getDefaultOptions(QueryOptions.class.getName()).getDefaultValues())
                            .putDefaultValue(QueryOptions.LIMIT_FIELDS, limitFields).build();
            // formatter:on

            return defaultOptions;
        }
    }
}
