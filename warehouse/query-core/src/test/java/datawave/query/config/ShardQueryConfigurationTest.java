package datawave.query.config;

import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.data.type.DateType;
import datawave.data.type.GeometryType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.ExcerptFields;
import datawave.query.attributes.UniqueFields;
import datawave.query.common.grouping.GroupFields;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.iterator.logic.TermFrequencyExcerptIterator;
import datawave.query.iterator.logic.TermFrequencyIndexIterator;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.model.QueryModel;
import datawave.query.planner.scanhints.IvaratorScanHint;
import datawave.util.TableName;

public class ShardQueryConfigurationTest {

    public final static Map<Class<?>,Class<?>> primitiveMap = new HashMap<>();
    static {
        primitiveMap.put(Boolean.class, boolean.class);
        primitiveMap.put(Byte.class, byte.class);
        primitiveMap.put(Short.class, short.class);
        primitiveMap.put(Character.class, char.class);
        primitiveMap.put(Integer.class, int.class);
        primitiveMap.put(Long.class, long.class);
        primitiveMap.put(Float.class, float.class);
        primitiveMap.put(Double.class, double.class);
    }

    // The set of default values.
    private final Map<String,Object> defaultValues = new HashMap<>();

    // The set of predicates for the subset of defaultValues that will
    // be used to evaluate the equality instead of .equals(Object).
    @SuppressWarnings("rawtypes")
    private final Map<String,Predicate> defaultPredicates = new HashMap<>();

    // The set of alternate values to test the setters/getters
    private final Map<String,Object> updatedValues = new HashMap<>();

    // These are fields that are NOT retrieved via the jackson library (e.g. JsonIgnore annotation)
    // but need to be set to test something else (e.g. queryTree vs queryString)
    private final Map<String,Object> extraValuesToSet = new HashMap<>();

    // The set of predicate for the subset of alternateValues that will
    // be used to evaluate the equality instead of .equals(Object).
    @SuppressWarnings("rawtypes")
    private final Map<String,Predicate> updatedPredicates = new HashMap<>();

    // The set of fields that are already set via one of the other fields.
    private final Set<String> alreadySet = new HashSet<>();

    @Before
    public void setUp() throws Exception {
        // The set of default values (optionally as predicates,
        // alternate values (to test the setters/getters),
        // and optional alternate predicates for testing equality.
        defaultValues.put("checkpointable", false);
        updatedValues.put("checkpointable", true);

        defaultValues.put("auths", Sets.newHashSet());
        updatedValues.put("auths", Sets.newHashSet("FOO", "BAR"));

        defaultValues.put("queries", Collections.emptyList());
        updatedValues.put("queries", Lists.newArrayList(new QueryImpl()));

        defaultValues.put("bloom", null);
        updatedValues.put("bloom", null);
        alreadySet.add("bloom");

        defaultValues.put("activeQueryLogName", "");
        updatedValues.put("activeQueryLogName", "ShardQueryConfiguration");
        alreadySet.add("activeQueryLogName");

        defaultValues.put("limitTermExpansionToModel", false);
        updatedValues.put("limitTermExpansionToModel", true);

        defaultValues.put("shardDateFormat", "yyyyMMdd");
        updatedValues.put("shardDateFormat", "yyyyMMddHHmmss");

        defaultValues.put("authorizations", Collections.singleton(Authorizations.EMPTY));
        updatedValues.put("authorizations", Collections.singleton(new Authorizations("FOO", "BAR")));

        defaultValues.put("queryString", null);
        updatedValues.put("queryString", "A == B");
        alreadySet.add("queryString"); // set by queryTree
        extraValuesToSet.put("queryTree", JexlASTHelper.parseAndFlattenJexlQuery("A == B"));

        defaultValues.put("beginDate", null);
        updatedValues.put("beginDate", new Date());
        defaultValues.put("endDate", null);
        updatedValues.put("endDate", new Date());
        defaultValues.put("maxWork", -1L);
        updatedValues.put("maxWork", 1000L);
        defaultValues.put("baseIteratorPriority", 100);
        updatedValues.put("baseIteratorPriority", 14);
        defaultValues.put("tableName", TableName.SHARD);
        updatedValues.put("tableName", "datawave." + TableName.SHARD);
        defaultValues.put("bypassAccumulo", false);
        updatedValues.put("bypassAccumulo", true);
        defaultValues.put("accumuloPassword", "");
        updatedValues.put("accumuloPassword", "secret");
        defaultValues.put("connPoolName", null);
        updatedValues.put("connPoolName", "default");
        defaultValues.put("reduceResults", false);
        updatedValues.put("reduceResults", true);
        defaultValues.put("tldQuery", false);
        updatedValues.put("tldQuery", true);
        defaultValues.put("filterOptions", Maps.newHashMap());
        updatedValues.put("filterOptions", Collections.singletonMap("FIELD_A", "FILTER"));
        defaultValues.put("disableIndexOnlyDocuments", false);
        updatedValues.put("disableIndexOnlyDocuments", true);
        defaultValues.put("maxScannerBatchSize", 1000);
        updatedValues.put("maxScannerBatchSize", 1300);
        defaultValues.put("maxIndexBatchSize", 1000);
        updatedValues.put("maxIndexBatchSize", 1100);
        defaultValues.put("allTermsIndexOnly", false);
        updatedValues.put("allTermsIndexOnly", true);
        defaultValues.put("maxIndexScanTimeMillis", Long.MAX_VALUE);
        updatedValues.put("maxIndexScanTimeMillis", 100000L);
        defaultValues.put("maxAnyFieldScanTimeMillis", Long.MAX_VALUE);
        updatedValues.put("maxAnyFieldScanTimeMillis", 100000L);
        defaultValues.put("parseTldUids", false);
        updatedValues.put("parseTldUids", true);
        defaultValues.put("ignoreNonExistentFields", false);
        updatedValues.put("ignoreNonExistentFields", true);
        defaultValues.put("collapseUids", false);
        updatedValues.put("collapseUids", true);
        defaultValues.put("collapseUidsThreshold", -1);
        updatedValues.put("collapseUidsThreshold", 400);
        defaultValues.put("enforceUniqueTermsWithinExpressions", false);
        updatedValues.put("enforceUniqueTermsWithinExpressions", true);
        defaultValues.put("reduceQueryFields", false);
        updatedValues.put("reduceQueryFields", true);
        defaultValues.put("reduceQueryFieldsPerShard", false);
        updatedValues.put("reduceQueryFieldsPerShard", true);
        defaultValues.put("rebuildDatatypeFilter", false);
        updatedValues.put("rebuildDatatypeFilter", true);
        defaultValues.put("rebuildDatatypeFilterPerShard", false);
        updatedValues.put("rebuildDatatypeFilterPerShard", true);
        defaultValues.put("reduceTypeMetadata", false);
        updatedValues.put("reduceTypeMetadata", true);
        defaultValues.put("reduceTypeMetadataPerShard", false);
        updatedValues.put("reduceTypeMetadataPerShard", true);
        defaultValues.put("sequentialScheduler", false);
        updatedValues.put("sequentialScheduler", true);
        defaultValues.put("collectTimingDetails", false);
        updatedValues.put("collectTimingDetails", true);
        defaultValues.put("logTimingDetails", false);
        updatedValues.put("logTimingDetails", true);
        defaultValues.put("sendTimingToStatsd", true);
        updatedValues.put("sendTimingToStatsd", false);
        defaultValues.put("statsdHost", "localhost");
        updatedValues.put("statsdHost", "10.0.0.1");
        defaultValues.put("statsdPort", 8125);
        updatedValues.put("statsdPort", 8126);
        defaultValues.put("statsdMaxQueueSize", 500);
        updatedValues.put("statsdMaxQueueSize", 530);
        defaultValues.put("limitAnyFieldLookups", false);
        updatedValues.put("limitAnyFieldLookups", true);
        defaultValues.put("bypassExecutabilityCheck", false);
        updatedValues.put("bypassExecutabilityCheck", true);
        defaultValues.put("generatePlanOnly", false);
        updatedValues.put("generatePlanOnly", true);
        defaultValues.put("backoffEnabled", false);
        updatedValues.put("backoffEnabled", true);
        defaultValues.put("unsortedUIDsEnabled", true);
        updatedValues.put("unsortedUIDsEnabled", false);
        defaultValues.put("serializeQueryIterator", false);
        updatedValues.put("serializeQueryIterator", true);
        defaultValues.put("debugMultithreadedSources", false);
        updatedValues.put("debugMultithreadedSources", true);
        defaultValues.put("sortGeoWaveQueryRanges", false);
        updatedValues.put("sortGeoWaveQueryRanges", true);
        defaultValues.put("numRangesToBuffer", 0);
        updatedValues.put("numRangesToBuffer", 4);
        defaultValues.put("rangeBufferTimeoutMillis", 0L);
        updatedValues.put("rangeBufferTimeoutMillis", 1000L);
        defaultValues.put("rangeBufferPollMillis", 100L);
        updatedValues.put("rangeBufferPollMillis", 140L);
        defaultValues.put("geometryMaxExpansion", 8);
        updatedValues.put("geometryMaxExpansion", 4);
        defaultValues.put("pointMaxExpansion", 32);
        updatedValues.put("pointMaxExpansion", 36);
        defaultValues.put("geoMaxExpansion", 32);
        updatedValues.put("geoMaxExpansion", 39);
        defaultValues.put("geoWaveRangeSplitThreshold", 16);
        updatedValues.put("geoWaveRangeSplitThreshold", 14);
        defaultValues.put("geoWaveMaxRangeOverlap", 0.25);
        updatedValues.put("geoWaveMaxRangeOverlap", 0.35);
        defaultValues.put("optimizeGeoWaveRanges", true);
        updatedValues.put("optimizeGeoWaveRanges", false);
        defaultValues.put("geoWaveMaxEnvelopes", 4);
        updatedValues.put("geoWaveMaxEnvelopes", 40);
        defaultValues.put("shardTableName", TableName.SHARD);
        updatedValues.put("shardTableName", "datawave." + TableName.SHARD);
        defaultValues.put("indexTableName", TableName.SHARD_INDEX);
        updatedValues.put("indexTableName", "datawave." + TableName.SHARD_INDEX);
        defaultValues.put("reverseIndexTableName", TableName.SHARD_RINDEX);
        updatedValues.put("reverseIndexTableName", "datawave." + TableName.SHARD_RINDEX);
        defaultValues.put("metadataTableName", TableName.METADATA);
        updatedValues.put("metadataTableName", "datawave." + TableName.METADATA);
        defaultValues.put("dateIndexTableName", TableName.DATE_INDEX);
        updatedValues.put("dateIndexTableName", "datawave." + TableName.DATE_INDEX);
        defaultValues.put("indexStatsTableName", TableName.INDEX_STATS);
        updatedValues.put("indexStatsTableName", "datawave." + TableName.INDEX_STATS);
        defaultValues.put("defaultDateTypeName", "EVENT");
        updatedValues.put("defaultDateTypeName", "DOCUMENT");
        defaultValues.put("cleanupShardsAndDaysQueryHints", true);
        updatedValues.put("cleanupShardsAndDaysQueryHints", false);
        defaultValues.put("numQueryThreads", 8);
        updatedValues.put("numQueryThreads", 80);
        defaultValues.put("numDateIndexThreads", 8);
        updatedValues.put("numDateIndexThreads", 20);
        defaultValues.put("maxDocScanTimeout", -1);
        updatedValues.put("maxDocScanTimeout", 10000);
        defaultValues.put("collapseDatePercentThreshold", 0.99f);
        updatedValues.put("collapseDatePercentThreshold", 0.92f);
        defaultValues.put("fullTableScanEnabled", true);
        updatedValues.put("fullTableScanEnabled", false);
        defaultValues.put("realmSuffixExclusionPatterns", null);
        updatedValues.put("realmSuffixExclusionPatterns", Collections.singletonList(".*"));
        defaultValues.put("defaultType", NoOpType.class);
        updatedValues.put("defaultType", LcNoDiacriticsType.class);
        defaultValues.put("shardDateFormatter", new SimpleDateFormat("yyyyMMdd"));
        defaultPredicates.put("shardDateFormatter", (Predicate<SimpleDateFormat>) o1 -> o1.toPattern().equals("yyyyMMdd"));
        updatedValues.put("shardDateFormatter", new SimpleDateFormat("yyyyMMddHHmmss"));
        updatedPredicates.put("shardDateFormatter", (Predicate<SimpleDateFormat>) o1 -> o1.toPattern().equals("yyyyMMddHHmmss"));
        defaultValues.put("useEnrichers", false);
        updatedValues.put("useEnrichers", true);
        defaultValues.put("useFilters", false);
        updatedValues.put("useFilters", true);
        defaultValues.put("indexFilteringClassNames", Lists.newArrayList());
        updatedValues.put("indexFilteringClassNames", Lists.newArrayList("proj.datawave.query.filter.someIndexFilterClass"));
        defaultValues.put("indexHoles", Lists.newArrayList());
        updatedValues.put("indexHoles", Lists.newArrayList(new IndexHole()));
        defaultValues.put("indexedFields", Sets.newHashSet());
        updatedValues.put("indexedFields", Sets.newHashSet("FIELD_C", "FIELD_D"));
        defaultValues.put("reverseIndexedFields", Sets.newHashSet());
        updatedValues.put("reverseIndexedFields", Sets.newHashSet("C_DLEIF", "D_DLEIF"));
        defaultValues.put("normalizedFields", Sets.newHashSet());
        updatedValues.put("normalizedFields", Sets.newHashSet("FIELD_G", "FIELD_H"));
        alreadySet.add("normalizedFields");
        defaultValues.put("fieldToDiscreteIndexTypes", Maps.newHashMap());
        updatedValues.put("fieldToDiscreteIndexTypes", Collections.singletonMap("FIELD_I", new GeometryType()));
        defaultValues.put("compositeToFieldMap", ArrayListMultimap.create());
        updatedValues.put("compositeToFieldMap", createArrayListMultimap(ImmutableMultimap.<String,String> builder().put("FIELD_C", "FIELD_D")
                        .put("FIELD_C", "FIELD_E").put("FIELD_F", "FIELD_G").put("FIELD_F", "FIELD_H").build()));
        defaultValues.put("compositeTransitionDates", Maps.newHashMap());
        updatedValues.put("compositeTransitionDates", Collections.singletonMap("VIRTUAL_FIELD", new Date()));
        defaultValues.put("compositeFieldSeparators", Maps.newHashMap());
        updatedValues.put("compositeFieldSeparators", Collections.singletonMap("VIRTUAL_FIELD", "|"));
        defaultValues.put("whindexCreationDates", Maps.newHashMap());
        updatedValues.put("whindexCreationDates", Collections.singletonMap("FIELD_W", new Date()));
        defaultValues.put("evaluationOnlyFields", Sets.newHashSet());
        updatedValues.put("evaluationOnlyFields", Sets.newHashSet("FIELD_E", "FIELD_F"));
        defaultValues.put("disallowedRegexPatterns", Sets.newHashSet(".*", ".*?"));
        updatedValues.put("disallowedRegexPatterns", Sets.newHashSet(".*", ".*?", ".*.*"));
        defaultValues.put("disableWhindexFieldMappings", false);
        updatedValues.put("disableWhindexFieldMappings", true);
        defaultValues.put("whindexMappingFields", Sets.newHashSet());
        updatedValues.put("whindexMappingFields", Sets.newHashSet("FIELD_A", "FIELD_B"));
        defaultValues.put("whindexFieldMappings", Maps.newHashMap());
        updatedValues.put("whindexFieldMappings", Collections.singletonMap("FIELD_A", Collections.singletonMap("FIELD_B", "FIELD_C")));
        defaultValues.put("sortedUIDs", true);
        updatedValues.put("sortedUIDs", false);
        defaultValues.put("queryTermFrequencyFields", Sets.newHashSet());
        updatedValues.put("queryTermFrequencyFields", Sets.newHashSet("FIELD_Q", "FIELD_R"));
        defaultValues.put("termFrequenciesRequired", false);
        updatedValues.put("termFrequenciesRequired", true);
        defaultValues.put("limitFieldsPreQueryEvaluation", false);
        updatedValues.put("limitFieldsPreQueryEvaluation", true);
        defaultValues.put("limitFieldsField", null);
        updatedValues.put("limitFieldsField", "LIMITED");
        defaultValues.put("hitList", false);
        updatedValues.put("hitList", true);
        defaultValues.put("dateIndexTimeTravel", false);
        updatedValues.put("dateIndexTimeTravel", true);
        defaultValues.put("beginDateCap", -1L);
        updatedValues.put("beginDateCap", 1000L);
        defaultValues.put("failOutsideValidDateRange", true);
        updatedValues.put("failOutsideValidDateRange", false);
        defaultValues.put("rawTypes", false);
        updatedValues.put("rawTypes", true);
        defaultValues.put("minSelectivity", -1.0);
        updatedValues.put("minSelectivity", -2.0);
        defaultValues.put("includeDataTypeAsField", false);
        updatedValues.put("includeDataTypeAsField", true);
        defaultValues.put("includeRecordId", true);
        updatedValues.put("includeRecordId", false);
        defaultValues.put("includeHierarchyFields", false);
        updatedValues.put("includeHierarchyFields", true);
        defaultValues.put("hierarchyFieldOptions", Maps.newHashMap());
        updatedValues.put("hierarchyFieldOptions", Collections.singletonMap("OPTION", "VALUE"));
        defaultValues.put("includeGroupingContext", false);
        updatedValues.put("includeGroupingContext", true);
        defaultValues.put("documentPermutations", Lists.newArrayList());
        updatedValues.put("documentPermutations", Lists.newArrayList("datawave.query.function.NoOpMaskedValueFilter"));
        defaultValues.put("filterMaskedValues", true);
        updatedValues.put("filterMaskedValues", false);
        defaultValues.put("reducedResponse", false);
        updatedValues.put("reducedResponse", true);
        defaultValues.put("allowShortcutEvaluation", true);
        updatedValues.put("allowShortcutEvaluation", false);
        defaultValues.put("speculativeScanning", false);
        updatedValues.put("speculativeScanning", true);
        defaultValues.put("disableEvaluation", false);
        updatedValues.put("disableEvaluation", true);
        defaultValues.put("containsIndexOnlyTerms", false);
        updatedValues.put("containsIndexOnlyTerms", true);
        defaultValues.put("containsCompositeTerms", false);
        updatedValues.put("containsCompositeTerms", true);
        defaultValues.put("allowFieldIndexEvaluation", true);
        updatedValues.put("allowFieldIndexEvaluation", false);
        defaultValues.put("allowTermFrequencyLookup", true);
        updatedValues.put("allowTermFrequencyLookup", false);
        defaultValues.put("expandUnfieldedNegations", true);
        updatedValues.put("expandUnfieldedNegations", false);
        defaultValues.put("returnType", DocumentSerialization.DEFAULT_RETURN_TYPE);
        updatedValues.put("returnType", DocumentSerialization.ReturnType.writable);
        defaultValues.put("eventPerDayThreshold", 10000);
        updatedValues.put("eventPerDayThreshold", 10340);
        defaultValues.put("shardsPerDayThreshold", 10);
        updatedValues.put("shardsPerDayThreshold", 18);
        defaultValues.put("initialMaxTermThreshold", 2500);
        updatedValues.put("initialMaxTermThreshold", 2540);
        defaultValues.put("intermediateMaxTermThreshold", 2500);
        updatedValues.put("intermediateMaxTermThreshold", 5500);
        defaultValues.put("indexedMaxTermThreshold", 2500);
        updatedValues.put("indexedMaxTermThreshold", 5500);
        defaultValues.put("finalMaxTermThreshold", 2500);
        updatedValues.put("finalMaxTermThreshold", 2501);
        defaultValues.put("maxDepthThreshold", 2500);
        updatedValues.put("maxDepthThreshold", 253);
        defaultValues.put("expandFields", true);
        updatedValues.put("expandFields", false);
        defaultValues.put("maxUnfieldedExpansionThreshold", 500);
        updatedValues.put("maxUnfieldedExpansionThreshold", 507);
        defaultValues.put("expandValues", true);
        updatedValues.put("expandValues", false);
        defaultValues.put("maxValueExpansionThreshold", 5000);
        updatedValues.put("maxValueExpansionThreshold", 5060);
        defaultValues.put("maxOrExpansionThreshold", 500);
        updatedValues.put("maxOrExpansionThreshold", 550);
        defaultValues.put("maxOrRangeThreshold", 10);
        updatedValues.put("maxOrRangeThreshold", 12);
        defaultValues.put("maxOrRangeIvarators", 10);
        updatedValues.put("maxOrRangeIvarators", 11);
        defaultValues.put("maxRangesPerRangeIvarator", 5);
        updatedValues.put("maxRangesPerRangeIvarator", 6);
        defaultValues.put("maxOrExpansionFstThreshold", 750);
        updatedValues.put("maxOrExpansionFstThreshold", 500);
        defaultValues.put("yieldThresholdMs", Long.MAX_VALUE);
        updatedValues.put("yieldThresholdMs", 65535L);
        defaultValues.put("hdfsSiteConfigURLs", null);
        updatedValues.put("hdfsSiteConfigURLs", "file://etc/hadoop/hdfs_site.xml");
        defaultValues.put("hdfsFileCompressionCodec", null);
        updatedValues.put("hdfsFileCompressionCodec", "sunny");
        defaultValues.put("zookeeperConfig", null);
        updatedValues.put("zookeeperConfig", "file://etc/zookeeper/conf");
        defaultValues.put("ivaratorCacheDirConfigs", Collections.emptyList());
        updatedValues.put("ivaratorCacheDirConfigs", Lists.newArrayList(new IvaratorCacheDirConfig("hdfs://instance-a/ivarators")));
        defaultValues.put("ivaratorFstHdfsBaseURIs", null);
        updatedValues.put("ivaratorFstHdfsBaseURIs", "hdfs://instance-a/fsts");
        defaultValues.put("ivaratorCacheBufferSize", 10000);
        updatedValues.put("ivaratorCacheBufferSize", 1040);
        defaultValues.put("ivaratorCacheScanPersistThreshold", 100000L);
        updatedValues.put("ivaratorCacheScanPersistThreshold", 1040L);
        defaultValues.put("ivaratorCacheScanTimeout", 3600000L);
        updatedValues.put("ivaratorCacheScanTimeout", 3600L);
        defaultValues.put("maxFieldIndexRangeSplit", 11);
        updatedValues.put("maxFieldIndexRangeSplit", 20);
        defaultValues.put("ivaratorMaxOpenFiles", 100);
        updatedValues.put("ivaratorMaxOpenFiles", 103);
        defaultValues.put("ivaratorNumRetries", 2);
        updatedValues.put("ivaratorNumRetries", 3);
        defaultValues.put("ivaratorPersistVerify", true);
        updatedValues.put("ivaratorPersistVerify", false);
        defaultValues.put("ivaratorPersistVerifyCount", 100);
        updatedValues.put("ivaratorPersistVerifyCount", 101);
        defaultValues.put("maxIvaratorSources", 33);
        updatedValues.put("maxIvaratorSources", 16);
        defaultValues.put("maxIvaratorResults", -1L);
        updatedValues.put("maxIvaratorResults", 10000L);
        defaultValues.put("maxIvaratorTerms", -1);
        updatedValues.put("maxIvaratorTerms", 50);
        defaultValues.put("maxIvaratorSourceWait", 1000L * 60 * 30);
        updatedValues.put("maxIvaratorSourceWait", 1000L * 60 * 10);
        defaultValues.put("maxEvaluationPipelines", 25);
        updatedValues.put("maxEvaluationPipelines", 24);
        defaultValues.put("maxPipelineCachedResults", 25);
        updatedValues.put("maxPipelineCachedResults", 26);
        defaultValues.put("expandAllTerms", false);
        updatedValues.put("expandAllTerms", true);
        defaultValues.put("queryModel", null);
        updatedValues.put("queryModel", new QueryModel());
        defaultValues.put("modelName", null);
        updatedValues.put("modelName", "MODEL_A");
        defaultValues.put("modelTableName", TableName.METADATA);
        updatedValues.put("modelTableName", "datawave." + TableName.METADATA);
        defaultValues.put("query", new QueryImpl());
        updatedValues.put("query", createQuery("A == B"));
        updatedPredicates.put("query", (Predicate<Query>) o1 -> o1.getQuery().equals("A == B"));
        defaultValues.put("compressServerSideResults", false);
        updatedValues.put("compressServerSideResults", true);
        defaultValues.put("indexOnlyFilterFunctionsEnabled", false);
        updatedValues.put("indexOnlyFilterFunctionsEnabled", true);
        defaultValues.put("compositeFilterFunctionsEnabled", false);
        updatedValues.put("compositeFilterFunctionsEnabled", true);
        defaultValues.put("uniqueFields", new UniqueFields());
        updatedValues.put("uniqueFields", UniqueFields.from("FIELD_U,FIELD_V"));
        defaultValues.put("cacheModel", false);
        updatedValues.put("cacheModel", true);
        defaultValues.put("trackSizes", true);
        updatedValues.put("trackSizes", false);
        defaultValues.put("contentFieldNames", Lists.newArrayList());
        updatedValues.put("contentFieldNames", Lists.newArrayList("FIELD_C", "FIELD_D"));
        defaultValues.put("activeQueryLogNameSource", null);
        updatedValues.put("activeQueryLogNameSource", ShardQueryConfiguration.QUERY_LOGIC_NAME_SOURCE);
        defaultValues.put("enforceUniqueConjunctionsWithinExpression", false);
        updatedValues.put("enforceUniqueConjunctionsWithinExpression", true);
        defaultValues.put("enforceUniqueDisjunctionsWithinExpression", false);
        updatedValues.put("enforceUniqueDisjunctionsWithinExpression", true);
        defaultValues.put("noExpansionFields", Sets.newHashSet());
        updatedValues.put("noExpansionFields", Sets.newHashSet("FIELD_N", "FIELD_O"));
        defaultValues.put("lenientFields", Sets.newHashSet());
        updatedValues.put("lenientFields", Sets.newHashSet("FIELD_L", "FIELD_M"));
        defaultValues.put("strictFields", Sets.newHashSet());
        updatedValues.put("strictFields", Sets.newHashSet("FIELD_S", "FIELD_T"));
        defaultValues.put("queryExecutionForPageTimeout", 3000000L);
        updatedValues.put("queryExecutionForPageTimeout", 30000L);
        defaultValues.put("excerptFields", new ExcerptFields());
        updatedValues.put("excerptFields", ExcerptFields.from("FIELD_E/10,FIELD_F/11"));
        defaultValues.put("excerptIterator", TermFrequencyExcerptIterator.class);
        updatedValues.put("excerptIterator", TermFrequencyIndexIterator.class);
        defaultValues.put("fiFieldSeek", -1);
        updatedValues.put("fiFieldSeek", 10);
        defaultValues.put("fiNextSeek", -1);
        updatedValues.put("fiNextSeek", 11);
        defaultValues.put("eventFieldSeek", -1);
        updatedValues.put("eventFieldSeek", 12);
        defaultValues.put("eventNextSeek", -1);
        updatedValues.put("eventNextSeek", 13);
        defaultValues.put("tfFieldSeek", -1);
        updatedValues.put("tfFieldSeek", 14);
        defaultValues.put("tfNextSeek", -1);
        updatedValues.put("tfNextSeek", 15);
        defaultValues.put("seekingEventAggregation", false);
        updatedValues.put("seekingEventAggregation", true);
        defaultValues.put("visitorFunctionMaxWeight", 5000000L);
        updatedValues.put("visitorFunctionMaxWeight", 1000000L);
        defaultValues.put("lazySetMechanismEnabled", false);
        updatedValues.put("lazySetMechanismEnabled", true);
        defaultValues.put("docAggregationThresholdMs", -1);
        updatedValues.put("docAggregationThresholdMs", 30000);
        defaultValues.put("tfAggregationThresholdMs", -1);
        updatedValues.put("tfAggregationThresholdMs", 10000);
        defaultValues.put("pruneQueryOptions", false);
        updatedValues.put("pruneQueryOptions", true);
        defaultValues.put("reduceIngestTypes", false);
        updatedValues.put("reduceIngestTypes", true);
        defaultValues.put("reduceIngestTypesPerShard", false);
        updatedValues.put("reduceIngestTypesPerShard", true);
        defaultValues.put("pruneQueryByIngestTypes", false);
        updatedValues.put("pruneQueryByIngestTypes", true);
        defaultValues.put("numIndexLookupThreads", 8);
        updatedValues.put("numIndexLookupThreads", 18);
        defaultValues.put("accrueStats", false);
        updatedValues.put("accrueStats", true);
        defaultValues.put("dataTypes", HashMultimap.create());
        updatedValues.put("dataTypes", createHashMultimap(
                        ImmutableMultimap.<String,Type<?>> builder().put("FIELD_C", new DateType()).put("FIELD_D", new LcNoDiacriticsType()).build()));

        defaultValues.put("enricherClassNames", null);
        updatedValues.put("enricherClassNames", Lists.newArrayList("proj.datawave.query.enricher.someEnricherClass"));

        defaultValues.put("filterClassNames", Collections.emptyList());
        updatedValues.put("filterClassNames", Lists.newArrayList("proj.datawave.query.filter.someFilterClass"));

        defaultValues.put("nonEventKeyPrefixes", Sets.newHashSet("d", "tf"));
        updatedValues.put("nonEventKeyPrefixes", Sets.newHashSet("d", "tf", "fi"));
        defaultValues.put("nonEventKeyPrefixesAsString", "d,tf");
        updatedValues.put("nonEventKeyPrefixesAsString", "d,tf,fi");
        alreadySet.add("nonEventKeyPrefixesAsString");

        defaultValues.put("unevaluatedFields", Sets.newHashSet());
        updatedValues.put("unevaluatedFields", Sets.newHashSet("FIELD_U", "FIELD_V"));

        defaultValues.put("datatypeFilter", Sets.newHashSet());
        updatedValues.put("datatypeFilter", Sets.newHashSet("TYPE_A", "TYPE_B"));
        defaultValues.put("datatypeFilterAsString", "");
        updatedValues.put("datatypeFilterAsString", "TYPE_A,TYPE_B");
        alreadySet.add("datatypeFilterAsString");

        defaultValues.put("projectFields", Sets.newHashSet());
        updatedValues.put("projectFields", Sets.newHashSet("FIELD_P", "FIELD_Q"));
        defaultValues.put("projectFieldsAsString", "");
        updatedValues.put("projectFieldsAsString", "FIELD_P,FIELD_Q");
        alreadySet.add("projectFieldsAsString");

        defaultValues.put("renameFields", Sets.newHashSet());
        updatedValues.put("renameFields", Collections.singleton("UUID=ID"));

        defaultValues.put("fieldIndexHoleMinThreshold", 1.0d);
        updatedValues.put("fieldIndexHoleMinThreshold", 0.5d);

        defaultValues.put("disallowlistedFields", Sets.newHashSet());
        updatedValues.put("disallowlistedFields", Sets.newHashSet("FIELD_B", "FIELD_C"));
        defaultValues.put("disallowlistedFieldsAsString", "");
        updatedValues.put("disallowlistedFieldsAsString", "FIELD_B,FIELD_C");
        alreadySet.add("disallowlistedFieldsAsString");

        defaultValues.put("queryFieldsDatatypes", HashMultimap.create());
        updatedValues.put("queryFieldsDatatypes", createHashMultimap(
                        ImmutableMultimap.<String,Type<?>> builder().put("FIELD_E", new DateType()).put("FIELD_F", new LcNoDiacriticsType()).build()));
        defaultValues.put("indexedFieldDataTypesAsString", "");
        updatedValues.put("indexedFieldDataTypesAsString", "FIELD_E:datawave.data.type.DateType;FIELD_F:datawave.data.type.LcNoDiacriticsType;");
        alreadySet.add("indexedFieldDataTypesAsString");

        defaultValues.put("normalizedFieldsDatatypes", HashMultimap.create());
        updatedValues.put("normalizedFieldsDatatypes", createHashMultimap(
                        ImmutableMultimap.<String,Type<?>> builder().put("FIELD_G", new DateType()).put("FIELD_H", new LcNoDiacriticsType()).build()));
        defaultValues.put("normalizedFieldNormalizersAsString", "");
        updatedValues.put("normalizedFieldNormalizersAsString", "FIELD_G:datawave.data.type.DateType;FIELD_H:datawave.data.type.LcNoDiacriticsType;");
        alreadySet.add("normalizedFieldNormalizersAsString");

        defaultValues.put("limitFields", Sets.newHashSet());
        updatedValues.put("limitFields", Sets.newHashSet("FIELD_L", "FIELD_M"));
        defaultValues.put("limitFieldsAsString", "");
        updatedValues.put("limitFieldsAsString", "FIELD_L,FIELD_M");
        alreadySet.add("limitFieldsAsString");

        defaultValues.put("matchingFieldSets", Sets.newHashSet());
        updatedValues.put("matchingFieldSets", Sets.newHashSet("FIELD_M=FIELD_N,FIELD_O=FIELD_P"));
        defaultValues.put("matchingFieldSetsAsString", "");
        updatedValues.put("matchingFieldSetsAsString", "FIELD_M=FIELD_N,FIELD_O=FIELD_P");
        alreadySet.add("matchingFieldSetsAsString");

        defaultValues.put("groupFieldsBatchSize", 0);
        updatedValues.put("groupFieldsBatchSize", 5);
        defaultValues.put("groupFieldsBatchSizeAsString", "0");
        updatedValues.put("groupFieldsBatchSizeAsString", "5");
        alreadySet.add("groupFieldsBatchSizeAsString");

        defaultValues.put("groupFields", new GroupFields());
        updatedValues.put("groupFields", GroupFields.from("GROUP(FIELD_G,FIELD_H)"));

        defaultValues.put("sortQueryPreIndexWithImpliedCounts", false);
        updatedValues.put("sortQueryPreIndexWithImpliedCounts", true);
        defaultValues.put("sortQueryPreIndexWithFieldCounts", false);
        updatedValues.put("sortQueryPreIndexWithFieldCounts", true);
        defaultValues.put("sortQueryPostIndexWithTermCounts", false);
        updatedValues.put("sortQueryPostIndexWithTermCounts", true);
        defaultValues.put("sortQueryPostIndexWithFieldCounts", false);
        updatedValues.put("sortQueryPostIndexWithFieldCounts", true);
        defaultValues.put("tableConsistencyLevels", Collections.emptyMap());
        updatedValues.put("tableConsistencyLevels", Collections.singletonMap(TableName.SHARD, ScannerBase.ConsistencyLevel.EVENTUAL));
        defaultValues.put("tableHints", Collections.emptyMap());
        updatedValues.put("tableHints", Collections.emptyMap());

        defaultValues.put("useQueryTreeScanHintRules", false);
        updatedValues.put("useQueryTreeScanHintRules", true);
        defaultValues.put("queryTreeScanHintRules", Collections.emptyList());
        updatedValues.put("queryTreeScanHintRules", Collections.singletonList(new IvaratorScanHint()));
    }

    private Query createQuery(String query) {
        QueryImpl q = new QueryImpl();
        q.setQuery(query);
        return q;
    }

    private HashMultimap<String,?> createHashMultimap(Multimap<String,?> multimap) {
        HashMultimap hashMultimap = HashMultimap.create();
        for (Map.Entry<String,?> entry : multimap.entries()) {
            hashMultimap.put(entry.getKey(), entry.getValue());
        }
        return hashMultimap;
    }

    private ArrayListMultimap<String,?> createArrayListMultimap(Multimap<String,?> multimap) {
        ArrayListMultimap arrayListMultimap = ArrayListMultimap.create();
        for (Map.Entry<String,?> entry : multimap.entries()) {
            arrayListMultimap.put(entry.getKey(), entry.getValue());
        }
        return arrayListMultimap;
    }

    private void testValues(ShardQueryConfiguration config, Map<String,Object> values, Map<String,Predicate> predicates) throws Exception {
        ObjectMapper mapper = JsonMapper.builder().enable(MapperFeature.PROPAGATE_TRANSIENT_MARKER).build();
        JsonNode root = mapper.readTree(mapper.writeValueAsString(config));
        Set<String> fieldsFound = new HashSet<>();
        for (Iterator<String> it = root.fieldNames(); it.hasNext();) {
            String fieldName = it.next();
            Assert.assertTrue("Missing values for " + fieldName + ".  Please add default and updated values at the top of " + this.getClass().getSimpleName(),
                            values.containsKey(fieldName));
            fieldsFound.add(fieldName);
            Object value = getValue(config, fieldName);
            if (predicates.containsKey(fieldName)) {
                Assert.assertTrue("Unexpected value for " + fieldName, predicates.get(fieldName).test(value));
            } else if (fieldName.endsWith("AsString")) {
                Assert.assertTrue("Unexpected value for " + fieldName, isUnorderedListEqual(String.valueOf(values.get(fieldName)), String.valueOf(value)));
            } else {
                Assert.assertEquals("Unexpected value for " + fieldName, values.get(fieldName), value);
            }
        }
        for (String fieldName : alreadySet) {
            Assert.assertTrue("Missing values for " + fieldName + ".  Please add default and updated values at the top of " + this.getClass().getSimpleName(),
                            values.containsKey(fieldName));
            fieldsFound.add(fieldName);
            Object value = getValue(config, fieldName);
            if (predicates.containsKey(fieldName)) {
                Assert.assertTrue("Unexpected value for " + fieldName, predicates.get(fieldName).test(value));
            } else if (fieldName.endsWith("AsString")) {
                Assert.assertTrue("Unexpected value for " + fieldName, isUnorderedListEqual(String.valueOf(values.get(fieldName)), String.valueOf(value)));
            } else {
                Assert.assertEquals("Unexpected value for " + fieldName, values.get(fieldName), value);
            }
        }
        Assert.assertEquals("Unexpected additional entries in defaultValues: " + Sets.difference(values.keySet(), fieldsFound), values.size(),
                        fieldsFound.size());
    }

    public boolean isUnorderedListEqual(String expected, String actual) {
        Set<String> expectedSet = new HashSet(Arrays.asList(expected.split("[,;]")));
        Set<String> actualSet = new HashSet(Arrays.asList(actual.split("[,;]")));
        return expectedSet.equals(actualSet);
    }

    public Object getValue(Object source, String fieldName) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        String getter = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        try {
            return source.getClass().getMethod(getter).invoke(source);
        } catch (NoSuchMethodException e) {
            getter = "is" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
            return source.getClass().getMethod(getter).invoke(source);
        }
    }

    public void setValue(Object source, String fieldName, Object value) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        setValue(source, fieldName, value, value.getClass());
    }

    public Object setValue(Object source, String fieldName, Object value, Class<?> valueClass)
                    throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String getter = "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        try {
            return source.getClass().getMethod(getter, valueClass).invoke(source, value);
        } catch (NoSuchMethodException e) {
            if (primitiveMap.containsKey(valueClass)) {
                return setValue(source, fieldName, value, primitiveMap.get(valueClass));
            } else if (!valueClass.equals(Object.class) && !valueClass.isPrimitive()) {
                for (Class<?> infc : valueClass.getInterfaces()) {
                    try {
                        return setValue(source, fieldName, value, infc);
                    } catch (NoSuchMethodException e2) {
                        // try next one
                    }
                }
                if (!valueClass.isInterface()) {
                    return setValue(source, fieldName, value, valueClass.getSuperclass());
                }
            }
            throw e;
        }
    }

    /**
     * Assert expected default values from an empty constructor call
     */
    @Test
    public void testEmptyConstructor() throws Exception {
        ShardQueryConfiguration config = ShardQueryConfiguration.create();

        testValues(config, defaultValues, defaultPredicates);
    }

    /**
     * Test that for a given set of collections, stored in a ShardQueryConfiguration, will in fact be deep-copied into a new ShardQueryConfiguration object.
     */
    @Test
    public void testDeepCopyConstructor() throws Exception {

        // Instantiate a 'other' ShardQueryConfiguration
        ShardQueryConfiguration other = ShardQueryConfiguration.create();

        for (Map.Entry<String,Object> entry : updatedValues.entrySet()) {
            if (!alreadySet.contains(entry.getKey())) {
                setValue(other, entry.getKey(), entry.getValue());
            }
        }

        for (Map.Entry<String,Object> entry : extraValuesToSet.entrySet()) {
            setValue(other, entry.getKey(), entry.getValue());
        }

        // Copy 'other' ShardQueryConfiguration into a new config
        ShardQueryConfiguration config = ShardQueryConfiguration.create(other);

        testValues(config, updatedValues, updatedPredicates);
    }

    @Test
    public void whenRetrievingActiveQueryLogName_givenTableNameSource_thenReturnsTableName() {
        ShardQueryConfiguration configuration = new ShardQueryConfiguration();
        configuration.setTableName("shardTable");
        configuration.setActiveQueryLogNameSource(ShardQueryConfiguration.TABLE_NAME_SOURCE);
        Assert.assertEquals("shardTable", configuration.getActiveQueryLogName());
    }

    @Test
    public void whenRetrievingActiveQueryLogName_givenQueryLogicNameSource_thenReturnsQueryLogicName() {
        ShardQueryConfiguration configuration = new ShardQueryConfiguration();
        configuration.setActiveQueryLogNameSource(ShardQueryConfiguration.QUERY_LOGIC_NAME_SOURCE);
        Assert.assertEquals(ShardQueryConfiguration.class.getSimpleName(), configuration.getActiveQueryLogName());
    }

    @Test
    public void whenRetrievingActiveQueryLogName_givenNoActiveQueryLogNameValue_thenReturnsBlankString() {
        ShardQueryConfiguration configuration = new ShardQueryConfiguration();
        Assert.assertEquals("", configuration.getActiveQueryLogName());
    }

    @Test
    public void whenRetrievingActiveQueryLogName_givenOtherValue_thenReturnsBlankString() {
        ShardQueryConfiguration configuration = new ShardQueryConfiguration();
        configuration.setActiveQueryLogNameSource("nonMatchingValue");
        Assert.assertEquals("", configuration.getActiveQueryLogName());
    }
}
