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

import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.data.type.DateType;
import datawave.data.type.GeometryType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.ExcerptFields;
import datawave.query.attributes.UniqueFields;
import datawave.query.common.grouping.GroupFields;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.iterator.logic.TermFrequencyExcerptIterator;
import datawave.query.iterator.logic.TermFrequencyIndexIterator;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.model.QueryModel;
import datawave.util.TableName;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;

public class ShardQueryConfigurationTest {

    // @formatter:off
    public final static Map<Class<?>,Class<?>> primitiveMap = new ImmutableMap.Builder<Class<?>, Class<?>>()
                    .put(Boolean.class, boolean.class)
                    .put(Byte.class, byte.class)
                    .put(Short.class, short.class)
                    .put(Character.class, char.class)
                    .put(Integer.class, int.class)
                    .put(Long.class, long.class)
                    .put(Float.class, float.class)
                    .put(Double.class, double.class)
                    .build();
    // @formatter:on

    /**
     * The set of properties of {@link ShardQueryConfiguration} that are set internally via other properties.
     */
    // @formatter:off
    private static final Set<String> internallySetFields = ImmutableSet.of(
                    "projectFieldsAsString",
                    "groupFieldsBatchSizeAsString",
                    "normalizedFields",
                    "indexedFieldDataTypesAsString",
                    "datatypeFilterAsString",
                    "unevaluatedFieldsAsString",
                    "queryString",
                    "filterClassNamesAsString",
                    "disallowlistedFieldsAsString",
                    "matchingFieldSetsAsString",
                    "enricherClassNamesAsString",
                    "limitFieldsAsString",
                    "normalizedFieldNormalizersAsString",
                    "nonEventKeyPrefixesAsString"
    );
    // @formatter:on

    /**
     * Assert expected default values from an empty constructor call.
     */
    @Test
    public void testEmptyConstructor() throws Exception {
        // @formatter:off
        Map<String, ValueAssert> asserts = new ValueAssertMapBuilder()
                        .assertValue("authorizations", Collections.singleton(Authorizations.EMPTY))
                        .assertValue("queryString", null)
                        .assertValue("beginDate", null)
                        .assertValue("endDate", null)
                        .assertValue("maxWork", -1L)
                        .assertValue("baseIteratorPriority", 100)
                        .assertValue("tableName", TableName.SHARD)
                        .assertValue("bypassAccumulo", false)
                        .assertValue("accumuloPassword", "")
                        .assertValue("tldQuery", false)
                        .assertValue("filterOptions", Maps.newHashMap())
                        .assertValue("disableIndexOnlyDocuments", false)
                        .assertValue("maxScannerBatchSize", 1000)
                        .assertValue("maxIndexBatchSize", 1000)
                        .assertValue("allTermsIndexOnly", false)
                        .assertValue("maxIndexScanTimeMillis", Long.MAX_VALUE)
                        .assertValue("parseTldUids", false)
                        .assertValue("ignoreNonExistentFields", false)
                        .assertValue("collapseUids", false)
                        .assertValue("collapseUidsThreshold", -1)
                        .assertValue("enforceUniqueTermsWithinExpressions", false)
                        .assertValue("reduceQueryFields", false)
                        .assertValue("reduceQueryFieldsPerShard", false)
                        .assertValue("reduceTypeMetadata", false)
                        .assertValue("reduceTypeMetadataPerShard", false)
                        .assertValue("sequentialScheduler", false)
                        .assertValue("collectTimingDetails", false)
                        .assertValue("logTimingDetails", false)
                        .assertValue("sendTimingToStatsd", true)
                        .assertValue("statsdHost", "localhost")
                        .assertValue("statsdPort", 8125)
                        .assertValue("statsdMaxQueueSize", 500)
                        .assertValue("limitAnyFieldLookups", false)
                        .assertValue("bypassExecutabilityCheck", false)
                        .assertValue("generatePlanOnly", false)
                        .assertValue("backoffEnabled", false)
                        .assertValue("unsortedUIDsEnabled", true)
                        .assertValue("serializeQueryIterator", false)
                        .assertValue("debugMultithreadedSources", false)
                        .assertValue("sortGeoWaveQueryRanges", false)
                        .assertValue("numRangesToBuffer", 0)
                        .assertValue("rangeBufferTimeoutMillis", 0L)
                        .assertValue("rangeBufferPollMillis", 100L)
                        .assertValue("geometryMaxExpansion", 8)
                        .assertValue("pointMaxExpansion", 32)
                        .assertValue("geoMaxExpansion", 32)
                        .assertValue("geoWaveRangeSplitThreshold", 16)
                        .assertValue("geoWaveMaxRangeOverlap", 0.25)
                        .assertValue("optimizeGeoWaveRanges", true)
                        .assertValue("geoWaveMaxEnvelopes", 4)
                        .assertValue("shardTableName", TableName.SHARD)
                        .assertValue("indexTableName", TableName.SHARD_INDEX)
                        .assertValue("reverseIndexTableName", TableName.SHARD_RINDEX)
                        .assertValue("metadataTableName", TableName.METADATA)
                        .assertValue("dateIndexTableName", TableName.DATE_INDEX)
                        .assertValue("indexStatsTableName", TableName.INDEX_STATS)
                        .assertValue("defaultDateTypeName", "EVENT")
                        .assertValue("cleanupShardsAndDaysQueryHints", true)
                        .assertValue("numQueryThreads", 8)
                        .assertValue("numDateIndexThreads", 8)
                        .assertValue("maxDocScanTimeout", -1)
                        .assertValue("collapseDatePercentThreshold", 0.99f)
                        .assertValue("fullTableScanEnabled", true)
                        .assertValue("realmSuffixExclusionPatterns", null)
                        .assertValue("defaultType", NoOpType.class)
                        .assertPredicate("shardDateFormatter", new SimpleDateFormat("yyyyMMdd"), (Predicate<SimpleDateFormat>) v -> v.toPattern().equals("yyyyMMdd"))
                        .assertValue("useEnrichers", false)
                        .assertValue("useFilters", false)
                        .assertValue("indexFilteringClassNames", Collections.emptyList())
                        .assertValue("indexHoles", Collections.emptyList())
                        .assertValue("indexedFields", Collections.emptySet())
                        .assertValue("reverseIndexedFields", Collections.emptySet())
                        .assertValue("normalizedFields", Collections.emptySet())
                        .assertValue("fieldToDiscreteIndexTypes", Collections.emptyMap())
                        .assertValue("compositeToFieldMap", ArrayListMultimap.create())
                        .assertValue("compositeTransitionDates", Collections.emptyMap())
                        .assertValue("compositeFieldSeparators", Collections.emptyMap())
                        .assertValue("whindexCreationDates", Collections.emptyMap())
                        .assertValue("evaluationOnlyFields", Collections.emptySet())
                        .assertValue("disallowedRegexPatterns", Sets.newHashSet(".*", ".*?"))
                        .assertValue("disableWhindexFieldMappings", false)
                        .assertValue("whindexMappingFields", Collections.emptySet())
                        .assertValue("whindexFieldMappings", Collections.emptyMap())
                        .assertValue("sortedUIDs", true)
                        .assertValue("queryTermFrequencyFields", Collections.emptySet())
                        .assertValue("termFrequenciesRequired", false)
                        .assertValue("limitFieldsPreQueryEvaluation", false)
                        .assertValue("limitFieldsField", null)
                        .assertValue("hitList", false)
                        .assertValue("dateIndexTimeTravel", false)
                        .assertValue("beginDateCap", -1L)
                        .assertValue("failOutsideValidDateRange", true)
                        .assertValue("rawTypes", false)
                        .assertValue("minSelectivity", -1.0)
                        .assertValue("includeDataTypeAsField", false)
                        .assertValue("includeRecordId", true)
                        .assertValue("includeHierarchyFields", false)
                        .assertValue("hierarchyFieldOptions", Collections.emptyMap())
                        .assertValue("includeGroupingContext", false)
                        .assertValue("documentPermutations", Collections.emptyList())
                        .assertValue("filterMaskedValues", true)
                        .assertValue("reducedResponse", false)
                        .assertValue("allowShortcutEvaluation", true)
                        .assertValue("speculativeScanning", false)
                        .assertValue("disableEvaluation", false)
                        .assertValue("containsIndexOnlyTerms", false)
                        .assertValue("containsCompositeTerms", false)
                        .assertValue("allowFieldIndexEvaluation", true)
                        .assertValue("allowTermFrequencyLookup", true)
                        .assertValue("expandUnfieldedNegations", true)
                        .assertValue("returnType", DocumentSerialization.DEFAULT_RETURN_TYPE)
                        .assertValue("eventPerDayThreshold", 10000)
                        .assertValue("shardsPerDayThreshold", 10)
                        .assertValue("initialMaxTermThreshold", 2500)
                        .assertValue("indexedMaxTermThreshold", 2500)
                        .assertValue("finalMaxTermThreshold", 2500)
                        .assertValue("maxDepthThreshold", 2500)
                        .assertValue("expandFields", true)
                        .assertValue("maxUnfieldedExpansionThreshold", 500)
                        .assertValue("expandValues", true)
                        .assertValue("maxValueExpansionThreshold", 5000)
                        .assertValue("maxOrExpansionThreshold", 500)
                        .assertValue("maxOrRangeThreshold", 10)
                        .assertValue("maxOrRangeIvarators", 10)
                        .assertValue("maxRangesPerRangeIvarator", 5)
                        .assertValue("maxOrExpansionFstThreshold", 750)
                        .assertValue("yieldThresholdMs", Long.MAX_VALUE)
                        .assertValue("hdfsSiteConfigURLs", null)
                        .assertValue("hdfsFileCompressionCodec", null)
                        .assertValue("zookeeperConfig", null)
                        .assertValue("ivaratorCacheDirConfigs", Collections.emptyList())
                        .assertValue("ivaratorFstHdfsBaseURIs", null)
                        .assertValue("ivaratorCacheBufferSize", 10000)
                        .assertValue("ivaratorCacheScanPersistThreshold", 100000L)
                        .assertValue("ivaratorCacheScanTimeout", 3600000L)
                        .assertValue("maxFieldIndexRangeSplit", 11)
                        .assertValue("ivaratorMaxOpenFiles", 100)
                        .assertValue("ivaratorNumRetries", 2)
                        .assertValue("ivaratorPersistVerify", true)
                        .assertValue("ivaratorPersistVerifyCount", 100)
                        .assertValue("maxIvaratorSources", 33)
                        .assertValue("maxIvaratorResults", -1L)
                        .assertValue("maxIvaratorTerms", -1)
                        .assertValue("maxIvaratorSourceWait", 1000L * 60 * 30)
                        .assertValue("maxEvaluationPipelines", 25)
                        .assertValue("maxPipelineCachedResults", 25)
                        .assertValue("expandAllTerms", false)
                        .assertValue("queryModel", null)
                        .assertValue("modelName", null)
                        .assertValue("modelTableName", TableName.METADATA)
                        .assertValue("query", new QueryImpl())
                        .assertValue("compressServerSideResults", false)
                        .assertValue("compositeFilterFunctionsEnabled", false)
                        .assertValue("indexOnlyFilterFunctionsEnabled", false)
                        .assertValue("uniqueFields", new UniqueFields())
                        .assertValue("cacheModel", false)
                        .assertValue("trackSizes", true)
                        .assertValue("contentFieldNames", Collections.emptyList())
                        .assertValue("activeQueryLogNameSource", null)
                        .assertValue("enforceUniqueConjunctionsWithinExpression", false)
                        .assertValue("enforceUniqueDisjunctionsWithinExpression", false)
                        .assertValue("noExpansionFields", Collections.emptySet())
                        .assertValue("lenientFields", Collections.emptySet())
                        .assertValue("strictFields", Collections.emptySet())
                        .assertValue("queryExecutionForPageTimeout", 3000000L)
                        .assertValue("excerptFields", new ExcerptFields())
                        .assertValue("excerptIterator", TermFrequencyExcerptIterator.class)
                        .assertValue("fiFieldSeek", -1)
                        .assertValue("fiNextSeek", -1)
                        .assertValue("eventFieldSeek", -1)
                        .assertValue("eventNextSeek", -1)
                        .assertValue("tfFieldSeek", -1)
                        .assertValue("tfNextSeek", -1)
                        .assertValue("visitorFunctionMaxWeight", 5000000L)
                        .assertValue("lazySetMechanismEnabled", false)
                        .assertValue("docAggregationThresholdMs", -1)
                        .assertValue("tfAggregationThresholdMs", -1)
                        .assertValue("pruneQueryOptions", false)
                        .assertValue("pruneQueryByIngestTypes", false)
                        .assertValue("numIndexLookupThreads", 8)
                        .assertValue("expansionLimitedToModelContents", false)
                        .assertValue("accrueStats", false)
                        .assertValue("dataTypes", HashMultimap.create())
                        .assertValue("enricherClassNames", null)
                        .assertValue("enricherClassNamesAsString", null)
                        .assertValue("filterClassNames", null)
                        .assertValue("filterClassNamesAsString", null)
                        .assertValue("nonEventKeyPrefixes", Sets.newHashSet("d", "tf"))
                        .assertValue("nonEventKeyPrefixesAsString", "d,tf")
                        .assertValue("unevaluatedFields", Collections.emptySet())
                        .assertValue("unevaluatedFieldsAsString", "")
                        .assertValue("datatypeFilter", Collections.emptySet())
                        .assertValue("datatypeFilterAsString", "")
                        .assertValue("projectFields", Collections.emptySet())
                        .assertValue("projectFieldsAsString", "")
                        .assertValue("disallowlistedFields", Collections.emptySet())
                        .assertValue("disallowlistedFieldsAsString", "")
                        .assertValue("queryFieldsDatatypes", HashMultimap.create())
                        .assertValue("indexedFieldDataTypesAsString", "")
                        .assertValue("normalizedFieldsDatatypes", HashMultimap.create())
                        .assertValue("normalizedFieldNormalizersAsString", "")
                        .assertValue("limitFields", Collections.emptySet())
                        .assertValue("limitFieldsAsString", "")
                        .assertValue("matchingFieldSets", Collections.emptySet())
                        .assertValue("matchingFieldSetsAsString", "")
                        .assertValue("groupFieldsBatchSize", 0)
                        .assertValue("groupFieldsBatchSizeAsString", "0")
                        .assertValue("groupFields", new GroupFields())
                        .assertValue("fieldIndexHoleMinThreshold", 1.0d)
                        .assertValue("intermediateMaxTermThreshold", 2500)
                        .build();
        // @formatter:on

        ShardQueryConfiguration config = ShardQueryConfiguration.create();
        testValues(config, asserts);
    }

    /**
     * Test that for a given set of collections, stored in a ShardQueryConfiguration, will in fact be deep-copied into a new ShardQueryConfiguration object.
     */
    @Test
    public void testDeepCopyConstructor() throws Exception {
        // @formatter:off
        Map<String, ValueAssert> asserts = new ValueAssertMapBuilder()
                        .assertValue("authorizations", Collections.singleton(new Authorizations("FOO", "BAR")))
                        .assertValue("queryString", "A == B")
                        .assertValue("beginDate", new Date())
                        .assertValue("endDate", new Date())
                        .assertValue("maxWork", 1000L)
                        .assertValue("baseIteratorPriority", 14)
                        .assertValue("tableName", "datawave." + TableName.SHARD)
                        .assertValue("bypassAccumulo", true)
                        .assertValue("accumuloPassword", "secret")
                        .assertValue("tldQuery", true)
                        .assertValue("filterOptions", Collections.singletonMap("FIELD_A", "FILTER"))
                        .assertValue("disableIndexOnlyDocuments", true)
                        .assertValue("maxScannerBatchSize", 1300)
                        .assertValue("maxIndexBatchSize", 1100)
                        .assertValue("allTermsIndexOnly", true)
                        .assertValue("maxIndexScanTimeMillis", 100000L)
                        .assertValue("parseTldUids", true)
                        .assertValue("ignoreNonExistentFields", true)
                        .assertValue("collapseUids", true)
                        .assertValue("collapseUidsThreshold", 400)
                        .assertValue("enforceUniqueTermsWithinExpressions", true)
                        .assertValue("reduceQueryFields", true)
                        .assertValue("reduceQueryFieldsPerShard", true)
                        .assertValue("reduceTypeMetadata", true)
                        .assertValue("reduceTypeMetadataPerShard", true)
                        .assertValue("sequentialScheduler", true)
                        .assertValue("collectTimingDetails", true)
                        .assertValue("logTimingDetails", true)
                        .assertValue("sendTimingToStatsd", false)
                        .assertValue("statsdHost", "10.0.0.1")
                        .assertValue("statsdPort", 8126)
                        .assertValue("statsdMaxQueueSize", 530)
                        .assertValue("limitAnyFieldLookups", true)
                        .assertValue("bypassExecutabilityCheck", true)
                        .assertValue("generatePlanOnly", true)
                        .assertValue("backoffEnabled", true)
                        .assertValue("unsortedUIDsEnabled", false)
                        .assertValue("serializeQueryIterator", true)
                        .assertValue("debugMultithreadedSources", true)
                        .assertValue("sortGeoWaveQueryRanges", true)
                        .assertValue("numRangesToBuffer", 4)
                        .assertValue("rangeBufferTimeoutMillis", 1000L)
                        .assertValue("rangeBufferPollMillis", 140L)
                        .assertValue("geometryMaxExpansion", 4)
                        .assertValue("pointMaxExpansion", 36)
                        .assertValue("geoMaxExpansion", 39)
                        .assertValue("geoWaveRangeSplitThreshold", 14)
                        .assertValue("geoWaveMaxRangeOverlap", 0.35)
                        .assertValue("optimizeGeoWaveRanges", false)
                        .assertValue("geoWaveMaxEnvelopes", 40)
                        .assertValue("shardTableName", "datawave." + TableName.SHARD)
                        .assertValue("indexTableName", "datawave." + TableName.SHARD_INDEX)
                        .assertValue("reverseIndexTableName", "datawave." + TableName.SHARD_RINDEX)
                        .assertValue("metadataTableName", "datawave." + TableName.METADATA)
                        .assertValue("dateIndexTableName", "datawave." + TableName.DATE_INDEX)
                        .assertValue("indexStatsTableName", "datawave." + TableName.INDEX_STATS)
                        .assertValue("defaultDateTypeName", "DOCUMENT")
                        .assertValue("cleanupShardsAndDaysQueryHints", false)
                        .assertValue("numQueryThreads", 80)
                        .assertValue("numDateIndexThreads", 20)
                        .assertValue("maxDocScanTimeout", 10000)
                        .assertValue("collapseDatePercentThreshold", 0.92f)
                        .assertValue("fullTableScanEnabled", false)
                        .assertValue("realmSuffixExclusionPatterns", Collections.singletonList(".*"))
                        .assertValue("defaultType", LcNoDiacriticsType.class)
                        .assertPredicate("shardDateFormatter", new SimpleDateFormat("yyyyMMddHHmmss"), (Predicate<SimpleDateFormat>) o1 -> o1.toPattern().equals("yyyyMMddHHmmss"))
                        .assertValue("useEnrichers", true)
                        .assertValue("useFilters", true)
                        .assertValue("indexFilteringClassNames", Lists.newArrayList("proj.datawave.query.filter.someIndexFilterClass"))
                        .assertValue("indexHoles", Lists.newArrayList(new IndexHole()))
                        .assertValue("indexedFields", Sets.newHashSet("FIELD_C", "FIELD_D"))
                        .assertValue("reverseIndexedFields", Sets.newHashSet("C_DLEIF", "D_DLEIF"))
                        .assertValue("normalizedFields", Sets.newHashSet("FIELD_G", "FIELD_H"))
                        .assertValue("fieldToDiscreteIndexTypes", Collections.singletonMap("FIELD_I", new GeometryType()))
                        .assertValue("compositeToFieldMap", createArrayListMultimap(ImmutableMultimap.<String,String> builder().put("FIELD_C", "FIELD_D")
                                        .put("FIELD_C", "FIELD_E").put("FIELD_F", "FIELD_G").put("FIELD_F", "FIELD_H").build()))
                        .assertValue("compositeTransitionDates", Collections.singletonMap("VIRTUAL_FIELD", new Date()))
                        .assertValue("compositeFieldSeparators", Collections.singletonMap("VIRTUAL_FIELD", "|"))
                        .assertValue("whindexCreationDates", Collections.singletonMap("FIELD_W", new Date()))
                        .assertValue("evaluationOnlyFields", Sets.newHashSet("FIELD_E", "FIELD_F"))
                        .assertValue("disallowedRegexPatterns", Sets.newHashSet(".*", ".*?", ".*.*"))
                        .assertValue("disableWhindexFieldMappings", true)
                        .assertValue("whindexMappingFields", Sets.newHashSet("FIELD_A", "FIELD_B"))
                        .assertValue("whindexFieldMappings", Collections.singletonMap("FIELD_A", Collections.singletonMap("FIELD_B", "FIELD_C")))
                        .assertValue("sortedUIDs", false)
                        .assertValue("queryTermFrequencyFields", Sets.newHashSet("FIELD_Q", "FIELD_R"))
                        .assertValue("termFrequenciesRequired", true)
                        .assertValue("limitFieldsPreQueryEvaluation", true)
                        .assertValue("limitFieldsField", "LIMITED")
                        .assertValue("hitList", true)
                        .assertValue("dateIndexTimeTravel", true)
                        .assertValue("beginDateCap", 1000L)
                        .assertValue("failOutsideValidDateRange", false)
                        .assertValue("rawTypes", true)
                        .assertValue("minSelectivity", -2.0)
                        .assertValue("includeDataTypeAsField", true)
                        .assertValue("includeRecordId", false)
                        .assertValue("includeHierarchyFields", true)
                        .assertValue("hierarchyFieldOptions", Collections.singletonMap("OPTION", "VALUE"))
                        .assertValue("includeGroupingContext", true)
                        .assertValue("documentPermutations", Lists.newArrayList("datawave.query.function.NoOpMaskedValueFilter"))
                        .assertValue("filterMaskedValues", false)
                        .assertValue("reducedResponse", true)
                        .assertValue("allowShortcutEvaluation", false)
                        .assertValue("speculativeScanning", true)
                        .assertValue("disableEvaluation", true)
                        .assertValue("containsIndexOnlyTerms", true)
                        .assertValue("containsCompositeTerms", true)
                        .assertValue("allowFieldIndexEvaluation", false)
                        .assertValue("allowTermFrequencyLookup", false)
                        .assertValue("expandUnfieldedNegations", false)
                        .assertValue("returnType", DocumentSerialization.ReturnType.writable)
                        .assertValue("eventPerDayThreshold", 10340)
                        .assertValue("shardsPerDayThreshold", 18)
                        .assertValue("initialMaxTermThreshold", 2540)
                        .assertValue("indexedMaxTermThreshold", 5500)
                        .assertValue("finalMaxTermThreshold", 2501)
                        .assertValue("maxDepthThreshold", 253)
                        .assertValue("expandFields", false)
                        .assertValue("maxUnfieldedExpansionThreshold", 507)
                        .assertValue("expandValues", false)
                        .assertValue("maxValueExpansionThreshold", 5060)
                        .assertValue("maxOrExpansionThreshold", 550)
                        .assertValue("maxOrRangeThreshold", 12)
                        .assertValue("maxOrRangeIvarators", 11)
                        .assertValue("maxRangesPerRangeIvarator", 6)
                        .assertValue("maxOrExpansionFstThreshold", 500)
                        .assertValue("yieldThresholdMs", 65535L)
                        .assertValue("hdfsSiteConfigURLs", "file://etc/hadoop/hdfs_site.xml")
                        .assertValue("hdfsFileCompressionCodec", "sunny")
                        .assertValue("zookeeperConfig", "file://etc/zookeeper/conf")
                        .assertValue("ivaratorCacheDirConfigs", Lists.newArrayList(new IvaratorCacheDirConfig("hdfs://instance-a/ivarators")))
                        .assertValue("ivaratorFstHdfsBaseURIs", "hdfs://instance-a/fsts")
                        .assertValue("ivaratorCacheBufferSize", 1040)
                        .assertValue("ivaratorCacheScanPersistThreshold", 1040L)
                        .assertValue("ivaratorCacheScanTimeout", 3600L)
                        .assertValue("maxFieldIndexRangeSplit", 20)
                        .assertValue("ivaratorMaxOpenFiles", 103)
                        .assertValue("ivaratorNumRetries", 3)
                        .assertValue("ivaratorPersistVerify", false)
                        .assertValue("ivaratorPersistVerifyCount", 101)
                        .assertValue("maxIvaratorSources", 16)
                        .assertValue("maxIvaratorResults", 10000L)
                        .assertValue("maxIvaratorTerms", 50)
                        .assertValue("maxIvaratorSourceWait", 1000L * 60 * 10)
                        .assertValue("maxEvaluationPipelines", 24)
                        .assertValue("maxPipelineCachedResults", 26)
                        .assertValue("expandAllTerms", true)
                        .assertValue("queryModel", new QueryModel())
                        .assertValue("modelName", "MODEL_A")
                        .assertValue("modelTableName", "datawave." + TableName.METADATA)
                        .assertPredicate("query", createQuery("A == B"), (Predicate<Query>) o1 -> o1.getQuery().equals("A == B"))
                        .assertValue("compressServerSideResults", true)
                        .assertValue("indexOnlyFilterFunctionsEnabled", true)
                        .assertValue("compositeFilterFunctionsEnabled", true)
                        .assertValue("uniqueFields", UniqueFields.from("FIELD_U,FIELD_V"))
                        .assertValue("cacheModel", true)
                        .assertValue("trackSizes", false)
                        .assertValue("contentFieldNames", Lists.newArrayList("FIELD_C", "FIELD_D"))
                        .assertValue("activeQueryLogNameSource", ShardQueryConfiguration.QUERY_LOGIC_NAME_SOURCE)
                        .assertValue("enforceUniqueConjunctionsWithinExpression", true)
                        .assertValue("enforceUniqueDisjunctionsWithinExpression", true)
                        .assertValue("noExpansionFields", Sets.newHashSet("FIELD_N", "FIELD_O"))
                        .assertValue("lenientFields", Sets.newHashSet("FIELD_L", "FIELD_M"))
                        .assertValue("strictFields", Sets.newHashSet("FIELD_S", "FIELD_T"))
                        .assertValue("queryExecutionForPageTimeout", 30000L)
                        .assertValue("excerptFields", ExcerptFields.from("FIELD_E/10,FIELD_F/11"))
                        .assertValue("excerptIterator", TermFrequencyIndexIterator.class)
                        .assertValue("fiFieldSeek", 10)
                        .assertValue("fiNextSeek", 11)
                        .assertValue("eventFieldSeek", 12)
                        .assertValue("eventNextSeek", 13)
                        .assertValue("tfFieldSeek", 14)
                        .assertValue("tfNextSeek", 15)
                        .assertValue("visitorFunctionMaxWeight", 1000000L)
                        .assertValue("lazySetMechanismEnabled", true)
                        .assertValue("docAggregationThresholdMs", 30000)
                        .assertValue("tfAggregationThresholdMs", 10000)
                        .assertValue("pruneQueryOptions", true)
                        .assertValue("pruneQueryByIngestTypes", true)
                        .assertValue("numIndexLookupThreads", 18)
                        .assertValue("expansionLimitedToModelContents", true)
                        .assertValue("accrueStats", true)
                        .assertValue("dataTypes", createHashMultimap(
                                        ImmutableMultimap.<String,Type<?>> builder().put("FIELD_C", new DateType()).put("FIELD_D", new LcNoDiacriticsType()).build()))
                        .assertValue("enricherClassNames", Lists.newArrayList("proj.datawave.query.enricher.someEnricherClass"))
                        .assertValue("enricherClassNamesAsString", "proj.datawave.query.enricher.someEnricherClass")
                        .assertValue("filterClassNames", Lists.newArrayList("proj.datawave.query.filter.someFilterClass"))
                        .assertValue("filterClassNamesAsString", "proj.datawave.query.filter.someFilterClass")
                        .assertValue("nonEventKeyPrefixes", Sets.newHashSet("d", "tf", "fi"))
                        .assertDelimitedString("nonEventKeyPrefixesAsString", "d,tf,fi")
                        .assertValue("unevaluatedFields", Sets.newHashSet("FIELD_U", "FIELD_V"))
                        .assertDelimitedString("unevaluatedFieldsAsString", "FIELD_U,FIELD_V")
                        .assertValue("datatypeFilter", Sets.newHashSet("TYPE_A", "TYPE_B"))
                        .assertDelimitedString("datatypeFilterAsString", "TYPE_A,TYPE_B")
                        .assertValue("projectFields", Sets.newHashSet("FIELD_P", "FIELD_Q"))
                        .assertDelimitedString("projectFieldsAsString", "FIELD_P,FIELD_Q")
                        .assertValue("disallowlistedFields", Sets.newHashSet("FIELD_B", "FIELD_C"))
                        .assertDelimitedString("disallowlistedFieldsAsString", "FIELD_B,FIELD_C")
                        .assertValue("queryFieldsDatatypes", createHashMultimap(
                                        ImmutableMultimap.<String,Type<?>> builder().put("FIELD_E", new DateType()).put("FIELD_F", new LcNoDiacriticsType()).build()))
                        .assertDelimitedString("indexedFieldDataTypesAsString", "FIELD_E:datawave.data.type.DateType;FIELD_F:datawave.data.type.LcNoDiacriticsType")
                        .assertValue("normalizedFieldsDatatypes", createHashMultimap(
                                        ImmutableMultimap.<String,Type<?>> builder().put("FIELD_G", new DateType()).put("FIELD_H", new LcNoDiacriticsType()).build()))
                        .assertDelimitedString("normalizedFieldNormalizersAsString", "FIELD_G:datawave.data.type.DateType;FIELD_H:datawave.data.type.LcNoDiacriticsType")
                        .assertValue("limitFields", Sets.newHashSet("FIELD_L", "FIELD_M"))
                        .assertDelimitedString("limitFieldsAsString", "FIELD_L,FIELD_M")
                        .assertValue("matchingFieldSets", Sets.newHashSet("FIELD_M=FIELD_N,FIELD_O=FIELD_P"))
                        .assertDelimitedString("matchingFieldSetsAsString", "FIELD_M=FIELD_N,FIELD_O=FIELD_P")
                        .assertValue("groupFieldsBatchSize", 5)
                        .assertValue("groupFieldsBatchSizeAsString", "5")
                        .assertValue("groupFields", GroupFields.from("GROUP(FIELD_G,FIELD_H)"))
                        .assertValue("fieldIndexHoleMinThreshold", 0.75d)
                        .assertValue("intermediateMaxTermThreshold", 5500)
                        .build();
        // @formatter:on

        // Instantiate ShardQueryConfiguration with non-default values.
        ShardQueryConfiguration other = ShardQueryConfiguration.create();
        for (String field : asserts.keySet()) {
            if (!internallySetFields.contains(field)) {
                Object value = asserts.get(field).getExpected();
                setValue(other, field, value);
            }
        }

        // Set this manually.
        setValue(other, "queryTree", JexlASTHelper.parseAndFlattenJexlQuery("A == B"));

        // Copy 'other' ShardQueryConfiguration into a new config
        ShardQueryConfiguration config = ShardQueryConfiguration.create(other);

        testValues(config, asserts);
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

    private void testValues(ShardQueryConfiguration config, Map<String,ValueAssert> asserts)
                    throws JsonProcessingException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(mapper.writeValueAsString(config));
        Set<String> fieldsSeen = new HashSet<>();

        for (Iterator<String> it = root.fieldNames(); it.hasNext();) {
            String field = it.next();
            fieldsSeen.add(field);

            // Verify a value assert was supplied for the current field.
            Assert.assertTrue("Missing value assert for " + field + ". Please add assert.", asserts.containsKey(field));

            // Fetch the value for the field from the configuration.
            Object value = getValue(config, field);

            // Assert the value is as expected.
            ValueAssert valueAssert = asserts.get(field);
            valueAssert.assertValue(value);
        }

        for (String field : internallySetFields) {
            fieldsSeen.add(field);

            // Verify a value assert was supplied for the current field.
            Assert.assertTrue("Missing value assert for " + field + ". Please add assert.", asserts.containsKey(field));

            // Fetch the value for the field from the configuration.
            Object value = getValue(config, field);

            // Assert the value is as expected.
            ValueAssert valueAssert = asserts.get(field);
            valueAssert.assertValue(value);
        }

        Set<String> expectedFields = asserts.keySet();
        Assert.assertEquals("Unexpected fields found: " + Sets.difference(expectedFields, fieldsSeen), expectedFields.size(), fieldsSeen.size());
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

    private interface ValueAssert {
        String getField();

        Object getExpected();

        void assertValue(Object actual);
    }

    private static class ValueAssertMapBuilder {

        private final Map<String,ValueAssert> asserts = new HashMap<>();

        public ValueAssertMapBuilder assertValue(String field, Object value) {
            asserts.put(field, new ObjectAssert(field, value));
            return this;
        }

        public ValueAssertMapBuilder assertDelimitedString(String field, String value) {
            asserts.put(field, new DelimitedStringAssert(field, value));
            return this;
        }

        @SuppressWarnings("rawtypes")
        public ValueAssertMapBuilder assertPredicate(String field, Object expected, Predicate predicate) {
            asserts.put(field, new PredicateAssert(field, expected, predicate));
            return this;
        }

        public Map<String,ValueAssert> build() {
            return asserts;
        }
    }

    private static abstract class BaseValueAssert implements ValueAssert {
        protected final String field;
        protected final Object expected;

        protected BaseValueAssert(String field, Object expected) {
            this.field = field;
            this.expected = expected;
        }

        @Override
        public String getField() {
            return field;
        }

        @Override
        public Object getExpected() {
            return expected;
        }
    }

    private static class ObjectAssert extends BaseValueAssert {

        public ObjectAssert(String field, Object expected) {
            super(field, expected);
        }

        @Override
        public void assertValue(Object actual) {
            Assert.assertEquals("Unexpected value for " + field, expected, actual);
        }
    }

    private static class DelimitedStringAssert extends BaseValueAssert {

        private static final String splitPattern = "[,;]";

        private final Set<String> expectedSet;

        public DelimitedStringAssert(String field, String expected) {
            super(field, expected);
            this.expectedSet = split(expected);
        }

        @Override
        public void assertValue(Object actual) {
            Assert.assertEquals("Unexpected value for " + field, expectedSet, split(String.valueOf(actual)));
        }

        private Set<String> split(String string) {
            if (string == null) {
                return null;
            }
            return new HashSet<>(Arrays.asList(string.split(splitPattern)));
        }
    }

    @SuppressWarnings("rawtypes")
    private static class PredicateAssert extends BaseValueAssert {

        private final Predicate predicate;

        public PredicateAssert(String field, Object expected, Predicate predicate) {
            super(field, expected);
            this.predicate = predicate;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void assertValue(Object actual) {
            Assert.assertTrue("Unexpected value for " + field, predicate.test(actual));
        }
    }
}
