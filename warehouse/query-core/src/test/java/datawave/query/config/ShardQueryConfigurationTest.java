package datawave.query.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.data.type.DateType;
import datawave.data.type.DiscreteIndexType;
import datawave.data.type.GeometryType;
import datawave.data.type.NoOpType;
import datawave.data.type.StringType;
import datawave.data.type.Type;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.UniqueFields;
import datawave.query.attributes.UniqueGranularity;
import datawave.query.function.DocumentPermutation;
import datawave.query.function.DocumentProjection;
import datawave.query.model.QueryModel;
import datawave.query.tables.ShardQueryLogic;
import datawave.util.TableName;
import datawave.webservice.query.QueryImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class ShardQueryConfigurationTest {
    
    private static ShardQueryConfiguration config;
    
    @BeforeAll
    static void setUp() {
        config = ShardQueryConfiguration.create();
    }
    
    /**
     * Assert expected default values from an empty constructor call
     */
    @Test
    public void testEmptyConstructor() {
        ShardQueryConfiguration config = ShardQueryConfiguration.create();
        
        Assertions.assertFalse(config.isTldQuery());
        Assertions.assertEquals(Maps.newHashMap(), config.getFilterOptions());
        Assertions.assertFalse(config.isDisableIndexOnlyDocuments());
        Assertions.assertEquals(1000, config.getMaxScannerBatchSize());
        Assertions.assertEquals(1000, config.getMaxIndexBatchSize());
        Assertions.assertFalse(config.isAllTermsIndexOnly());
        Assertions.assertEquals("", config.getAccumuloPassword());
        Assertions.assertEquals(Long.MAX_VALUE, config.getMaxIndexScanTimeMillis());
        Assertions.assertFalse(config.getCollapseUids());
        Assertions.assertFalse(config.getParseTldUids());
        Assertions.assertFalse(config.getSequentialScheduler());
        Assertions.assertFalse(config.getCollectTimingDetails());
        Assertions.assertFalse(config.getLogTimingDetails());
        Assertions.assertTrue(config.getSendTimingToStatsd());
        Assertions.assertEquals("localhost", config.getStatsdHost());
        Assertions.assertEquals(8125, config.getStatsdPort());
        Assertions.assertEquals(500, config.getStatsdMaxQueueSize());
        Assertions.assertFalse(config.getLimitAnyFieldLookups());
        Assertions.assertFalse(config.isBypassExecutabilityCheck());
        Assertions.assertFalse(config.getBackoffEnabled());
        Assertions.assertTrue(config.getUnsortedUIDsEnabled());
        Assertions.assertFalse(config.getSerializeQueryIterator());
        Assertions.assertFalse(config.isDebugMultithreadedSources());
        Assertions.assertFalse(config.isSortGeoWaveQueryRanges());
        Assertions.assertEquals(0, config.getNumRangesToBuffer());
        Assertions.assertEquals(0, config.getRangeBufferTimeoutMillis());
        Assertions.assertEquals(100, config.getRangeBufferPollMillis());
        Assertions.assertEquals(8, config.getGeometryMaxExpansion());
        Assertions.assertEquals(32, config.getPointMaxExpansion());
        Assertions.assertEquals(16, config.getGeoWaveRangeSplitThreshold());
        Assertions.assertEquals(0.25, config.getGeoWaveMaxRangeOverlap(), 0.0);
        Assertions.assertTrue(config.isOptimizeGeoWaveRanges());
        Assertions.assertEquals(4, config.getGeoWaveMaxEnvelopes());
        Assertions.assertEquals(TableName.SHARD, config.getShardTableName());
        Assertions.assertEquals(TableName.SHARD_INDEX, config.getIndexTableName());
        Assertions.assertEquals(TableName.SHARD_RINDEX, config.getReverseIndexTableName());
        Assertions.assertEquals(TableName.METADATA, config.getMetadataTableName());
        Assertions.assertEquals(TableName.DATE_INDEX, config.getDateIndexTableName());
        Assertions.assertEquals(TableName.INDEX_STATS, config.getIndexStatsTableName());
        Assertions.assertEquals("EVENT", config.getDefaultDateTypeName());
        Assertions.assertTrue(config.isCleanupShardsAndDaysQueryHints());
        Assertions.assertEquals(new Integer(8), config.getNumQueryThreads());
        Assertions.assertEquals(new Integer(8), config.getNumIndexLookupThreads());
        Assertions.assertEquals(new Integer(8), config.getNumDateIndexThreads());
        Assertions.assertEquals(new Integer(-1), config.getMaxDocScanTimeout());
        Assertions.assertNotNull(config.getFstCount());
        Assertions.assertEquals(0.99f, config.getCollapseDatePercentThreshold(), 0);
        Assertions.assertTrue(config.getFullTableScanEnabled());
        Assertions.assertNull(config.getRealmSuffixExclusionPatterns());
        Assertions.assertEquals(NoOpType.class, config.getDefaultType());
        Assertions.assertEquals(new SimpleDateFormat("yyyyMMdd"), config.getShardDateFormatter());
        Assertions.assertFalse(config.getUseEnrichers());
        Assertions.assertNull(config.getEnricherClassNames());
        Assertions.assertFalse(config.getUseFilters());
        Assertions.assertNull(config.getFilterClassNames());
        Assertions.assertEquals(Lists.newArrayList(), config.getIndexFilteringClassNames());
        Assertions.assertEquals(Sets.newHashSet("d", "tf"), config.getNonEventKeyPrefixes());
        Assertions.assertEquals(Sets.newHashSet(), config.getUnevaluatedFields());
        Assertions.assertEquals(Sets.newHashSet(), config.getDatatypeFilter());
        Assertions.assertEquals(Lists.newArrayList(), config.getIndexHoles());
        Assertions.assertEquals(Sets.newHashSet(), config.getProjectFields());
        Assertions.assertEquals(Sets.newHashSet(), config.getBlacklistedFields());
        Assertions.assertEquals(Sets.newHashSet(), config.getIndexedFields());
        Assertions.assertEquals(Sets.newHashSet(), config.getNormalizedFields());
        Assertions.assertEquals(HashMultimap.create(), config.getDataTypes());
        Assertions.assertEquals(HashMultimap.create(), config.getQueryFieldsDatatypes());
        Assertions.assertEquals(HashMultimap.create(), config.getNormalizedFieldsDatatypes());
        Assertions.assertEquals(ArrayListMultimap.create(), config.getCompositeToFieldMap());
        Assertions.assertEquals(Maps.newHashMap(), config.getFieldToDiscreteIndexTypes());
        Assertions.assertEquals(Maps.newHashMap(), config.getCompositeTransitionDates());
        Assertions.assertTrue(config.isSortedUIDs());
        Assertions.assertEquals(Sets.newHashSet(), config.getQueryTermFrequencyFields());
        Assertions.assertFalse(config.isTermFrequenciesRequired());
        Assertions.assertEquals(Sets.newHashSet(), config.getLimitFields());
        Assertions.assertFalse(config.isLimitFieldsPreQueryEvaluation());
        Assertions.assertNull(config.getLimitFieldsField());
        Assertions.assertFalse(config.isHitList());
        Assertions.assertFalse(config.isDateIndexTimeTravel());
        Assertions.assertEquals(-1L, config.getBeginDateCap());
        Assertions.assertTrue(config.isFailOutsideValidDateRange());
        Assertions.assertFalse(config.isRawTypes());
        Assertions.assertEquals(-1.0, config.getMinSelectivity(), 0);
        Assertions.assertFalse(config.getIncludeDataTypeAsField());
        Assertions.assertTrue(config.getIncludeRecordId());
        Assertions.assertFalse(config.getIncludeHierarchyFields());
        Assertions.assertEquals(Maps.newHashMap(), config.getHierarchyFieldOptions());
        Assertions.assertFalse(config.getIncludeGroupingContext());
        Assertions.assertEquals(Lists.newArrayList(), config.getDocumentPermutations());
        Assertions.assertTrue(config.getFilterMaskedValues());
        Assertions.assertFalse(config.isReducedResponse());
        Assertions.assertTrue(config.getAllowShortcutEvaluation());
        Assertions.assertFalse(config.getBypassAccumulo());
        Assertions.assertFalse(config.getSpeculativeScanning());
        Assertions.assertFalse(config.isDisableEvaluation());
        Assertions.assertFalse(config.isContainsIndexOnlyTerms());
        Assertions.assertFalse(config.isContainsCompositeTerms());
        Assertions.assertTrue(config.isAllowFieldIndexEvaluation());
        Assertions.assertTrue(config.isAllowTermFrequencyLookup());
        Assertions.assertEquals(DocumentSerialization.DEFAULT_RETURN_TYPE, config.getReturnType());
        Assertions.assertEquals(10000, config.getEventPerDayThreshold());
        Assertions.assertEquals(10, config.getShardsPerDayThreshold());
        Assertions.assertEquals(2500, config.getInitialMaxTermThreshold());
        Assertions.assertEquals(2500, config.getFinalMaxTermThreshold());
        Assertions.assertEquals(2500, config.getMaxDepthThreshold());
        Assertions.assertEquals(500, config.getMaxUnfieldedExpansionThreshold());
        Assertions.assertEquals(5000, config.getMaxValueExpansionThreshold());
        Assertions.assertEquals(500, config.getMaxOrExpansionThreshold());
        Assertions.assertEquals(10, config.getMaxOrRangeThreshold());
        Assertions.assertEquals(10, config.getMaxOrRangeIvarators());
        Assertions.assertEquals(5, config.getMaxRangesPerRangeIvarator());
        Assertions.assertEquals(750, config.getMaxOrExpansionFstThreshold());
        Assertions.assertEquals(Long.MAX_VALUE, config.getYieldThresholdMs());
        Assertions.assertNull(config.getHdfsSiteConfigURLs());
        Assertions.assertNull(config.getHdfsFileCompressionCodec());
        Assertions.assertNull(config.getZookeeperConfig());
        Assertions.assertTrue(config.getIvaratorCacheDirConfigs().isEmpty());
        Assertions.assertEquals(2, config.getIvaratorNumRetries());
        Assertions.assertEquals(100, config.getIvaratorPersistVerifyCount());
        Assertions.assertTrue(config.isIvaratorPersistVerify());
        Assertions.assertNull(config.getIvaratorFstHdfsBaseURIs());
        Assertions.assertEquals(10000, config.getIvaratorCacheBufferSize());
        Assertions.assertEquals(100000, config.getIvaratorCacheScanPersistThreshold());
        Assertions.assertEquals(3600000, config.getIvaratorCacheScanTimeout());
        Assertions.assertEquals(11, config.getMaxFieldIndexRangeSplit());
        Assertions.assertEquals(100, config.getIvaratorMaxOpenFiles());
        Assertions.assertEquals(33, config.getMaxIvaratorSources());
        Assertions.assertEquals(25, config.getMaxEvaluationPipelines());
        Assertions.assertEquals(25, config.getMaxPipelineCachedResults());
        Assertions.assertFalse(config.isExpandAllTerms());
        Assertions.assertNull(config.getQueryModel());
        Assertions.assertNull(config.getModelName());
        Assertions.assertEquals(TableName.METADATA, config.getModelTableName());
        Assertions.assertFalse(config.isExpansionLimitedToModelContents());
        Assertions.assertEquals(new QueryImpl(), config.getQuery());
        Assertions.assertFalse(config.isCompressServerSideResults());
        Assertions.assertFalse(config.isIndexOnlyFilterFunctionsEnabled());
        Assertions.assertFalse(config.isCompositeFilterFunctionsEnabled());
        Assertions.assertEquals(0, config.getGroupFieldsBatchSize());
        Assertions.assertFalse(config.getAccrueStats());
        Assertions.assertEquals(Sets.newHashSet(), config.getGroupFields());
        Assertions.assertEquals(new UniqueFields(), config.getUniqueFields());
        Assertions.assertFalse(config.getCacheModel());
        Assertions.assertTrue(config.isTrackSizes());
        Assertions.assertEquals(Lists.newArrayList(), config.getContentFieldNames());
        Assertions.assertNull(config.getActiveQueryLogNameSource());
        Assertions.assertEquals("", config.getActiveQueryLogName());
        Assertions.assertFalse(config.isDisableWhindexFieldMappings());
        Assertions.assertEquals(Sets.newHashSet(), config.getWhindexMappingFields());
        Assertions.assertEquals(Maps.newHashMap(), config.getWhindexFieldMappings());
        Assertions.assertEquals(Collections.emptySet(), config.getNoExpansionFields());
        Assertions.assertEquals(Sets.newHashSet(".*", ".*?"), config.getDisallowedRegexPatterns());
        Assertions.assertEquals(5000000L, config.getVisitorFunctionMaxWeight());
    }
    
    /**
     * Test that for a given set of collections, stored in a ShardQueryConfiguration, will in fact be deep-copied into a new ShardQueryConfiguration object.
     */
    @Test
    public void testDeepCopyConstructor() {
        
        // Instantiate a 'other' ShardQueryConfiguration
        ShardQueryConfiguration other = ShardQueryConfiguration.create();
        
        // Setup collections for deep copy
        List<String> realmSuffixExclusionPatterns = Lists.newArrayList("somePattern");
        SimpleDateFormat shardDateFormatter = new SimpleDateFormat("yyyyMMdd");
        List<String> enricherClassNames = Lists.newArrayList("enricherClassNameA");
        List<String> filterClassNames = Lists.newArrayList("filterClassNameA");
        List<String> indexFilteringClassNames = Lists.newArrayList("indexFilteringClassNameA");
        Set<String> nonEventKeyPrefixes = Sets.newHashSet("nonEventKeyPrefixA");
        Set<String> unevaluatedFields = Sets.newHashSet("unevaluatedFieldA");
        Set<String> dataTypeFilter = Sets.newHashSet("dataTypeFilterA");
        IndexHole indexHole = new IndexHole(new String[] {"0", "1"}, new String[] {"2", "3"});
        List<IndexHole> indexHoles = Lists.newArrayList(indexHole);
        Set<String> projectFields = Sets.newHashSet("projectFieldA");
        Set<String> blacklistedFields = Sets.newHashSet("blacklistedFieldA");
        Set<String> indexedFields = Sets.newHashSet("indexedFieldA");
        Set<String> normalizedFields = Sets.newHashSet("normalizedFieldA");
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.put("K001", new NoOpType("V"));
        Multimap<String,Type<?>> queryFieldsDatatypes = HashMultimap.create();
        queryFieldsDatatypes.put("K002", new NoOpType("V"));
        Multimap<String,Type<?>> normalizedFieldsDatatypes = HashMultimap.create();
        normalizedFieldsDatatypes.put("K003", new NoOpType("V"));
        Multimap<String,String> compositeToFieldMap = HashMultimap.create();
        compositeToFieldMap.put("K004", "V");
        Map<String,DiscreteIndexType<?>> fieldToDiscreteIndexType = Maps.newHashMap();
        fieldToDiscreteIndexType.put("GEO", new GeometryType());
        Map<String,Date> compositeTransitionDates = Maps.newHashMap();
        Date transitionDate = new Date();
        compositeTransitionDates.put("K005", transitionDate);
        Map<String,String> compositeFieldSeparators = Maps.newHashMap();
        compositeFieldSeparators.put("GEO", " ");
        Set<String> queryTermFrequencyFields = Sets.newHashSet("fieldA");
        Set<String> limitFields = Sets.newHashSet("limitFieldA");
        Map<String,String> hierarchyFieldOptions = Maps.newHashMap();
        hierarchyFieldOptions.put("K006", "V");
        List<String> documentPermutations = Lists.newArrayList(DocumentPermutation.class.getName());
        QueryModel queryModel = new QueryModel();
        QueryImpl query = new QueryImpl();
        Set<String> groupFields = Sets.newHashSet("groupFieldA");
        UniqueFields uniqueFields = new UniqueFields();
        uniqueFields.put("uniqueFieldA", UniqueGranularity.ALL);
        List<String> contentFieldNames = Lists.newArrayList("fieldA");
        Set<String> noExpansionFields = Sets.newHashSet("NoExpansionFieldA");
        Set<String> disallowedRegexPatterns = Sets.newHashSet(".*", ".*?");
        long visitorFunctionMaxWeight = 200L;
        
        // Set collections on 'other' ShardQueryConfiguration
        other.setRealmSuffixExclusionPatterns(realmSuffixExclusionPatterns);
        other.setShardDateFormatter(shardDateFormatter);
        other.setEnricherClassNames(enricherClassNames);
        other.setFilterClassNames(filterClassNames);
        other.setIndexFilteringClassNames(indexFilteringClassNames);
        other.setNonEventKeyPrefixes(nonEventKeyPrefixes);
        other.setUnevaluatedFields(unevaluatedFields);
        other.setDatatypeFilter(dataTypeFilter);
        other.setIndexHoles(indexHoles);
        other.setProjectFields(projectFields);
        other.setBlacklistedFields(blacklistedFields);
        other.setIndexedFields(indexedFields);
        other.setNormalizedFields(normalizedFields);
        other.setDataTypes(dataTypes);
        other.setQueryFieldsDatatypes(queryFieldsDatatypes);
        other.setNormalizedFieldsDatatypes(normalizedFieldsDatatypes);
        other.setCompositeToFieldMap(compositeToFieldMap);
        other.setFieldToDiscreteIndexTypes(fieldToDiscreteIndexType);
        other.setCompositeTransitionDates(compositeTransitionDates);
        other.setCompositeFieldSeparators(compositeFieldSeparators);
        other.setQueryTermFrequencyFields(queryTermFrequencyFields);
        other.setLimitFields(limitFields);
        other.setHierarchyFieldOptions(hierarchyFieldOptions);
        other.setDocumentPermutations(documentPermutations);
        other.setQueryModel(queryModel);
        other.setQuery(query);
        other.setGroupFields(groupFields);
        other.setUniqueFields(uniqueFields);
        other.setContentFieldNames(contentFieldNames);
        other.setNoExpansionFields(noExpansionFields);
        other.setDisallowedRegexPatterns(disallowedRegexPatterns);
        other.setVisitorFunctionMaxWeight(visitorFunctionMaxWeight);
        
        // Copy 'other' ShardQueryConfiguration into a new config
        ShardQueryConfiguration config = ShardQueryConfiguration.create(other);
        
        // Modify original collections
        realmSuffixExclusionPatterns.add("anotherPattern");
        shardDateFormatter = new SimpleDateFormat("yyyyMMdd-mm:SS");
        enricherClassNames.add("enricherClassNameB");
        filterClassNames.add("filterClassNameB");
        indexFilteringClassNames.add("indexFilteringClassNameB");
        nonEventKeyPrefixes.add("nonEventKeyPrefixB");
        unevaluatedFields.add("unevaluatedFieldB");
        dataTypeFilter.add("dataTypeFilterB");
        IndexHole otherIndexHole = new IndexHole(new String[] {"4", "5"}, new String[] {"6", "7"});
        indexHoles.add(otherIndexHole);
        projectFields.add("projectFieldB");
        blacklistedFields.add("blacklistedFieldB");
        indexedFields.add("indexedFieldB");
        normalizedFields.add("normalizedFieldB");
        dataTypes.put("K2", new NoOpType("V2"));
        queryFieldsDatatypes.put("K", new NoOpType("V2"));
        normalizedFieldsDatatypes.put("K2", new NoOpType("V2"));
        compositeToFieldMap.put("K2", "V2");
        queryTermFrequencyFields.add("fieldB");
        limitFields.add("limitFieldB");
        hierarchyFieldOptions.put("K2", "V2");
        documentPermutations.add(DocumentProjection.class.getName());
        queryModel.addTermToModel("aliasA", "diskNameA");
        query.setId(UUID.randomUUID());
        groupFields.add("groupFieldB");
        uniqueFields.put("uniqueFieldB", UniqueGranularity.ALL);
        contentFieldNames.add("fieldB");
        disallowedRegexPatterns.add("blah");
        
        // Assert that copied collections were deep copied and remain unchanged
        Assertions.assertEquals(Lists.newArrayList("somePattern"), config.getRealmSuffixExclusionPatterns());
        Assertions.assertEquals(new SimpleDateFormat("yyyyMMdd"), config.getShardDateFormatter());
        Assertions.assertEquals(Lists.newArrayList("enricherClassNameA"), config.getEnricherClassNames());
        Assertions.assertEquals(Lists.newArrayList("filterClassNameA"), config.getFilterClassNames());
        Assertions.assertEquals(Lists.newArrayList("indexFilteringClassNameA"), config.getIndexFilteringClassNames());
        Assertions.assertEquals(Sets.newHashSet("nonEventKeyPrefixA"), config.getNonEventKeyPrefixes());
        Assertions.assertEquals(Sets.newHashSet("unevaluatedFieldA"), config.getUnevaluatedFields());
        Assertions.assertEquals(Sets.newHashSet("dataTypeFilterA"), config.getDatatypeFilter());
        IndexHole expectedIndexHole = new IndexHole(new String[] {"0", "1"}, new String[] {"2", "3"});
        Assertions.assertEquals(Lists.newArrayList(expectedIndexHole), config.getIndexHoles());
        Assertions.assertEquals(Sets.newHashSet("projectFieldA"), config.getProjectFields());
        Assertions.assertEquals(Sets.newHashSet("blacklistedFieldA"), config.getBlacklistedFields());
        Assertions.assertEquals(Sets.newHashSet("indexedFieldA"), config.getIndexedFields());
        // This assert is different from the setter as setNormalizedFieldsAsDatatypes will overwrite the normalizedFields with
        // a new keyset.
        Assertions.assertEquals(Sets.newHashSet("K003"), config.getNormalizedFields());
        Multimap<String,Type<?>> expectedDataTypes = HashMultimap.create();
        expectedDataTypes.put("K001", new NoOpType("V"));
        Assertions.assertEquals(expectedDataTypes, config.getDataTypes());
        Multimap<String,Type<?>> expectedQueryFieldsDatatypes = HashMultimap.create();
        expectedQueryFieldsDatatypes.put("K002", new NoOpType("V"));
        Assertions.assertEquals(expectedQueryFieldsDatatypes, config.getQueryFieldsDatatypes());
        Multimap<String,Type<?>> expectedNormalizedFieldsDatatypes = HashMultimap.create();
        expectedNormalizedFieldsDatatypes.put("K003", new NoOpType("V"));
        Assertions.assertEquals(expectedNormalizedFieldsDatatypes, config.getNormalizedFieldsDatatypes());
        Multimap<String,String> expectedCompositeToFieldMap = ArrayListMultimap.create();
        expectedCompositeToFieldMap.put("K004", "V");
        Assertions.assertEquals(expectedCompositeToFieldMap, config.getCompositeToFieldMap());
        Map<String,DiscreteIndexType<?>> expectedFieldToDiscreteIndexType = Maps.newHashMap();
        expectedFieldToDiscreteIndexType.put("GEO", new GeometryType());
        Assertions.assertEquals(expectedFieldToDiscreteIndexType, config.getFieldToDiscreteIndexTypes());
        Map<String,Date> expectedCompositeTransitionDates = Maps.newHashMap();
        expectedCompositeTransitionDates.put("K005", transitionDate);
        Assertions.assertEquals(expectedCompositeTransitionDates, config.getCompositeTransitionDates());
        Map<String,String> expectedCompositeFieldSeparators = Maps.newHashMap();
        expectedCompositeFieldSeparators.put("GEO", " ");
        Assertions.assertEquals(expectedCompositeFieldSeparators, config.getCompositeFieldSeparators());
        Assertions.assertEquals(Sets.newHashSet("fieldA"), config.getQueryTermFrequencyFields());
        Assertions.assertEquals(Sets.newHashSet("limitFieldA"), config.getLimitFields());
        Map<String,String> expectedHierarchyFieldOptions = Maps.newHashMap();
        expectedHierarchyFieldOptions.put("K006", "V");
        Assertions.assertEquals(expectedHierarchyFieldOptions, config.getHierarchyFieldOptions());
        Assertions.assertEquals(Lists.newArrayList(DocumentPermutation.class.getName()), config.getDocumentPermutations());
        QueryModel expectedQueryModel = new QueryModel();
        Assertions.assertEquals(expectedQueryModel.getForwardQueryMapping(), config.getQueryModel().getForwardQueryMapping());
        Assertions.assertEquals(expectedQueryModel.getReverseQueryMapping(), config.getQueryModel().getReverseQueryMapping());
        Assertions.assertEquals(expectedQueryModel.getUnevaluatedFields(), config.getQueryModel().getUnevaluatedFields());
        Assertions.assertEquals(Sets.newHashSet(".*", ".*?"), config.getDisallowedRegexPatterns());
        Assertions.assertEquals(visitorFunctionMaxWeight, config.getVisitorFunctionMaxWeight());
        
        // Account for QueryImpl.duplicate() generating a random UUID on the duplicate
        QueryImpl expectedQuery = new QueryImpl();
        expectedQuery.setId(config.getQuery().getId());
        Assertions.assertEquals(expectedQuery, config.getQuery());
        Assertions.assertEquals(Sets.newHashSet("groupFieldA"), config.getGroupFields());
        UniqueFields expectedUniqueFields = new UniqueFields();
        expectedUniqueFields.put("uniqueFieldA", UniqueGranularity.ALL);
        Assertions.assertEquals(expectedUniqueFields, config.getUniqueFields());
        Assertions.assertEquals(Lists.newArrayList("fieldA"), config.getContentFieldNames());
        Assertions.assertEquals(Sets.newHashSet("NoExpansionFieldA"), config.getNoExpansionFields());
    }
    
    @Test
    public void testGetSetDataTypeFilter() {
        String expected = "filterA,filterB";
        Set<String> dataTypeFilters = Sets.newHashSet("filterA", "filterB");
        config.setDatatypeFilter(dataTypeFilters);
        Assertions.assertEquals(expected, config.getDatatypeFilterAsString());
    }
    
    @Test
    public void testGetSetProjectFields() {
        String expected = "projectB,projectA"; // Set ordering.
        Set<String> projectFields = Sets.newHashSet("projectA", "projectB");
        config.setProjectFields(projectFields);
        Assertions.assertEquals(expected, config.getProjectFieldsAsString());
    }
    
    @Test
    public void testGetSetConjunctionsWithinExpression() {
        ShardQueryLogic logic = new ShardQueryLogic();
        boolean expected = true; // Set ordering.
        logic.setEnforceUniqueConjunctionsWithinExpression(true);
        Assertions.assertEquals(expected, logic.getEnforceUniqueConjunctionsWithinExpression());
    }
    
    @Test
    public void testGetSetDisjunctionsWithinExpression() {
        ShardQueryLogic logic = new ShardQueryLogic();
        boolean expected = true; // Set ordering.
        logic.setEnforceUniqueDisjunctionsWithinExpression(true);
        Assertions.assertEquals(expected, logic.getEnforceUniqueDisjunctionsWithinExpression());
    }
    
    @Test
    public void testGetSetBlacklistedFields() {
        String expected = "blacklistA,blacklistB";
        Set<String> blacklistedFields = Sets.newHashSet("blacklistA", "blacklistB");
        config.setBlacklistedFields(blacklistedFields);
        Assertions.assertEquals(expected, config.getBlacklistedFieldsAsString());
    }
    
    @Test
    public void testGetSetIndexedFieldDataTypes() {
        Assertions.assertEquals("", config.getIndexedFieldDataTypesAsString());
        
        Set<String> indexedFields = Sets.newHashSet("fieldA", "fieldB");
        Multimap<String,Type<?>> queryFieldsDatatypes = ArrayListMultimap.create();
        queryFieldsDatatypes.put("fieldA", new DateType());
        queryFieldsDatatypes.put("fieldB", new StringType());
        
        config.setIndexedFields(indexedFields);
        config.setQueryFieldsDatatypes(queryFieldsDatatypes);
        
        String expected = "fieldA:datawave.data.type.DateType;fieldB:datawave.data.type.StringType;";
        Assertions.assertEquals(expected, config.getIndexedFieldDataTypesAsString());
    }
    
    @Test
    public void testGetSetNormalizedFieldNormalizers() {
        Assertions.assertEquals("", config.getNormalizedFieldNormalizersAsString());
        
        Set<String> normalizedFields = Sets.newHashSet("fieldA", "fieldB");
        Multimap<String,Type<?>> normalizedFieldsDatatypes = ArrayListMultimap.create();
        normalizedFieldsDatatypes.put("fieldA", new DateType());
        normalizedFieldsDatatypes.put("fieldB", new StringType());
        
        config.setIndexedFields(normalizedFields);
        config.setNormalizedFieldsDatatypes(normalizedFieldsDatatypes);
        
        String expected = "fieldA:datawave.data.type.DateType;fieldB:datawave.data.type.StringType;";
        Assertions.assertEquals(expected, config.getNormalizedFieldNormalizersAsString());
    }
    
    @Test
    public void testIsTldQuery() {
        Assertions.assertFalse(config.isTldQuery());
        
        config.setTldQuery(true);
        Assertions.assertTrue(config.isTldQuery());
    }
    
    /**
     * This test will fail if a new variable is added improperly to the ShardQueryConfiguration
     *
     */
    @Test
    public void testCheckForNewAdditions() throws IOException {
        int expectedObjectCount = 188;
        ShardQueryConfiguration config = ShardQueryConfiguration.create();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(mapper.writeValueAsString(config));
        Iterator<JsonNode> rootIter = root.elements();
        int objectCount = 0;
        while (rootIter.hasNext()) {
            rootIter.next();
            objectCount++;
        }
        
        Assertions.assertEquals(expectedObjectCount, objectCount, "New variable was added to or removed from the ShardQueryConfiguration");
    }
    
    @Test
    public void whenRetrievingActiveQueryLogName_givenTableNameSource_thenReturnsTableName() {
        ShardQueryConfiguration configuration = new ShardQueryConfiguration();
        configuration.setTableName("shardTable");
        configuration.setActiveQueryLogNameSource(ShardQueryConfiguration.TABLE_NAME_SOURCE);
        Assertions.assertEquals("shardTable", configuration.getActiveQueryLogName());
    }
    
    @Test
    public void whenRetrievingActiveQueryLogName_givenQueryLogicNameSource_thenReturnsQueryLogicName() {
        ShardQueryConfiguration configuration = new ShardQueryConfiguration();
        configuration.setActiveQueryLogNameSource(ShardQueryConfiguration.QUERY_LOGIC_NAME_SOURCE);
        Assertions.assertEquals(ShardQueryConfiguration.class.getSimpleName(), configuration.getActiveQueryLogName());
    }
    
    @Test
    public void whenRetrievingActiveQueryLogName_givenNoActiveQueryLogNameValue_thenReturnsBlankString() {
        ShardQueryConfiguration configuration = new ShardQueryConfiguration();
        Assertions.assertEquals("", configuration.getActiveQueryLogName());
    }
    
    @Test
    public void whenRetrievingActiveQueryLogName_givenOtherValue_thenReturnsBlankString() {
        ShardQueryConfiguration configuration = new ShardQueryConfiguration();
        configuration.setActiveQueryLogNameSource("nonMatchingValue");
        Assertions.assertEquals("", configuration.getActiveQueryLogName());
    }
}
