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
import datawave.query.function.DocumentPermutation;
import datawave.query.function.DocumentProjection;
import datawave.query.model.QueryModel;
import datawave.query.attributes.UniqueFields;
import datawave.query.attributes.UniqueGranularity;
import datawave.util.TableName;
import datawave.webservice.query.QueryImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    
    private ShardQueryConfiguration config;
    
    @Before
    public void setUp() {
        config = ShardQueryConfiguration.create();
    }
    
    /**
     * Assert expected default values from an empty constructor call
     */
    @Test
    public void testEmptyConstructor() {
        ShardQueryConfiguration config = ShardQueryConfiguration.create();
        
        Assert.assertFalse(config.isTldQuery());
        Assert.assertEquals(Maps.newHashMap(), config.getFilterOptions());
        Assert.assertFalse(config.isDisableIndexOnlyDocuments());
        Assert.assertEquals(1000, config.getMaxScannerBatchSize());
        Assert.assertEquals(1000, config.getMaxIndexBatchSize());
        Assert.assertFalse(config.isAllTermsIndexOnly());
        Assert.assertEquals("", config.getAccumuloPassword());
        Assert.assertEquals(Long.MAX_VALUE, config.getMaxIndexScanTimeMillis());
        Assert.assertFalse(config.getCollapseUids());
        Assert.assertFalse(config.getParseTldUids());
        Assert.assertFalse(config.getSequentialScheduler());
        Assert.assertFalse(config.getCollectTimingDetails());
        Assert.assertFalse(config.getLogTimingDetails());
        Assert.assertTrue(config.getSendTimingToStatsd());
        Assert.assertEquals("localhost", config.getStatsdHost());
        Assert.assertEquals(8125, config.getStatsdPort());
        Assert.assertEquals(500, config.getStatsdMaxQueueSize());
        Assert.assertFalse(config.getLimitAnyFieldLookups());
        Assert.assertFalse(config.isBypassExecutabilityCheck());
        Assert.assertFalse(config.getBackoffEnabled());
        Assert.assertTrue(config.getUnsortedUIDsEnabled());
        Assert.assertFalse(config.getSerializeQueryIterator());
        Assert.assertFalse(config.isDebugMultithreadedSources());
        Assert.assertFalse(config.isSortGeoWaveQueryRanges());
        Assert.assertEquals(0, config.getNumRangesToBuffer());
        Assert.assertEquals(0, config.getRangeBufferTimeoutMillis());
        Assert.assertEquals(100, config.getRangeBufferPollMillis());
        Assert.assertEquals(8, config.getGeometryMaxExpansion());
        Assert.assertEquals(32, config.getPointMaxExpansion());
        Assert.assertEquals(16, config.getGeoWaveRangeSplitThreshold());
        Assert.assertEquals(0.25, config.getGeoWaveMaxRangeOverlap(), 0.0);
        Assert.assertTrue(config.isOptimizeGeoWaveRanges());
        Assert.assertEquals(4, config.getGeoWaveMaxEnvelopes());
        Assert.assertEquals(TableName.SHARD, config.getShardTableName());
        Assert.assertEquals(TableName.SHARD_INDEX, config.getIndexTableName());
        Assert.assertEquals(TableName.SHARD_RINDEX, config.getReverseIndexTableName());
        Assert.assertEquals(TableName.METADATA, config.getMetadataTableName());
        Assert.assertEquals(TableName.DATE_INDEX, config.getDateIndexTableName());
        Assert.assertEquals(TableName.INDEX_STATS, config.getIndexStatsTableName());
        Assert.assertEquals("EVENT", config.getDefaultDateTypeName());
        Assert.assertTrue(config.isCleanupShardsAndDaysQueryHints());
        Assert.assertEquals(new Integer(8), config.getNumQueryThreads());
        Assert.assertEquals(new Integer(8), config.getNumIndexLookupThreads());
        Assert.assertEquals(new Integer(8), config.getNumDateIndexThreads());
        Assert.assertEquals(new Integer(-1), config.getMaxDocScanTimeout());
        Assert.assertNotNull(config.getFstCount());
        Assert.assertEquals(0.99f, config.getCollapseDatePercentThreshold(), 0);
        Assert.assertTrue(config.getFullTableScanEnabled());
        Assert.assertNull(config.getRealmSuffixExclusionPatterns());
        Assert.assertEquals(NoOpType.class, config.getDefaultType());
        Assert.assertEquals(new SimpleDateFormat("yyyyMMdd"), config.getShardDateFormatter());
        Assert.assertFalse(config.getUseEnrichers());
        Assert.assertNull(config.getEnricherClassNames());
        Assert.assertFalse(config.getUseFilters());
        Assert.assertNull(config.getFilterClassNames());
        Assert.assertEquals(Lists.newArrayList(), config.getIndexFilteringClassNames());
        Assert.assertEquals(Sets.newHashSet("d", "tf"), config.getNonEventKeyPrefixes());
        Assert.assertEquals(Sets.newHashSet(), config.getUnevaluatedFields());
        Assert.assertEquals(Sets.newHashSet(), config.getDatatypeFilter());
        Assert.assertEquals(Lists.newArrayList(), config.getIndexHoles());
        Assert.assertEquals(Sets.newHashSet(), config.getProjectFields());
        Assert.assertEquals(Sets.newHashSet(), config.getBlacklistedFields());
        Assert.assertEquals(Sets.newHashSet(), config.getIndexedFields());
        Assert.assertEquals(Sets.newHashSet(), config.getNormalizedFields());
        Assert.assertEquals(HashMultimap.create(), config.getDataTypes());
        Assert.assertEquals(HashMultimap.create(), config.getQueryFieldsDatatypes());
        Assert.assertEquals(HashMultimap.create(), config.getNormalizedFieldsDatatypes());
        Assert.assertEquals(ArrayListMultimap.create(), config.getCompositeToFieldMap());
        Assert.assertEquals(Maps.newHashMap(), config.getFieldToDiscreteIndexTypes());
        Assert.assertEquals(Maps.newHashMap(), config.getCompositeTransitionDates());
        Assert.assertTrue(config.isSortedUIDs());
        Assert.assertEquals(Sets.newHashSet(), config.getQueryTermFrequencyFields());
        Assert.assertFalse(config.isTermFrequenciesRequired());
        Assert.assertEquals(Sets.newHashSet(), config.getLimitFields());
        Assert.assertFalse(config.isLimitFieldsPreQueryEvaluation());
        Assert.assertNull(config.getLimitFieldsField());
        Assert.assertFalse(config.isHitList());
        Assert.assertFalse(config.isDateIndexTimeTravel());
        Assert.assertEquals(-1L, config.getBeginDateCap());
        Assert.assertTrue(config.isFailOutsideValidDateRange());
        Assert.assertFalse(config.isRawTypes());
        Assert.assertEquals(-1.0, config.getMinSelectivity(), 0);
        Assert.assertFalse(config.getIncludeDataTypeAsField());
        Assert.assertTrue(config.getIncludeRecordId());
        Assert.assertFalse(config.getIncludeHierarchyFields());
        Assert.assertEquals(Maps.newHashMap(), config.getHierarchyFieldOptions());
        Assert.assertFalse(config.getIncludeGroupingContext());
        Assert.assertEquals(Lists.newArrayList(), config.getDocumentPermutations());
        Assert.assertTrue(config.getFilterMaskedValues());
        Assert.assertFalse(config.isReducedResponse());
        Assert.assertTrue(config.getAllowShortcutEvaluation());
        Assert.assertFalse(config.getBypassAccumulo());
        Assert.assertFalse(config.getSpeculativeScanning());
        Assert.assertFalse(config.isDisableEvaluation());
        Assert.assertFalse(config.isContainsIndexOnlyTerms());
        Assert.assertFalse(config.isContainsCompositeTerms());
        Assert.assertTrue(config.isAllowFieldIndexEvaluation());
        Assert.assertTrue(config.isAllowTermFrequencyLookup());
        Assert.assertEquals(DocumentSerialization.DEFAULT_RETURN_TYPE, config.getReturnType());
        Assert.assertEquals(10000, config.getEventPerDayThreshold());
        Assert.assertEquals(10, config.getShardsPerDayThreshold());
        Assert.assertEquals(2500, config.getMaxTermThreshold());
        Assert.assertEquals(2500, config.getMaxDepthThreshold());
        Assert.assertEquals(500, config.getMaxUnfieldedExpansionThreshold());
        Assert.assertEquals(5000, config.getMaxValueExpansionThreshold());
        Assert.assertEquals(500, config.getMaxOrExpansionThreshold());
        Assert.assertEquals(10, config.getMaxOrRangeThreshold());
        Assert.assertEquals(10, config.getMaxOrRangeIvarators());
        Assert.assertEquals(5, config.getMaxRangesPerRangeIvarator());
        Assert.assertEquals(750, config.getMaxOrExpansionFstThreshold());
        Assert.assertEquals(Long.MAX_VALUE, config.getYieldThresholdMs());
        Assert.assertNull(config.getHdfsSiteConfigURLs());
        Assert.assertNull(config.getHdfsFileCompressionCodec());
        Assert.assertNull(config.getZookeeperConfig());
        Assert.assertTrue(config.getIvaratorCacheDirConfigs().isEmpty());
        Assert.assertEquals(2, config.getIvaratorNumRetries());
        Assert.assertEquals(100, config.getIvaratorPersistVerifyCount());
        Assert.assertEquals(true, config.isIvaratorPersistVerify());
        Assert.assertNull(config.getIvaratorFstHdfsBaseURIs());
        Assert.assertEquals(10000, config.getIvaratorCacheBufferSize());
        Assert.assertEquals(100000, config.getIvaratorCacheScanPersistThreshold());
        Assert.assertEquals(3600000, config.getIvaratorCacheScanTimeout());
        Assert.assertEquals(11, config.getMaxFieldIndexRangeSplit());
        Assert.assertEquals(100, config.getIvaratorMaxOpenFiles());
        Assert.assertEquals(33, config.getMaxIvaratorSources());
        Assert.assertEquals(25, config.getMaxEvaluationPipelines());
        Assert.assertEquals(25, config.getMaxPipelineCachedResults());
        Assert.assertFalse(config.isExpandAllTerms());
        Assert.assertNull(config.getQueryModel());
        Assert.assertNull(config.getModelName());
        Assert.assertEquals(TableName.METADATA, config.getModelTableName());
        Assert.assertFalse(config.isExpansionLimitedToModelContents());
        Assert.assertEquals(new QueryImpl(), config.getQuery());
        Assert.assertFalse(config.isCompressServerSideResults());
        Assert.assertFalse(config.isIndexOnlyFilterFunctionsEnabled());
        Assert.assertFalse(config.isCompositeFilterFunctionsEnabled());
        Assert.assertEquals(0, config.getGroupFieldsBatchSize());
        Assert.assertFalse(config.getAccrueStats());
        Assert.assertEquals(Sets.newHashSet(), config.getGroupFields());
        Assert.assertEquals(new UniqueFields(), config.getUniqueFields());
        Assert.assertFalse(config.getCacheModel());
        Assert.assertTrue(config.isTrackSizes());
        Assert.assertEquals(Lists.newArrayList(), config.getContentFieldNames());
        Assert.assertNull(config.getActiveQueryLogNameSource());
        Assert.assertEquals("", config.getActiveQueryLogName());
        Assert.assertFalse(config.isDisableWhindexFieldMappings());
        Assert.assertEquals(Sets.newHashSet(), config.getWhindexMappingFields());
        Assert.assertEquals(Maps.newHashMap(), config.getWhindexFieldMappings());
        Assert.assertEquals(Collections.emptySet(), config.getNoExpansionFields());
        Assert.assertEquals(Sets.newHashSet(".*", ".*?"), config.getDisallowedRegexPatterns());
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
        Assert.assertEquals(Lists.newArrayList("somePattern"), config.getRealmSuffixExclusionPatterns());
        Assert.assertEquals(new SimpleDateFormat("yyyyMMdd"), config.getShardDateFormatter());
        Assert.assertEquals(Lists.newArrayList("enricherClassNameA"), config.getEnricherClassNames());
        Assert.assertEquals(Lists.newArrayList("filterClassNameA"), config.getFilterClassNames());
        Assert.assertEquals(Lists.newArrayList("indexFilteringClassNameA"), config.getIndexFilteringClassNames());
        Assert.assertEquals(Sets.newHashSet("nonEventKeyPrefixA"), config.getNonEventKeyPrefixes());
        Assert.assertEquals(Sets.newHashSet("unevaluatedFieldA"), config.getUnevaluatedFields());
        Assert.assertEquals(Sets.newHashSet("dataTypeFilterA"), config.getDatatypeFilter());
        IndexHole expectedIndexHole = new IndexHole(new String[] {"0", "1"}, new String[] {"2", "3"});
        Assert.assertEquals(Lists.newArrayList(expectedIndexHole), config.getIndexHoles());
        Assert.assertEquals(Sets.newHashSet("projectFieldA"), config.getProjectFields());
        Assert.assertEquals(Sets.newHashSet("blacklistedFieldA"), config.getBlacklistedFields());
        Assert.assertEquals(Sets.newHashSet("indexedFieldA"), config.getIndexedFields());
        // This assert is different from the setter as setNormalizedFieldsAsDatatypes will overwrite the normalizedFields with
        // a new keyset.
        Assert.assertEquals(Sets.newHashSet("K003"), config.getNormalizedFields());
        Multimap<String,Type<?>> expectedDataTypes = HashMultimap.create();
        expectedDataTypes.put("K001", new NoOpType("V"));
        Assert.assertEquals(expectedDataTypes, config.getDataTypes());
        Multimap<String,Type<?>> expectedQueryFieldsDatatypes = HashMultimap.create();
        expectedQueryFieldsDatatypes.put("K002", new NoOpType("V"));
        Assert.assertEquals(expectedQueryFieldsDatatypes, config.getQueryFieldsDatatypes());
        Multimap<String,Type<?>> expectedNormalizedFieldsDatatypes = HashMultimap.create();
        expectedNormalizedFieldsDatatypes.put("K003", new NoOpType("V"));
        Assert.assertEquals(expectedNormalizedFieldsDatatypes, config.getNormalizedFieldsDatatypes());
        Multimap<String,String> expectedCompositeToFieldMap = ArrayListMultimap.create();
        expectedCompositeToFieldMap.put("K004", "V");
        Assert.assertEquals(expectedCompositeToFieldMap, config.getCompositeToFieldMap());
        Map<String,DiscreteIndexType<?>> expectedFieldToDiscreteIndexType = Maps.newHashMap();
        expectedFieldToDiscreteIndexType.put("GEO", new GeometryType());
        Assert.assertEquals(expectedFieldToDiscreteIndexType, config.getFieldToDiscreteIndexTypes());
        Map<String,Date> expectedCompositeTransitionDates = Maps.newHashMap();
        expectedCompositeTransitionDates.put("K005", transitionDate);
        Assert.assertEquals(expectedCompositeTransitionDates, config.getCompositeTransitionDates());
        Map<String,String> expectedCompositeFieldSeparators = Maps.newHashMap();
        expectedCompositeFieldSeparators.put("GEO", " ");
        Assert.assertEquals(expectedCompositeFieldSeparators, config.getCompositeFieldSeparators());
        Assert.assertEquals(Sets.newHashSet("fieldA"), config.getQueryTermFrequencyFields());
        Assert.assertEquals(Sets.newHashSet("limitFieldA"), config.getLimitFields());
        Map<String,String> expectedHierarchyFieldOptions = Maps.newHashMap();
        expectedHierarchyFieldOptions.put("K006", "V");
        Assert.assertEquals(expectedHierarchyFieldOptions, config.getHierarchyFieldOptions());
        Assert.assertEquals(Lists.newArrayList(DocumentPermutation.class.getName()), config.getDocumentPermutations());
        QueryModel expectedQueryModel = new QueryModel();
        Assert.assertEquals(expectedQueryModel.getForwardQueryMapping(), config.getQueryModel().getForwardQueryMapping());
        Assert.assertEquals(expectedQueryModel.getReverseQueryMapping(), config.getQueryModel().getReverseQueryMapping());
        Assert.assertEquals(expectedQueryModel.getUnevaluatedFields(), config.getQueryModel().getUnevaluatedFields());
        Assert.assertEquals(Sets.newHashSet(".*", ".*?"), config.getDisallowedRegexPatterns());
        
        // Account for QueryImpl.duplicate() generating a random UUID on the duplicate
        QueryImpl expectedQuery = new QueryImpl();
        expectedQuery.setId(config.getQuery().getId());
        Assert.assertEquals(expectedQuery, config.getQuery());
        Assert.assertEquals(Sets.newHashSet("groupFieldA"), config.getGroupFields());
        UniqueFields expectedUniqueFields = new UniqueFields();
        expectedUniqueFields.put("uniqueFieldA", UniqueGranularity.ALL);
        Assert.assertEquals(expectedUniqueFields, config.getUniqueFields());
        Assert.assertEquals(Lists.newArrayList("fieldA"), config.getContentFieldNames());
        Assert.assertEquals(Sets.newHashSet("NoExpansionFieldA"), config.getNoExpansionFields());
    }
    
    @Test
    public void testGetSetDataTypeFilter() {
        String expected = "filterA,filterB";
        Set<String> dataTypeFilters = Sets.newHashSet("filterA", "filterB");
        config.setDatatypeFilter(dataTypeFilters);
        Assert.assertEquals(expected, config.getDatatypeFilterAsString());
    }
    
    @Test
    public void testGetSetProjectFields() {
        String expected = "projectB,projectA"; // Set ordering.
        Set<String> projectFields = Sets.newHashSet("projectA", "projectB");
        config.setProjectFields(projectFields);
        Assert.assertEquals(expected, config.getProjectFieldsAsString());
    }
    
    @Test
    public void testGetSetBlacklistedFields() {
        String expected = "blacklistA,blacklistB";
        Set<String> blacklistedFields = Sets.newHashSet("blacklistA", "blacklistB");
        config.setBlacklistedFields(blacklistedFields);
        Assert.assertEquals(expected, config.getBlacklistedFieldsAsString());
    }
    
    @Test
    public void testGetSetIndexedFieldDataTypes() {
        Assert.assertEquals("", config.getIndexedFieldDataTypesAsString());
        
        Set<String> indexedFields = Sets.newHashSet("fieldA", "fieldB");
        Multimap<String,Type<?>> queryFieldsDatatypes = ArrayListMultimap.create();
        queryFieldsDatatypes.put("fieldA", new DateType());
        queryFieldsDatatypes.put("fieldB", new StringType());
        
        config.setIndexedFields(indexedFields);
        config.setQueryFieldsDatatypes(queryFieldsDatatypes);
        
        String expected = "fieldA:datawave.data.type.DateType;fieldB:datawave.data.type.StringType;";
        Assert.assertEquals(expected, config.getIndexedFieldDataTypesAsString());
    }
    
    @Test
    public void testGetSetNormalizedFieldNormalizers() {
        Assert.assertEquals("", config.getNormalizedFieldNormalizersAsString());
        
        Set<String> normalizedFields = Sets.newHashSet("fieldA", "fieldB");
        Multimap<String,Type<?>> normalizedFieldsDatatypes = ArrayListMultimap.create();
        normalizedFieldsDatatypes.put("fieldA", new DateType());
        normalizedFieldsDatatypes.put("fieldB", new StringType());
        
        config.setIndexedFields(normalizedFields);
        config.setNormalizedFieldsDatatypes(normalizedFieldsDatatypes);
        
        String expected = "fieldA:datawave.data.type.DateType;fieldB:datawave.data.type.StringType;";
        Assert.assertEquals(expected, config.getNormalizedFieldNormalizersAsString());
    }
    
    @Test
    public void testIsTldQuery() {
        Assert.assertFalse(config.isTldQuery());
        
        config.setTldQuery(true);
        Assert.assertTrue(config.isTldQuery());
    }
    
    /**
     * This test will fail if a new variable is added improperly to the ShardQueryConfiguration
     *
     * @throws IOException
     */
    @Test
    public void testCheckForNewAdditions() throws IOException {
        int expectedObjectCount = 182;
        ShardQueryConfiguration config = ShardQueryConfiguration.create();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(mapper.writeValueAsString(config));
        Iterator<JsonNode> rootIter = root.elements();
        int objectCount = 0;
        while (rootIter.hasNext()) {
            rootIter.next();
            objectCount++;
        }
        
        Assert.assertEquals("New variable was added to or removed from the ShardQueryConfiguration", expectedObjectCount, objectCount);
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
