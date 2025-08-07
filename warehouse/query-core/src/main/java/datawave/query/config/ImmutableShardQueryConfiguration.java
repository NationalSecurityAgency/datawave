package datawave.query.config;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;

import com.google.common.collect.Multimap;
import com.google.common.hash.BloomFilter;

import datawave.core.query.configuration.ImmutableGenericQueryConfiguration;
import datawave.data.type.DiscreteIndexType;
import datawave.data.type.Type;
import datawave.next.scanner.DocumentScannerConfig;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.ExcerptFields;
import datawave.query.attributes.SummaryOptions;
import datawave.query.attributes.UniqueFields;
import datawave.query.common.grouping.GroupFields;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.model.QueryModel;
import datawave.query.util.QueryStopwatch;

public interface ImmutableShardQueryConfiguration extends ImmutableGenericQueryConfiguration {
    String getShardTableName();

    String getMetadataTableName();

    String getDateIndexTableName();

    String getDefaultDateTypeName();

    String getIndexTableName();

    String getReverseIndexTableName();

    String getIndexStatsTableName();

    Integer getNumQueryThreads();

    Integer getNumIndexLookupThreads();

    Integer getNumDateIndexThreads();

    Integer getMaxDocScanTimeout();

    float getCollapseDatePercentThreshold();

    Boolean getFullTableScanEnabled();

    String getShardDateFormat();

    SimpleDateFormat getShardDateFormatter();

    Set<String> getDatatypeFilter();

    String getDatatypeFilterAsString();

    Set<String> getProjectFields();

    String getProjectFieldsAsString();

    Set<String> getRenameFields();

    Set<String> getDisallowlistedFields();

    String getDisallowlistedFieldsAsString();

    Boolean getUseEnrichers();

    List<String> getEnricherClassNames();

    boolean isTldQuery();

    boolean isDebugMultithreadedSources();

    boolean isSortGeoWaveQueryRanges();

    int getNumRangesToBuffer();

    long getRangeBufferTimeoutMillis();

    long getRangeBufferPollMillis();

    int getGeometryMaxExpansion();

    int getPointMaxExpansion();

    int getGeoMaxExpansion();

    int getGeoWaveRangeSplitThreshold();

    double getGeoWaveMaxRangeOverlap();

    boolean isOptimizeGeoWaveRanges();

    int getGeoWaveMaxEnvelopes();

    Boolean getUseFilters();

    Map<String,String> getFilterOptions();

    List<String> getFilterClassNames();

    String getFieldRuleClassName();

    List<String> getIndexFilteringClassNames();

    Class<? extends Type<?>> getDefaultType();

    Set<String> getNonEventKeyPrefixes();

    String getNonEventKeyPrefixesAsString();

    Set<String> getUnevaluatedFields();

    @Deprecated(since = "7.1.0", forRemoval = true)
    int getEventPerDayThreshold();

    @Deprecated(since = "7.1.0", forRemoval = true)
    int getShardsPerDayThreshold();

    int getInitialMaxTermThreshold();

    int getIntermediateMaxTermThreshold();

    int getIndexedMaxTermThreshold();

    int getFinalMaxTermThreshold();

    int getMaxDepthThreshold();

    boolean isExpandFields();

    int getMaxUnfieldedExpansionThreshold();

    boolean isExpandValues();

    int getMaxValueExpansionThreshold();

    int getMaxScannerBatchSize();

    int getMaxIndexBatchSize();

    int getMaxOrExpansionThreshold();

    int getMaxOrRangeThreshold();

    int getMaxOrRangeIvarators();

    int getMaxRangesPerRangeIvarator();

    int getMaxOrExpansionFstThreshold();

    String getHdfsSiteConfigURLs();

    String getHdfsFileCompressionCodec();

    String getZookeeperConfig();

    List<IvaratorCacheDirConfig> getIvaratorCacheDirConfigs();

    List<IvaratorCacheDirConfig> getLocalIvaratorCacheDirConfigs();

    String getIvaratorFstHdfsBaseURIs();

    int getUniqueCacheBufferSize();

    int getIvaratorCacheBufferSize();

    long getIvaratorCacheScanPersistThreshold();

    long getIvaratorCacheScanTimeout();

    int getMaxFieldIndexRangeSplit();

    int getIvaratorMaxOpenFiles();

    int getIvaratorNumRetries();

    boolean isIvaratorPersistVerify();

    int getIvaratorPersistVerifyCount();

    int getMaxIvaratorSources();

    long getMaxIvaratorSourceWait();

    long getMaxIvaratorResults();

    int getMaxIvaratorTerms();

    int getMaxEvaluationPipelines();

    int getMaxPipelineCachedResults();

    boolean isExpandAllTerms();

    String getIndexedFieldDataTypesAsString();

    String getNormalizedFieldNormalizersAsString();

    Set<String> getIndexedFields();

    Set<String> getReverseIndexedFields();

    Set<String> getNormalizedFields();

    Multimap<String,Type<?>> getDataTypes();

    Multimap<String,Type<?>> getQueryFieldsDatatypes();

    Map<String,DiscreteIndexType<?>> getFieldToDiscreteIndexTypes();

    Multimap<String,String> getCompositeToFieldMap();

    Map<String,Date> getCompositeTransitionDates();

    Map<String,String> getCompositeFieldSeparators();

    Map<String,Date> getWhindexCreationDates();

    Multimap<String,Type<?>> getNormalizedFieldsDatatypes();

    Set<String> getLimitFields();

    String getLimitFieldsAsString();

    Set<String> getMatchingFieldSets();

    String getMatchingFieldSetsAsString();

    boolean isLimitFieldsPreQueryEvaluation();

    String getLimitFieldsField();

    boolean isDateIndexTimeTravel();

    boolean isDateIndexIterator();

    boolean getIgnoreNonExistentFields();

    long getBeginDateCap();

    boolean isFailOutsideValidDateRange();

    int getGroupFieldsBatchSize();

    String getGroupFieldsBatchSizeAsString();

    boolean isDisableIteratorUniqueFields();

    UniqueFields getUniqueFields();

    boolean isHitList();

    boolean isRawTypes();

    double getMinSelectivity();

    boolean canRunQuery();

    boolean getFilterMaskedValues();

    boolean getIncludeDataTypeAsField();

    boolean getIncludeRecordId();

    boolean getIncludeHierarchyFields();

    Map<String,String> getHierarchyFieldOptions();

    boolean getIncludeGroupingContext();

    List<String> getDocumentPermutations();

    boolean isReducedResponse();

    boolean isDisableEvaluation();

    boolean isDisableIndexOnlyDocuments();

    boolean isContainsIndexOnlyTerms();

    boolean isContainsCompositeTerms();

    boolean isAllowFieldIndexEvaluation();

    boolean isAllowTermFrequencyLookup();

    boolean isExpandUnfieldedNegations();

    boolean isAllTermsIndexOnly();

    QueryModel getQueryModel();

    String getModelName();

    String getModelTableName();

    DocumentSerialization.ReturnType getReturnType();

    QueryStopwatch getTimers();

    ASTJexlScript getQueryTree();

    String getQueryString();

    boolean isCompressServerSideResults();

    boolean isIndexOnlyFilterFunctionsEnabled();

    boolean isCompositeFilterFunctionsEnabled();

    List<String> getRealmSuffixExclusionPatterns();

    Set<String> getQueryTermFrequencyFields();

    boolean isTermFrequenciesRequired();

    boolean isLimitTermExpansionToModel();

    long getMaxIndexScanTimeMillis();

    boolean getParseTldUids();

    boolean getCollapseUids();

    int getCollapseUidsThreshold();

    boolean getEnforceUniqueTermsWithinExpressions();

    boolean getPruneQueryByIngestTypes();

    boolean getReduceQueryFields();

    boolean getReduceQueryFieldsPerShard();

    boolean getReduceTypeMetadata();

    boolean getReduceTypeMetadataPerShard();

    boolean getLimitAnyFieldLookups();

    boolean getAllowShortcutEvaluation();

    boolean getAccrueStats();

    List<IndexValueHole> getIndexValueHoles();

    boolean getCollectTimingDetails();

    boolean getLogTimingDetails();

    String getStatsdHost();

    int getStatsdPort();

    int getStatsdMaxQueueSize();

    boolean getSendTimingToStatsd();

    boolean isCleanupShardsAndDaysQueryHints();

    AtomicInteger getFstCount();

    boolean getCacheModel();

    boolean isBypassExecutabilityCheck();

    boolean getBackoffEnabled();

    boolean getUnsortedUIDsEnabled();

    boolean getSpeculativeScanning();

    boolean getSerializeQueryIterator();

    boolean isSortedUIDs();

    long getYieldThresholdMs();

    boolean isTrackSizes();

    List<String> getContentFieldNames();

    Set<String> getEvaluationOnlyFields();

    Set<String> getDisallowedRegexPatterns();

    String getActiveQueryLogNameSource();

    String getActiveQueryLogName();

    boolean isDisableWhindexFieldMappings();

    Set<String> getWhindexMappingFields();

    Map<String,Map<String,String>> getWhindexFieldMappings();

    boolean isGeneratePlanOnly();

    boolean getEnforceUniqueConjunctionsWithinExpression();

    boolean getEnforceUniqueDisjunctionsWithinExpression();

    BloomFilter<byte[]> getBloom();

    Set<String> getNoExpansionFields();

    Set<String> getLenientFields();

    Set<String> getStrictFields();

    ExcerptFields getExcerptFields();

    Class<? extends SortedKeyValueIterator<Key,Value>> getExcerptIterator();

    SummaryOptions getSummaryOptions();

    Class<? extends SortedKeyValueIterator<Key,Value>> getSummaryIterator();

    String getSummaryFieldName();

    int getFiFieldSeek();

    int getFiNextSeek();

    int getEventFieldSeek();

    int getEventNextSeek();

    int getTfFieldSeek();

    int getTfNextSeek();

    boolean isSeekingEventAggregation();

    long getVisitorFunctionMaxWeight();

    long getQueryExecutionForPageTimeout();

    boolean isLazySetMechanismEnabled();

    int getDocAggregationThresholdMs();

    int getTfAggregationThresholdMs();

    GroupFields getGroupFields();

    boolean getPruneQueryOptions();

    boolean isRebuildDatatypeFilter();

    boolean isRebuildDatatypeFilterPerShard();

    double getIndexFieldHoleMinThreshold();

    boolean getReduceIngestTypes();

    boolean getReduceIngestTypesPerShard();

    boolean isSortQueryPreIndexWithImpliedCounts();

    boolean isSortQueryPreIndexWithFieldCounts();

    boolean isSortQueryPostIndexWithFieldCounts();

    boolean isSortQueryPostIndexWithTermCounts();

    int getCardinalityThreshold();

    boolean isUseQueryTreeScanHintRules();

    List<ScanHintRule<JexlNode>> getQueryTreeScanHintRules();

    long getMaxAnyFieldScanTimeMillis();

    Set<String> getNoExpansionIfCurrentDateTypes();

    DocumentScannerConfig getDocumentScannerConfig();

    boolean isUseDocumentScheduler();

    int getMaxLinesToPrint();

    boolean canHandleExceededValueThreshold();

    boolean canHandleExceededTermThreshold();

}
