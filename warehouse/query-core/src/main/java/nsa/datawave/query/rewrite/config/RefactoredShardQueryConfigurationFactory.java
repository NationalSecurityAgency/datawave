package nsa.datawave.query.rewrite.config;

import nsa.datawave.query.QueryParameters;
import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.rewrite.tables.RefactoredShardQueryLogic;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.QueryImpl;

import java.util.Arrays;
import java.util.Set;

/**
 * Temporary replacement of RefactoredShardQueryConfiguration(RefactoredShardQueryLogic configuredLogic, Query query) constructor to break circular dependency
 * of config & logic.
 *
 */
public class RefactoredShardQueryConfigurationFactory {
    
    public static RefactoredShardQueryConfiguration createRefactoredShardQueryConfigurationFromConfiguredLogic(RefactoredShardQueryLogic configuredLogic,
                    Query query) {
        
        RefactoredShardQueryConfiguration config = new RefactoredShardQueryConfiguration();
        
        // normally set via super constructor
        config.setTableName(configuredLogic.getTableName());
        config.setMaxQueryResults(configuredLogic.getMaxResults());
        config.setMaxRowsToScan(configuredLogic.getMaxRowsToScan());
        config.setUndisplayedVisibilities(configuredLogic.getUndisplayedVisibilities());
        config.setBaseIteratorPriority(configuredLogic.getBaseIteratorPriority());
        
        // General query options
        if (-1 == configuredLogic.getMaxResults()) {
            config.setMaxQueryResults(Long.MAX_VALUE);
        } else {
            config.setMaxQueryResults(configuredLogic.getMaxResults());
        }
        
        config.setMaxRowsToScan(configuredLogic.getMaxRowsToScan());
        config.setNumQueryThreads(configuredLogic.getQueryThreads());
        config.setDefaultType(configuredLogic.getDefaultType());
        config.setNumIndexLookupThreads(configuredLogic.getIndexLookupThreads());
        config.setNumDateIndexThreads(configuredLogic.getDateIndexThreads());
        config.setCollapseDatePercentThreshold(configuredLogic.getCollapseDatePercentThreshold());
        // Table names
        config.setShardTableName(configuredLogic.getTableName());
        config.setIndexTableName(configuredLogic.getIndexTableName());
        config.setReverseIndexTableName(configuredLogic.getReverseIndexTableName());
        config.setMetadataTableName(configuredLogic.getMetadataTableName());
        config.setCleanupShardsAndDaysQueryHints(configuredLogic.isCleanupShardsAndDaysQueryHints());
        config.setDateIndexTableName(configuredLogic.getDateIndexTableName());
        config.setQueryModel(configuredLogic.getQueryModel());
        config.setLimitAnyFieldLookups(configuredLogic.getLimitAnyFieldLookups());
        config.setModelTableName(configuredLogic.getModelTableName());
        config.setCacheModel(configuredLogic.getCacheModel());
        config.setIndexStatsTableName(configuredLogic.getIndexStatsTableName());
        
        config.setIndexHoles(configuredLogic.getIndexHoles());
        
        config.setSequentialScheduler(configuredLogic.getSequentialScheduler());
        // Enrichment properties
        config.setUseEnrichers(configuredLogic.isUseEnrichers());
        config.setEnricherClassNames(configuredLogic.getEnricherClassNames());
        
        // is config a tld query logic
        config.setTldQuery(configuredLogic.isTldQuery());
        
        // Filter properties
        config.setUseFilters(configuredLogic.isUseFilters());
        config.setFilterClassNames(configuredLogic.getFilterClassNames());
        config.setIndexFilteringClassNames(configuredLogic.getIndexFilteringClassNames());
        config.putFilterOptions(configuredLogic.getFilterOptions());
        
        config.setFullTableScanEnabled(configuredLogic.isFullTableScanEnabled());
        config.setRealmSuffixExclusionPatterns(configuredLogic.getRealmSuffixExclusionPatterns());
        config.setNonEventKeyPrefixes(Arrays.asList(nsa.datawave.util.StringUtils.split(configuredLogic.getNonEventKeyColFams(), Constants.PARAM_VALUE_SEP)));
        config.setUnevaluatedFields(configuredLogic.getUnevaluatedFields());
        config.setMinSelectivity(configuredLogic.getMinimumSelectivity());
        
        config.setFilterMaskedValues(configuredLogic.getFilterMaskedValues());
        
        config.setIncludeDataTypeAsField(configuredLogic.getIncludeDataTypeAsField());
        config.setIncludeHierarchyFields(configuredLogic.getIncludeHierarchyFields());
        config.setHierarchyFieldOptions(configuredLogic.getHierarchyFieldOptions());
        config.setBlacklistedFields(configuredLogic.getBlacklistedFields());
        
        // ShardEventEvaluatingIterator options
        config.setIncludeGroupingContext(configuredLogic.getIncludeGroupingContext());
        
        // Pass down the RangeCalculator options
        config.setEventPerDayThreshold(configuredLogic.getEventPerDayThreshold());
        config.setShardsPerDayThreshold(configuredLogic.getShardsPerDayThreshold());
        config.setMaxTermThreshold(configuredLogic.getMaxTermThreshold());
        config.setMaxDepthThreshold(configuredLogic.getMaxDepthThreshold());
        config.setMaxUnfieldedExpansionThreshold(configuredLogic.getMaxUnfieldedExpansionThreshold());
        config.setMaxValueExpansionThreshold(configuredLogic.getMaxValueExpansionThreshold());
        config.setMaxOrExpansionThreshold(configuredLogic.getMaxOrExpansionThreshold());
        config.setMaxOrExpansionFstThreshold(configuredLogic.getMaxOrExpansionFstThreshold());
        
        config.setYieldThresholdMs(configuredLogic.getYieldThresholdMs());
        
        config.setExpandAllTerms(configuredLogic.isExpandAllTerms());
        
        config.setHdfsSiteConfigURLs(configuredLogic.getHdfsSiteConfigURLs());
        config.setHdfsFileCompressionCodec(configuredLogic.getHdfsFileCompressionCodec());
        config.setZookeeperConfig(configuredLogic.getZookeeperConfig());
        
        config.setIvaratorCacheBaseURIs(configuredLogic.getIvaratorCacheBaseURIs());
        config.setIvaratorFstHdfsBaseURIs(configuredLogic.getIvaratorFstHdfsBaseURIs());
        config.setIvaratorCacheBufferSize(configuredLogic.getIvaratorCacheBufferSize());
        config.setIvaratorCacheScanPersistThreshold(configuredLogic.getIvaratorCacheScanPersistThreshold());
        config.setIvaratorCacheScanTimeout(configuredLogic.getIvaratorCacheScanTimeout());
        
        config.setMaxFieldIndexRangeSplit(configuredLogic.getMaxFieldIndexRangeSplit());
        config.setIvaratorMaxOpenFiles(configuredLogic.getIvaratorMaxOpenFiles());
        config.setMaxIvaratorSources(configuredLogic.getMaxIvaratorSources());
        config.setMaxEvaluationPipelines(configuredLogic.getMaxEvaluationPipelines());
        config.setMaxPipelineCachedResults(configuredLogic.getMaxPipelineCachedResults());
        
        config.setReducedResponse(configuredLogic.isReducedResponse());
        config.setDisableEvaluation(configuredLogic.isDisableEvaluation());
        config.setDisableIndexOnlyDocuments(configuredLogic.disableIndexOnlyDocuments());
        config.setHitList(configuredLogic.isHitList());
        config.setTypeMetadataInHdfs(configuredLogic.isTypeMetadataInHdfs());
        config.setCompressServerSideResults(configuredLogic.isCompressServerSideResults());
        
        // Allow index-only JEXL functions, which can potentially use a huge
        // amount of memory, to be turned on or off
        config.setIndexOnlyFilterFunctionsEnabled(configuredLogic.isIndexOnlyFilterFunctionsEnabled());
        
        config.setAccrueStats(configuredLogic.getAccrueStats());
        
        config.setLimitFields(configuredLogic.getLimitFields());
        config.setGroupFields(configuredLogic.getGroupFields());
        config.setAccumuloPassword(configuredLogic.getAccumuloPassword());
        
        config.setCollapseUids(configuredLogic.getCollapseUids());
        
        config.setMaxIndexScanTimeMillis(configuredLogic.getMaxIndexScanTimeMillis());
        config.setMaxDocScanTimeout(configuredLogic.getMaxDocScanTimeout());
        config.setAccrueStats(configuredLogic.getAccrueStats());
        config.setMaxScannerBatchSize(configuredLogic.getMaxScannerBatchSize());
        config.setMaxIndexBatchSize(configuredLogic.getMaxIndexBatchSize());
        
        config.setAllowShortcutEvaluation(configuredLogic.getAllowShortcutEvaluation());
        config.setAllowFieldIndexEvaluation(configuredLogic.isAllowFieldIndexEvaluation());
        config.setBypassAccumulo(configuredLogic.isBypassAccumulo());
        config.setSpeculativeScanning(configuredLogic.getSpeculativeScanning());
        config.setLimitFields(configuredLogic.getLimitFields());
        config.setBackoffEnabled(configuredLogic.getBackoffEnabled());
        config.setUnsortedUIDsEnabled(configuredLogic.getUnsortedUIDsEnabled());
        config.setQuery(query);
        Set<QueryImpl.Parameter> parameterSet = query.getParameters();
        for (QueryImpl.Parameter parameter : parameterSet) {
            String name = parameter.getParameterName();
            String value = parameter.getParameterValue();
            if (name.equals(QueryParameters.HIT_LIST)) {
                config.setHitList(Boolean.parseBoolean(value));
            }
            if (name.equals(QueryParameters.DATE_INDEX_TIME_TRAVEL)) {
                config.setDateIndexTimeTravel(Boolean.parseBoolean(value));
            }
        }
        
        config.setBeginDateCap(configuredLogic.getBeginDateCap());
        config.setFailOutsideValidDateRange(configuredLogic.isFailOutsideValidDateRange());
        
        config.setQuery(query);
        
        config.setDebugMultithreadedSources(configuredLogic.isDebugMultithreadedSources());
        
        config.setCollectTimingDetails(configuredLogic.getCollectTimingDetails());
        config.setLogTimingDetails(configuredLogic.getLogTimingDetails());
        config.setSendTimingToStatsd(configuredLogic.getSendTimingToStatsd());
        config.setStatsdHost(configuredLogic.getStatsdHost());
        config.setStatsdPort(configuredLogic.getStatsdPort());
        config.setStatsdMaxQueueSize(configuredLogic.getStatsdMaxQueueSize());
        
        return config;
    }
}
