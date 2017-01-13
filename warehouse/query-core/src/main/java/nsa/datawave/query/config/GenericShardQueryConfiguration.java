package nsa.datawave.query.config;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nsa.datawave.data.type.NoOpType;
import nsa.datawave.data.type.Type;
import nsa.datawave.query.parser.DatawaveTreeNode;
import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.tables.ShardQueryLogic;
import nsa.datawave.query.tables.shard.ShardQueryTable;
import nsa.datawave.util.StringUtils;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.QueryImpl;
import nsa.datawave.webservice.query.configuration.GenericQueryConfiguration;
import nsa.datawave.webservice.query.configuration.QueryData;

import org.apache.accumulo.core.data.Range;
import org.apache.log4j.Logger;

/**
 * <p>
 * A GenericQueryConfiguration implementation that provides the additional logic on top of the traditional query that is needed to run a DATAWAVE sharded
 * boolean-logic query
 * </p>
 *
 * <p>
 * Provides support for normalizers, enricher classes, filter classes, projection, and datatype filters, in addition to additional parameters also exposed in
 * the Webservice QueryTable interface
 * </p>
 *
 * <p>
 * This class can be initialized with an instance of a ShardQueryLogic or ShardQueryTable which will grab the already configured parameters from the Accumulo
 * Webservice QueryTable and apply them to this configuration object
 * </p>
 *
 * 
 * @deprecated replaced by {@link RefactoredShardQueryConfiguration}
 */
@Deprecated
public abstract class GenericShardQueryConfiguration extends GenericQueryConfiguration {
    private static final Logger log = Logger.getLogger(GenericShardQueryConfiguration.class);
    private static final String EMPTY_STR = "";
    public static final char PARAM_VALUE_SEP = ',';
    public static final String PARAM_VALUE_SEP_STR = new String(new char[] {PARAM_VALUE_SEP});
    
    private String evaluationQuery = EMPTY_STR;
    private String fieldIndexQuery = EMPTY_STR;
    
    private Collection<Range> ranges = Collections.emptySet();
    
    private String shardTableName = "shard";
    private String indexTableName = "shardIndex";
    private String reverseIndexTableName = "shardReverseIndex";
    private String metadataTableName = "DatawaveMetadata";
    
    // BatchScanner and query results options
    private Integer numQueryThreads = 100;
    
    // Do we drop to a full-table scan if the query is not "optimized"
    private Boolean fullTableScanEnabled = false;
    
    // A default normalizer to use
    private Class<? extends Type<?>> defaultDataType = NoOpType.class;
    
    private Collection<Type<?>> dataTypes = Collections.emptyList();
    
    private SimpleDateFormat shardDateFormatter = new SimpleDateFormat("yyyyMMdd");
    
    // Enrichment properties
    private Boolean useEnrichers = true;
    private List<String> enricherClassNames = Collections.singletonList("nsa.datawave.query.enrich.DatawaveTermFrequencyEnricher");
    
    // Filter properties
    private Boolean useFilters = true;
    private List<String> filterClassNames = Collections.singletonList("nsa.datawave.query.filter.DatawavePhraseFilter");
    
    // ReadAheadIterator properties
    private Boolean useReadAheadIterator = false;
    private Integer readAheadQueueSize = 10;
    private Integer readAheadTimeOut = 120;
    
    // Used for ignoring 'd' and 'tf' column family in `shard`
    private Set<String> nonEventKeyPrefixes = new HashSet<>(Arrays.asList("d", "tf"));
    
    // Default to having no unevaluatedFields
    private Set<String> unevaluatedFields = Collections.emptySet();
    
    // Filter results on datatypes
    private Set<String> datatypeFilter = Collections.emptySet();
    
    // Limit fields returned per event
    private Set<String> projectFields = Collections.emptySet();
    private Set<String> blacklistedFields = new HashSet<>(0);
    
    private Multimap<String,Type<?>> indexedFieldsDataTypes = null;
    
    // Used to choose how "selective" a term is (indexStats)
    private double minSelectivity = -1.0;
    
    // Used to add the event datatype to the event as an event field.
    private boolean includeDataTypeAsField = false;
    
    // Used to add event hierarchy info to the event as fields such as CHILD_COUNT and PARENT_UID
    private boolean includeHierarchyFields = false;
    
    // Used to set the ShardEventEvaluating iterator INCLUDE_GROUPING_CONTEXT
    private boolean includeGroupingContext = false;
    
    // Threshold values used in the new RangeCalculator
    private int eventPerDayThreshold = 10000;
    private int shardsPerDayThreshold = 10;
    private int maxTermThreshold = 2500;
    private int rangeExpansionThreshold = 1000;
    private int maxTermExpansionThreshold = 5000;
    
    private DatawaveTreeNode queryTree = null;
    
    // This is a UidMapper class that is used by the ShardUidMappingIterator and the GlobalIndexUidMappingIterator to
    // remap the underlying key/value uids. The use case is to set the TopLevelDocumentUidMapper which maps all
    // children uids to the top level uid allowing queries to be applied to the entire document tree.
    private String uidMapperClass = null;
    
    // This is a UidMapper class that is used by the evaluating iterator to return an alternate document from the one
    // that was evaluated. The use case is to set the ParentDocumentUidMapper which will allow us to query documents
    // and to return the parent documents instead of the ones that were hit by the query.
    private String returnUidMapperClass = null;
    
    private Query query = null;
    
    public GenericShardQueryConfiguration() {
        this.query = new QueryImpl();
    }
    
    /**
     * Automatically configure this class with a ShardQueryTable instance
     *
     * @param configuredLogic
     *            A configured ShardQueryTable
     */
    public GenericShardQueryConfiguration(ShardQueryLogic configuredLogic, Query query) {
        // General query options
        if (-1 == configuredLogic.getMaxResults()) {
            this.setMaxQueryResults(Long.MAX_VALUE);
        } else {
            this.setMaxQueryResults(configuredLogic.getMaxResults());
        }
        
        this.setMaxRowsToScan(configuredLogic.getMaxRowsToScan());
        this.setNumQueryThreads(configuredLogic.getQueryThreads());
        this.setDefaultType(configuredLogic.getDefaultType());
        
        // Table names
        this.setShardTableName(configuredLogic.getTableName());
        this.setIndexTableName(configuredLogic.getIndexTableName());
        this.setReverseIndexTableName(configuredLogic.getReverseIndexTableName());
        this.setMetadataTableName(configuredLogic.getMetadataTableName());
        
        // Enrichment properties
        this.setUseEnrichers(configuredLogic.isUseEnrichers());
        this.setEnricherClassNames(configuredLogic.getEnricherClassNames());
        
        // Filter properties
        this.setUseFilters(configuredLogic.isUseFilters());
        this.setFilterClassNames(configuredLogic.getFilterClassNames());
        
        // ReadAheadIterator properties
        this.setUseReadAheadIterator(configuredLogic.isUseReadAheadIterator());
        
        if (this.getUseReadAheadIterator()) {
            this.setReadAheadQueueSize(Integer.parseInt(configuredLogic.getReadAheadQueueSize()));
            this.setReadAheadTimeOut(Integer.parseInt(configuredLogic.getReadAheadTimeOut()));
        } else {
            this.setReadAheadQueueSize(0);
            this.setReadAheadTimeOut(0);
        }
        
        this.setFullTableScanEnabled(configuredLogic.isFullTableScanEnabled());
        this.setNonEventKeyPrefixes(Arrays.asList(StringUtils.split(configuredLogic.getNonEventKeyColFams(), PARAM_VALUE_SEP)));
        this.setUnevaluatedFields(configuredLogic.getUnevaluatedFields());
        this.setMinSelectivity(configuredLogic.getMinimumSelectivity());
        
        this.setIncludeDataTypeAsField(configuredLogic.getIncludeDataTypeAsField());
        this.setIncludeHierarchyFields(configuredLogic.getIncludeHierarchyFields());
        this.setBlacklistedFields(configuredLogic.getBlacklistedFields());
        
        // ShardEventEvaluatingIterator options
        this.setIncludeGroupingContext(configuredLogic.getIncludeGroupingContext());
        
        // Pass down the RangeCalculator options
        this.setEventPerDayThreshold(configuredLogic.getEventPerDayThreshold());
        this.setShardsPerDayThreshold(configuredLogic.getShardsPerDayThreshold());
        this.setMaxTermThreshold(configuredLogic.getMaxTermThreshold());
        this.setRangeExpansionThreshold(configuredLogic.getRangeExpansionThreshold());
        this.setMaxTermExpansionThreshold(configuredLogic.getMaxTermExpansionThreshold());
        
        this.setUidMapperClass(configuredLogic.getUidMapperClass());
        this.setReturnUidMapperClass(configuredLogic.getReturnUidMapperClass());
        
        this.setDataTypes(configuredLogic.getDataTypes());
        
        this.setQuery(query);
    }
    
    public GenericShardQueryConfiguration(ShardQueryTable configuredLogic, Query query) {
        this((ShardQueryLogic) configuredLogic, query);
        
        this.setShardDateFormatter(configuredLogic.getDateFormatter());
    }
    
    public Collection<Range> getRanges() {
        return ranges;
    }
    
    public void setRanges(Collection<Range> ranges) {
        this.ranges = ranges;
    }
    
    public String getShardTableName() {
        return shardTableName;
    }
    
    public void setShardTableName(String shardTableName) {
        this.shardTableName = shardTableName;
    }
    
    public String getMetadataTableName() {
        return metadataTableName;
    }
    
    public void setMetadataTableName(String metadataTableName) {
        this.metadataTableName = metadataTableName;
    }
    
    public String getIndexTableName() {
        return indexTableName;
    }
    
    public void setIndexTableName(String indexTableName) {
        this.indexTableName = indexTableName;
    }
    
    public String getReverseIndexTableName() {
        return reverseIndexTableName;
    }
    
    public void setReverseIndexTableName(String reverseIndexTableName) {
        this.reverseIndexTableName = reverseIndexTableName;
    }
    
    public Integer getNumQueryThreads() {
        return numQueryThreads;
    }
    
    public void setNumQueryThreads(Integer numQueryThreads) {
        this.numQueryThreads = numQueryThreads;
    }
    
    public Boolean getFullTableScanEnabled() {
        return fullTableScanEnabled;
    }
    
    public void setFullTableScanEnabled(Boolean fullTableScanEnabled) {
        this.fullTableScanEnabled = fullTableScanEnabled;
    }
    
    public SimpleDateFormat getShardDateFormatter() {
        return shardDateFormatter;
    }
    
    public void setShardDateFormatter(SimpleDateFormat shardDateFormatter) {
        this.shardDateFormatter = shardDateFormatter;
    }
    
    public Boolean getUseReadAheadIterator() {
        return useReadAheadIterator;
    }
    
    public void setUseReadAheadIterator(Boolean useReadAheadIterator) {
        this.useReadAheadIterator = useReadAheadIterator;
    }
    
    public Integer getReadAheadQueueSize() {
        return readAheadQueueSize;
    }
    
    public void setReadAheadQueueSize(Integer readAheadQueueSize) {
        this.readAheadQueueSize = readAheadQueueSize;
    }
    
    public Integer getReadAheadTimeOut() {
        return readAheadTimeOut;
    }
    
    public void setReadAheadTimeOut(Integer readAheadTimeOut) {
        this.readAheadTimeOut = readAheadTimeOut;
    }
    
    public Set<String> getDatatypeFilter() {
        return datatypeFilter;
    }
    
    public String getDatatypeFilterAsString() {
        return org.apache.commons.lang.StringUtils.join(this.getDatatypeFilter(), PARAM_VALUE_SEP);
    }
    
    public void setDatatypeFilter(Set<String> typeFilter) {
        this.datatypeFilter = typeFilter;
    }
    
    public Set<String> getProjectFields() {
        return projectFields;
    }
    
    public String getProjectFieldsAsString() {
        return org.apache.commons.lang.StringUtils.join(this.getProjectFields(), PARAM_VALUE_SEP);
    }
    
    public void setProjectFields(Set<String> projectFields) {
        this.projectFields = projectFields;
    }
    
    public Set<String> getBlacklistedFields() {
        return blacklistedFields;
    }
    
    public String getBlacklistedFieldsAsString() {
        return org.apache.commons.lang.StringUtils.join(this.getBlacklistedFields(), PARAM_VALUE_SEP);
    }
    
    public void setBlacklistedFields(Set<String> blacklistedFields) {
        this.blacklistedFields.addAll(blacklistedFields);
    }
    
    public String getEvaluationQuery() {
        return evaluationQuery;
    }
    
    public void setEvaluationQuery(String evaluationQuery) {
        this.evaluationQuery = evaluationQuery;
    }
    
    public String getFieldIndexQuery() {
        return fieldIndexQuery;
    }
    
    public void setFieldIndexQuery(String fieldIndexQuery) {
        this.fieldIndexQuery = fieldIndexQuery;
    }
    
    public Boolean getUseEnrichers() {
        return useEnrichers;
    }
    
    public void setUseEnrichers(Boolean useEnrichers) {
        this.useEnrichers = useEnrichers;
    }
    
    public List<String> getEnricherClassNames() {
        return enricherClassNames;
    }
    
    public String getEnricherClassNamesAsString() {
        return org.apache.commons.lang.StringUtils.join(this.getEnricherClassNames(), PARAM_VALUE_SEP);
    }
    
    public void setEnricherClassNames(List<String> enricherClassNames) {
        this.enricherClassNames = enricherClassNames;
    }
    
    public Boolean getUseFilters() {
        return useFilters;
    }
    
    public void setUseFilters(Boolean useFilters) {
        this.useFilters = useFilters;
    }
    
    public List<String> getFilterClassNames() {
        return filterClassNames;
    }
    
    public void setFilterClassNames(List<String> filterClassNames) {
        this.filterClassNames = filterClassNames;
    }
    
    public String getFilterClassNamesAsString() {
        return org.apache.commons.lang.StringUtils.join(this.getFilterClassNames(), PARAM_VALUE_SEP);
    }
    
    public Class<? extends Type<?>> getDefaultType() {
        return defaultDataType;
    }
    
    public void setDefaultType(Class<? extends Type<?>> defaultType) {
        this.defaultDataType = defaultType;
    }
    
    public Set<String> getNonEventKeyPrefixes() {
        return nonEventKeyPrefixes;
    }
    
    public String getNonEventKeyPrefixesAsString() {
        return org.apache.commons.lang.StringUtils.join(this.getNonEventKeyPrefixes(), PARAM_VALUE_SEP);
    }
    
    public void setNonEventKeyPrefixes(Collection<String> nonEventKeyPrefixes) {
        if (null == nonEventKeyPrefixes) {
            this.nonEventKeyPrefixes = new HashSet<>();
        } else {
            this.nonEventKeyPrefixes = new HashSet<>(nonEventKeyPrefixes);
        }
    }
    
    public Set<String> getUnevaluatedFields() {
        return unevaluatedFields;
    }
    
    public String getUnevaluatedFieldsAsString() {
        return org.apache.commons.lang.StringUtils.join(this.unevaluatedFields, PARAM_VALUE_SEP);
    }
    
    public void setUnevaluatedFields(Collection<String> unevaluatedFields) {
        if (null == unevaluatedFields) {
            this.unevaluatedFields = new HashSet<>();
        } else {
            this.unevaluatedFields = new HashSet<>(unevaluatedFields);
        }
    }
    
    public Multimap<String,Type<?>> getIndexedFieldsDataTypes() {
        return indexedFieldsDataTypes;
    }
    
    public int getEventPerDayThreshold() {
        return eventPerDayThreshold;
    }
    
    public int getShardsPerDayThreshold() {
        return shardsPerDayThreshold;
    }
    
    public int getMaxTermThreshold() {
        return maxTermThreshold;
    }
    
    public int getRangeExpansionThreshold() {
        return rangeExpansionThreshold;
    }
    
    public void setEventPerDayThreshold(int eventPerDayThreshold) {
        this.eventPerDayThreshold = eventPerDayThreshold;
    }
    
    public void setShardsPerDayThreshold(int shardsPerDayThreshold) {
        this.shardsPerDayThreshold = shardsPerDayThreshold;
    }
    
    public void setMaxTermThreshold(int maxTermThreshold) {
        this.maxTermThreshold = maxTermThreshold;
    }
    
    public void setRangeExpansionThreshold(int rangeExpansionThreshold) {
        this.rangeExpansionThreshold = rangeExpansionThreshold;
    }
    
    public int getMaxTermExpansionThreshold() {
        return maxTermExpansionThreshold;
    }
    
    public void setMaxTermExpansionThreshold(int maxTermExpansionThreshold) {
        this.maxTermExpansionThreshold = maxTermExpansionThreshold;
    }
    
    /**
     * Creates string, mapping the dataTypes used for a field to pass to the QueryEvaluator through the options map.
     *
     * @return FIELDNAME1:normalizer.class;FIELDNAME2:normalizer.class;
     */
    public String getIndexedFieldsDataTypesAsString() {
        if (null == this.getIndexedFieldsDataTypes()) {
            return "";
        }
        
        StringBuilder sb = new StringBuilder();
        for (String fieldName : this.getIndexedFieldsDataTypes().keySet()) {
            sb.append(fieldName);
            for (Type<?> tn : this.getIndexedFieldsDataTypes().get(fieldName)) {
                sb.append(":");
                sb.append(tn.getClass().getName());
            }
            sb.append(";");
        }
        
        return sb.toString();
    }
    
    public void setIndexedFieldsDataTypes(Multimap<String,Type<?>> indexedFieldsDataTypes) {
        this.indexedFieldsDataTypes = indexedFieldsDataTypes;
    }
    
    public double getMinSelectivity() {
        return minSelectivity;
    }
    
    public void setMinSelectivity(double minSelectivity) {
        this.minSelectivity = minSelectivity;
    }
    
    /**
     * Checks for non-null, sane values for the configured values
     *
     * @return True if all of the encapsulated values have legitimate values, otherwise false
     */
    @Override
    public boolean canRunQuery() {
        List<QueryData> queries = Lists.newArrayList(this.getQueries());
        this.setQueries(queries.iterator());
        
        if (queries.size() == 0) {
            return false;
        }
        
        QueryData queryData = queries.get(0);
        
        // Ensure we were given connector and authorizations
        if (null == this.getConnector() || null == this.getAuthorizations()) {
            return false;
        }
        
        // Ensure valid dates
        if (null == this.getBeginDate() || null == this.getEndDate() || getEndDate().before(getBeginDate())) {
            return false;
        }
        
        // A non-empty table was given
        if (null == getTableName() || this.getTableName().isEmpty()) {
            return false;
        }
        
        if (null == this.getEvaluationQuery() || this.getEvaluationQuery().isEmpty()) {
            if (null == queryData.getQuery() || queryData.getQuery().isEmpty()) {
                log.warn("No valid evaluation query or 'regular' query. Cannot run query!");
                return false;
            }
        } else {
            if (null == this.getFieldIndexQuery() || this.getFieldIndexQuery().isEmpty()) {
                log.warn("No field index query was given when we had a valid evaluation query. Cannot run query!");
                return false;
            }
        }
        
        if (null == queryData.getRanges() || queryData.getRanges().isEmpty()) {
            log.warn("No ranges were provided. Cannot run query!");
            return false;
        }
        
        return true;
    }
    
    public boolean getIncludeDataTypeAsField() {
        return includeDataTypeAsField;
    }
    
    public void setIncludeDataTypeAsField(boolean includeDataTypeAsField) {
        this.includeDataTypeAsField = includeDataTypeAsField;
    }
    
    public boolean getIncludeHierarchyFields() {
        return includeHierarchyFields;
    }
    
    public void setIncludeHierarchyFields(boolean includeHierarchyFields) {
        this.includeHierarchyFields = includeHierarchyFields;
    }
    
    public boolean getIncludeGroupingContext() {
        return includeGroupingContext;
    }
    
    public void setIncludeGroupingContext(boolean withContextOption) {
        this.includeGroupingContext = withContextOption;
    }
    
    public DatawaveTreeNode getQueryTree() {
        return queryTree;
    }
    
    public void setQueryTree(DatawaveTreeNode queryTree) {
        this.queryTree = queryTree;
    }
    
    public String getUidMapperClass() {
        return uidMapperClass;
    }
    
    public void setUidMapperClass(String uidMapperClass) {
        this.uidMapperClass = uidMapperClass;
    }
    
    public String getReturnUidMapperClass() {
        return returnUidMapperClass;
    }
    
    public void setReturnUidMapperClass(String returnUidMapperClass) {
        this.returnUidMapperClass = returnUidMapperClass;
    }
    
    public Collection<Type<?>> getDataTypes() {
        return dataTypes;
    }
    
    public void setDataTypes(Collection<Type<?>> dataTypes) {
        this.dataTypes = dataTypes;
    }
    
    public Query getQuery() {
        return query;
    }
    
    public void setQuery(Query query) {
        this.query = query;
    }
}
