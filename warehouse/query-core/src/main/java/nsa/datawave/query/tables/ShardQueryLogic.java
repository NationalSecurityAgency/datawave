package nsa.datawave.query.tables;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import nsa.datawave.core.iterators.BooleanLogicIteratorJexl;
import nsa.datawave.core.iterators.DatatypeFilterIterator;
import nsa.datawave.core.iterators.EnrichingIterator;
import nsa.datawave.core.iterators.EvaluatingIterator;
import nsa.datawave.core.iterators.ReadAheadIterator;
import nsa.datawave.core.iterators.ShardEventOptimizationIterator;
import nsa.datawave.core.iterators.uid.ShardUidMappingIterator;
import nsa.datawave.data.hash.UID;
import nsa.datawave.data.type.NoOpType;
import nsa.datawave.data.type.NumberType;
import nsa.datawave.data.type.Type;
import nsa.datawave.query.QueryParameters;
import nsa.datawave.query.config.GenericShardQueryConfiguration;
import nsa.datawave.query.enrich.DataEnricher;
import nsa.datawave.query.enrich.EnrichingMaster;
import nsa.datawave.query.index.stats.IndexStatsClient;
import nsa.datawave.query.language.parser.QueryParser;
import nsa.datawave.query.language.tree.QueryNode;
import nsa.datawave.query.model.QueryModel;
import nsa.datawave.query.parser.DatawaveQueryAnalyzer;
import nsa.datawave.query.parser.DatawaveTreeNode;
import nsa.datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import nsa.datawave.query.parser.JexlOperatorConstants;
import nsa.datawave.query.parser.RangeCalculator;
import nsa.datawave.query.parser.RangeCalculator.RangeExpansionException;
import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.rewrite.exceptions.DoNotPerformOptimizedQueryException;
import nsa.datawave.query.rewrite.exceptions.InvalidQueryException;
import nsa.datawave.query.rewrite.exceptions.TooManyTermsException;
import nsa.datawave.query.util.MetadataHelper;
import nsa.datawave.query.util.MetadataHelperFactory;
import nsa.datawave.util.StringUtils;
import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.configuration.QueryData;
import nsa.datawave.webservice.query.exception.DatawaveErrorCode;
import nsa.datawave.webservice.query.exception.QueryException;
import nsa.datawave.webservice.query.logic.BaseQueryLogic;
import nsa.datawave.webservice.query.logic.QueryLogicTransformer;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

//import nsa.datawave.ingest.util.RegionTimer;

/**
 * <pre>
 * <h2>Overview</h2>
 * QueryTable implementation that works with the JEXL grammar. This QueryTable
 * uses the DATAWAVE metadata, global index, and sharded event table to return
 * results based on the query. The runServerQuery method is the main method
 * that is called from the web service, and it contains the logic used to
 * run the queries against ACCUMULO. Example queries:
 * 
 *  <b>Single Term Query</b>
 *  'foo' - looks in global index for foo, and if any entries are found, then the query
 *          is rewritten to be field1 == 'foo' or field2 == 'foo', etc. This is then passed
 *          down the optimized query path which uses the intersecting iterators on the shard
 *          table.
 * 
 *  <b>Boolean expression</b>
 *  field == 'foo' - For fielded queries, those that contain a field, an operator, and a literal (string or number),
 *                   the query is parsed and the set of eventFields in the query that are indexed is determined by
 *                   querying the metadata table. Depending on the conjunctions in the query (or, and, not) and the
 *                   eventFields that are indexed, the query may be sent down the optimized path or the full scan path.
 * 
 *  We are not supporting all of the operators that JEXL supports at this time. We are supporting the following operators:
 * 
 *  ==, !=, &gt;, &ge;, &lt;, &le;, =~, !~, and the reserved word 'null'
 * 
 *  Custom functions can be created and registered with the Jexl engine. The functions can be used in the queries in conjunction
 *  with other supported operators. A sample function has been created, called between, and is bound to the 'f' namespace. An
 *  example using this function is : "f:between(LATITUDE,60.0, 70.0)"
 * 
 *  <h2>Constraints on Query Structure</h2>
 *  Queries that are sent to this class need to be formatted such that there is a space on either side of the operator. We are
 *  rewriting the query in some cases and the current implementation is expecting a space on either side of the operator.
 * 
 *  <h2>Notes on Optimization</h2>
 *  Queries that meet any of the following criteria will perform a full scan of the events in the sharded event table:
 * 
 *  1. An 'or' conjunction exists in the query but not all of the terms are indexed.
 *  2. No indexed terms exist in the query
 *  3. An unsupported operator exists in the query
 * 
 *  <h2>Notes on Features</h2>
 * 
 *  1. If there is no type specified for a field in the metadata table, then it defaults to using the NoOpType. The default
 *     can be overriden by calling setDefaultType()
 *  2. We support fields that are indexed, but not in the event. An example of this is for text documents, where the text is tokenized
 *     and indexed, but the tokens are not stored with the event.
 *  3. We support fields that have term frequency records in the shard table containing lists of offsets.  This would be for tokens
 *     parsed out of content and can subsequently be used in various content functions such as 'within' or 'adjacent' or 'phrase'.
 *  4. We support the ability to define a list of {@link DataEnricher}s to add additional information to returned events. Found events are
 *     passed through the {@link EnrichingMaster} which passes the event through each configured data enricher class. Only the value
 *     can be modified. The key *cannot* be modified through this interface (as it could break the sorted order). Enriching must be enabled
 *     by setting {@link #useEnrichers} to true and providing a list of {@link nsa.datawave.query.enrich.DataEnrich} class names in
 *     {@link #enricherClassNames}.
 *  5. A list of {@link nsa.datawave.query.filter.DataFilter}s can be specified to remove found Events before they are returned to the user.
 *     These data filters can return a true/false value on whether the Event should be returned to the user or discarded. Additionally,
 *     the filter can return a Map<String, Object> that can be passed into a JexlContext (provides the necessary information for Jexl to
 *     evaluate an Event based on information not already present in the Event or information that doesn't need to be returned with the Event.
 *     Filtering must be enabled by setting {@link #useFilters} to true and providing a list of {@link nsa.datawave.query.filter.DataFilter} class
 *     names in {@link #filterClassNames}.
 *  6. The query limits the results (default: 5000) using the setMaxResults method. In addition, "max.results.override" can be passed to the
 *     query as part of the Parameters object which allows query specific limits (but will not be more than set default)
 *  7. Projection can be accomplished by setting the {@link EvaluatingIterator.RETURN_FIELDS} parameter to a '/'-separated list of field names.
 *
 * </pre>
 *
 * @see nsa.datawave.query.enrich
 * @see nsa.datawave.query.filter
 *
 *      Deprecated - see nsa.datawave.query.rewrite.tables.RefactoredShardQueryLogic
 */
@Deprecated
public abstract class ShardQueryLogic extends BaseQueryLogic<Entry<Key,Value>> {
    
    protected static final Logger log = Logger.getLogger(ShardQueryLogic.class);
    
    public static final String NULL_BYTE = "\0";
    
    /**
     * Object that is used to hold ranges found in the index. Subclasses may compute the final range set in various ways.
     */
    public static abstract class IndexRanges {
        
        private Map<String,String> indexValuesToOriginalValues = null;
        private Multimap<String,String> fieldNamesAndValues = HashMultimap.create();
        private Map<String,Long> termCardinality = new HashMap<>();
        protected Map<String,TreeSet<Range>> ranges = new HashMap<>();
        
        public Multimap<String,String> getFieldNamesAndValues() {
            return fieldNamesAndValues;
        }
        
        public void setFieldNamesAndValues(Multimap<String,String> fieldNamesAndValues) {
            this.fieldNamesAndValues = fieldNamesAndValues;
        }
        
        public final Map<String,Long> getTermCardinality() {
            return termCardinality;
        }
        
        public Map<String,String> getIndexValuesToOriginalValues() {
            return indexValuesToOriginalValues;
        }
        
        public void setIndexValuesToOriginalValues(Map<String,String> indexValuesToOriginalValues) {
            this.indexValuesToOriginalValues = indexValuesToOriginalValues;
        }
        
        public abstract void add(String term, Range r);
        
        public abstract Set<Range> getRanges();
    }
    
    /**
     * Object that computes the ranges by unioning all of the ranges for all of the terms together. In the case where ranges overlap, the largest range is used.
     */
    public static class UnionIndexRanges extends IndexRanges {
        
        public static final String DEFAULT_KEY = "default";
        
        public UnionIndexRanges() {
            this.ranges.put(DEFAULT_KEY, new TreeSet<Range>());
        }
        
        @Override
        public Set<Range> getRanges() {
            // So the set of ranges is ordered. It *should* be the case that
            // ranges with shard ids will sort before ranges that point to
            // a specific event. Populate a new set of ranges but don't add a
            // range for an event where that range is contained in a range already
            // added.
            Set<Text> shardsAdded = new HashSet<>();
            Set<Range> returnSet = new HashSet<>();
            for (Range r : ranges.get(DEFAULT_KEY)) {
                if (!shardsAdded.contains(r.getStartKey().getRow())) {
                    // Only add ranges with a start key for the entire shard.
                    if (r.getStartKey().getColumnFamily() == null) {
                        shardsAdded.add(r.getStartKey().getRow());
                    }
                    returnSet.add(r);
                } else {
                    if (log.isDebugEnabled())
                        log.debug("Skipping event specific range: " + r.toString() + " because shard range has already been added: "
                                        + shardsAdded.contains(r.getStartKey().getRow()));
                }
            }
            return returnSet;
        }
        
        @Override
        public void add(String term, Range r) {
            ranges.get(DEFAULT_KEY).add(r);
        }
    }
    
    private String metadataTableName;
    private String indexTableName;
    private String reverseIndexTableName;
    private String tableName;
    private int queryThreads = 8;
    private String readAheadQueueSize = "0";
    private String readAheadTimeOut = "0";
    private boolean useReadAheadIterator;
    private List<String> enricherClassNames = null;
    private List<String> filterClassNames = null;
    private boolean useEnrichers = false;
    private boolean useFilters = false;
    private List<String> unevaluatedFields = Collections.emptyList();
    private Class<? extends Type<?>> defaultType = NoOpType.class;
    private boolean fullTableScanEnabled = true;
    private String nonEventKeyColFams = "d" + GenericShardQueryConfiguration.PARAM_VALUE_SEP + "tf";
    private String statsTable = "shardIndexStats";
    private double minSelectivity = -1.0;
    private boolean includeDataTypeAsField = false;
    private boolean includeHierarchyFields = false;
    private boolean includeGroupingContext = false;
    
    private Set<String> blacklistedFields = new HashSet<>(0);
    protected Collection<Type<?>> dataTypes = Collections.emptySet();
    private boolean allowAllOrNothingQuery = false;
    private String modelName = null;
    private String modelTableName = null;
    protected QueryModel queryModel = null;
    protected ScannerFactory scannerFactory;
    protected String mandatoryQuerySyntax = null;
    
    // Map of syntax names to QueryParser classes
    Map<String,QueryParser> querySyntaxParsers = new HashMap<>();
    
    // Threshold values used in the new RangeCalculator
    private int eventPerDayThreshold = 10000;
    private int shardsPerDayThreshold = 10;
    private int maxTermThreshold = 2500;
    private int rangeExpansionThreshold = 1000;
    private int maxTermExpansionThreshold = 5000;
    
    protected MetadataHelperFactory metadataHelperFactory;
    protected MetadataHelper metadataHelper;
    
    // This is a UidMapper class that is used by the ShardUidMappingIterator and the GlobalIndexUidMappingIterator to
    // remap the underlying key/value uids. The use case is to set the TopLevelDocumentUidMapper which maps all
    // children uids to the top level uid allowing queries to be applied to the entire document tree.
    private String uidMapperClass = null;
    
    // This is a UidMapper class that is used by the evaluating iterator to return an alternate document from the one
    // that was evaluated. The use case is to set the ParentDocumentUidMapper which will allow us to query documents
    // and to return the parent documents instead of the ones that were hit by the query.
    private String returnUidMapperClass = null;
    
    public ShardQueryLogic() {
        super();
    }
    
    public ShardQueryLogic(ShardQueryLogic other) {
        super(other);
        this.setMetadataTableName(other.getMetadataTableName());
        this.setMetadataHelperFactory(other.getMetadataHelperFactory());
        this.setIndexTableName(other.getIndexTableName());
        this.setReverseIndexTableName(other.getReverseIndexTableName());
        this.setTableName(other.getTableName());
        this.setQueryThreads(other.getQueryThreads());
        this.setReadAheadQueueSize(other.getReadAheadQueueSize());
        this.setReadAheadTimeOut(other.getReadAheadTimeOut());
        this.setUseReadAheadIterator(other.isUseReadAheadIterator());
        this.setEnricherClassNames(other.getEnricherClassNames());
        this.setFilterClassNames(other.getFilterClassNames());
        this.setUseEnrichers(other.isUseEnrichers());
        this.setUseFilters(other.isUseFilters());
        this.setUnevaluatedFields(other.getUnevaluatedFields());
        this.setFullTableScanEnabled(other.isFullTableScanEnabled());
        this.setNonEventKeyColFams(other.getNonEventKeyColFams());
        this.setMinimumSelectivity(other.getMinimumSelectivity());
        this.setIncludeDataTypeAsField(other.getIncludeDataTypeAsField());
        this.setIncludeHierarchyFields(other.getIncludeHierarchyFields());
        this.setIncludeGroupingContext(other.getIncludeGroupingContext());
        this.setBlacklistedFields(other.getBlacklistedFields());
        this.setEventPerDayThreshold(other.getEventPerDayThreshold());
        this.setShardsPerDayThreshold(other.getShardsPerDayThreshold());
        this.setMaxTermThreshold(other.getMaxTermThreshold());
        this.setRangeExpansionThreshold(other.getRangeExpansionThreshold());
        this.setMaxTermExpansionThreshold(other.getMaxTermExpansionThreshold());
        this.setUidMapperClass(other.getUidMapperClass());
        this.setReturnUidMapperClass(other.getReturnUidMapperClass());
        this.setAllowAllOrNothingQuery(other.isAllowAllOrNothingQuery());
        this.setQueryModel(other.queryModel);
        this.setModelName(other.modelName);
        this.setModelTableName(other.modelTableName);
        this.setQuerySyntaxParsers(other.getQuerySyntaxParsers());
        this.setMandatoryQuerySyntax(other.getMandatoryQuerySyntax());
    }
    
    /**
     * Initialize a metadata helper if we haven't already
     *
     * @param connector
     * @param metadataTableName
     * @param auths
     * @throws ExecutionException
     * @throws TableNotFoundException
     */
    protected void initializeMetadataHelper(Connector connector, String metadataTableName, Set<Authorizations> auths) throws TableNotFoundException,
                    ExecutionException {
        if (null == this.metadataHelperFactory)
            throw new RuntimeException("MetadataHelperFactory was not set. Fix this");
        if (this.metadataHelper == null) {
            this.metadataHelper = this.metadataHelperFactory.createMetadataHelper();
        }
        this.metadataHelper.initialize(connector, metadataTableName, auths);
    }
    
    /**
     * Queries metadata table to determine which terms are indexed and loads the normalizerCache
     *
     * @param config
     * @param queryLiterals
     * @return map of indexed field names to types used in this date range
     * @throws TableNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    protected Multimap<String,Type<?>> findIndexedTerms(GenericShardQueryConfiguration config, MetadataHelper metadataHelper, Set<String> queryLiterals)
                    throws TableNotFoundException, InstantiationException, IllegalAccessException {
        
        Multimap<String,Type<?>> indexedFields = HashMultimap.create();
        
        for (String literal : queryLiterals) {
            if (log.isDebugEnabled()) {
                log.debug("Querying " + config.getMetadataTableName() + " table for " + literal);
            }
            
            Set<Type<?>> dataTypes = metadataHelper.getDatatypesForField(literal, null);
            
            if (dataTypes.size() > 0) {
                if (log.isTraceEnabled()) {
                    log.trace("DataTypes for " + literal.toUpperCase() + ": " + dataTypes);
                }
                
                indexedFields.putAll(literal, dataTypes);
            } else if (log.isDebugEnabled()) {
                log.debug("Could not find any dataTypes for " + literal);
            }
        }
        
        return indexedFields;
    }
    
    /**
     * Performs a lookup in the global index for an ANYFIELD term and returns the field names where the term is found
     *
     * @param config
     * @param node
     * @return set of field names from the global index for the nodes value
     * @throws TableNotFoundException
     */
    protected abstract Set<String> getUnfieldedTermIndexInformation(GenericShardQueryConfiguration config, DatawaveTreeNode node)
                    throws TableNotFoundException, IOException, InstantiationException, IllegalAccessException, JavaRegexParseException;
    
    /**
     * Performs a lookup in the global index / reverse index and returns a RangeCalculator
     *
     * @param c
     *            Accumulo connection
     * @param auths
     *            authset for queries
     * @param indexedFields
     *            multimap of indexed field name and Normalizers used
     * @param terms
     *            multimap of field name and QueryTerm object
     * @param begin
     *            query begin date
     * @param end
     *            query end date
     * @param dateFormatter
     * @param indexTableName
     * @param reverseIndexTableName
     * @param queryString
     *            original query string
     * @param queryThreads
     * @param datatypes
     *            - optional list of types
     * @return range calculator
     * @throws TableNotFoundException
     */
    protected abstract RangeCalculator getTermIndexInformation(GenericShardQueryConfiguration config, Set<String> indexedFieldNames, DatawaveTreeNode root)
                    throws TableNotFoundException, org.apache.commons.jexl2.parser.ParseException, TooManyTermsException, RangeExpansionException,
                    JavaRegexParseException;
    
    protected abstract Collection<Range> getFullScanRange(GenericShardQueryConfiguration config, Multimap<String,DatawaveTreeNode> terms);
    
    protected abstract SimpleDateFormat getDateFormatter();
    
    public String getMetadataTableName() {
        return metadataTableName;
    }
    
    public String getIndexTableName() {
        return indexTableName;
    }
    
    @Override
    public String getTableName() {
        return tableName;
    }
    
    public void setMetadataTableName(String metadataTableName) {
        this.metadataTableName = metadataTableName;
    }
    
    public void setIndexTableName(String indexTableName) {
        this.indexTableName = indexTableName;
    }
    
    @Override
    public void setTableName(String tableName) {
        this.tableName = tableName;
        super.setTableName(tableName);
    }
    
    public int getQueryThreads() {
        return queryThreads;
    }
    
    public void setQueryThreads(int queryThreads) {
        this.queryThreads = queryThreads;
    }
    
    public String getReadAheadQueueSize() {
        return readAheadQueueSize;
    }
    
    public String getReadAheadTimeOut() {
        return readAheadTimeOut;
    }
    
    public boolean isUseReadAheadIterator() {
        return useReadAheadIterator;
    }
    
    public void setReadAheadQueueSize(String readAheadQueueSize) {
        this.readAheadQueueSize = readAheadQueueSize;
    }
    
    public void setReadAheadTimeOut(String readAheadTimeOut) {
        this.readAheadTimeOut = readAheadTimeOut;
    }
    
    public void setUseReadAheadIterator(boolean useReadAheadIterator) {
        this.useReadAheadIterator = useReadAheadIterator;
    }
    
    public List<String> getEnricherClassNames() {
        return this.enricherClassNames;
    }
    
    public void setEnricherClassNames(List<String> enricherClassNames) {
        this.enricherClassNames = enricherClassNames;
    }
    
    public boolean isUseEnrichers() {
        return this.useEnrichers;
    }
    
    public void setUseEnrichers(boolean useEnrichers) {
        this.useEnrichers = useEnrichers;
    }
    
    public List<String> getFilterClassNames() {
        return filterClassNames;
    }
    
    public void setFilterClassNames(List<String> filterClassNames) {
        this.filterClassNames = filterClassNames;
    }
    
    public boolean isUseFilters() {
        return this.useFilters;
    }
    
    public void setUseFilters(boolean useFilters) {
        this.useFilters = useFilters;
    }
    
    public String getReverseIndexTableName() {
        return reverseIndexTableName;
    }
    
    public void setReverseIndexTableName(String reverseIndexTableName) {
        this.reverseIndexTableName = reverseIndexTableName;
    }
    
    public List<String> getUnevaluatedFields() {
        return unevaluatedFields;
    }
    
    public void setUnevaluatedFields(List<String> unevaluatedFields) {
        this.unevaluatedFields = unevaluatedFields;
    }
    
    public void setUnevaluatedFields(String unevaluatedFieldList) {
        this.unevaluatedFields = Arrays.asList(unevaluatedFieldList.split(GenericShardQueryConfiguration.PARAM_VALUE_SEP_STR));
    }
    
    public Class<? extends Type<?>> getDefaultType() {
        return defaultType;
    }
    
    @SuppressWarnings("unchecked")
    public void setDefaultType(String className) {
        try {
            defaultType = (Class<? extends Type<?>>) Class.forName(className);
        } catch (ClassNotFoundException ex) {
            log.warn("Class name: " + className + " not found, defaulting to NoOpNormalizer.class");
            defaultType = NoOpType.class;
        }
    }
    
    public boolean isFullTableScanEnabled() {
        return fullTableScanEnabled;
    }
    
    public void setFullTableScanEnabled(boolean fullTableScanEnabled) {
        this.fullTableScanEnabled = fullTableScanEnabled;
    }
    
    public double getMinimumSelectivity() {
        return this.minSelectivity;
    }
    
    public void setMinimumSelectivity(double d) {
        this.minSelectivity = d;
    }
    
    public String getIndexStatsTable() {
        return this.statsTable;
    }
    
    public void setIndexStatsTable(String t) {
        this.statsTable = t;
    }
    
    public MetadataHelperFactory getMetadataHelperFactory() {
        return metadataHelperFactory;
    }
    
    public void setMetadataHelperFactory(MetadataHelperFactory metadataHelperFactory) {
        this.metadataHelperFactory = metadataHelperFactory;
    }
    
    public Map<String,Set<String>> getFieldValueMappingsFromGlobalIndex(Multimap<String,DatawaveTreeNode> fieldToNodeMap, GenericShardQueryConfiguration config)
                    throws IllegalAccessException, InstantiationException {
        // FieldValue -> Set<FieldNames>
        Map<String,Set<String>> allMappings = new HashMap<>();
        
        for (DatawaveTreeNode node : fieldToNodeMap.get(Constants.ANY_FIELD)) {
            if (allMappings.containsKey(node.getFieldValue().toLowerCase())) {
                // we already have it, no need to look it up again
                continue;
            }
            
            try {
                Set<String> thisNodesMappings = this.getUnfieldedTermIndexInformation(config, node);
                allMappings.put(node.getFieldValue().toLowerCase(), thisNodesMappings);
                if (log.isDebugEnabled()) {
                    log.debug("added mapping: " + node.getFieldValue().toLowerCase() + " : " + thisNodesMappings);
                }
            } catch (IOException ex) {
                throw new RuntimeException("Global index table could not be scanned", ex);
            } catch (TableNotFoundException ex) {
                throw new RuntimeException("Global index table not found", ex);
            } catch (JavaRegexParseException ex) {
                throw new RuntimeException("Could not parse a java regex", ex);
            }
        }
        return allMappings;
    }
    
    public void initialize(GenericShardQueryConfiguration config, Connector connection, Query settings, Set<Authorizations> auths) throws Exception {
        // Set the connector and the authorizations into the config object
        config.setConnector(connection);
        config.setAuthorizations(auths);
        
        initializeMetadataHelper(connection, config.getMetadataTableName(), auths);
        
        QueryData queryData = new QueryData();
        
        // Determine query syntax (i.e. JEXL, LUCENE, etc.)
        String querySyntax = settings.findParameter(QueryParameters.QUERY_SYNTAX).getParameterValue();
        QueryParser querySyntaxParser = null;
        if (org.apache.commons.lang.StringUtils.isEmpty(querySyntax)) {
            if (null == this.mandatoryQuerySyntax) {
                querySyntax = "JEXL";
            } else {
                querySyntax = this.mandatoryQuerySyntax;
            }
        }
        if (!("JEXL".equals(querySyntax))) {
            if (null == querySyntaxParsers)
                throw new IllegalStateException("Query syntax parsers not configured");
            querySyntaxParser = querySyntaxParsers.get(querySyntax);
            if (null == querySyntaxParser) {
                throw new IllegalArgumentException("QueryParser not configured for syntax: " + querySyntax);
            }
        }
        
        if (null != this.mandatoryQuerySyntax && !(this.mandatoryQuerySyntax.equals(querySyntax))) {
            throw new IllegalStateException("Syntax not supported, required: " + this.mandatoryQuerySyntax + ", submitted: " + querySyntax);
        }
        
        String queryString = settings.getQuery();
        if (null == queryString) {
            throw new IllegalArgumentException("Query cannot be null");
        } else {
            if ("JEXL".equals(querySyntax)) {
                config.setQueryString(queryString);
                queryData.setQuery(queryString);
                if (log.isTraceEnabled()) {
                    log.trace("jexlQueryString: " + queryString);
                }
            } else {
                String jexlQueryString = null;
                try {
                    QueryNode node = querySyntaxParser.parse(queryString);
                    jexlQueryString = node.getOriginalQuery();
                } catch (nsa.datawave.query.language.parser.ParseException e) {
                    throw new RuntimeException("Error parsing query: " + queryString + " with parser: " + querySyntaxParser.getClass().getName()
                                    + " using syntax: " + querySyntax, e);
                }
                if (log.isTraceEnabled()) {
                    log.trace("luceneQueryString: " + queryString + " --> jexlQueryString: " + jexlQueryString);
                }
                queryString = jexlQueryString;
                config.setQueryString(queryString);
            }
        }
        
        // Check if the default modelName and modelTableNames have been overriden by custom parameters.
        if (null != settings.findParameter(QueryParameters.PARAMETER_MODEL_NAME)
                        && !settings.findParameter(QueryParameters.PARAMETER_MODEL_NAME).getParameterValue().trim().isEmpty()) {
            this.modelName = settings.findParameter(QueryParameters.PARAMETER_MODEL_NAME).getParameterValue().trim();
        }
        if (null != settings.findParameter(QueryParameters.PARAMETER_MODEL_TABLE_NAME)
                        && !settings.findParameter(QueryParameters.PARAMETER_MODEL_TABLE_NAME).getParameterValue().trim().isEmpty()) {
            this.modelTableName = settings.findParameter(QueryParameters.PARAMETER_MODEL_TABLE_NAME).getParameterValue().trim();
        }
        
        if (null != this.modelName && null == this.modelTableName) {
            throw new IllegalArgumentException(QueryParameters.PARAMETER_MODEL_NAME + " has been specified but " + QueryParameters.PARAMETER_MODEL_TABLE_NAME
                            + " is missing. Both are required to use a model");
        }
        if (null != this.modelName && null != this.modelTableName) {
            this.queryModel = this.metadataHelper.getQueryModel(modelTableName, modelName, getUnevaluatedFields());
            if (log.isTraceEnabled()) {
                log.trace("forward queryModel: " + this.queryModel.getForwardQueryMapping());
                log.trace("reverse queryModel: " + this.queryModel.getReverseQueryMapping());
            }
        }
        
        // Get the list of fields to project up the stack. May be null.
        String projectFields = settings.findParameter(QueryParameters.RETURN_FIELDS).getParameterValue();
        if (null != projectFields && projectFields.trim().length() > 0) {
            List<String> projectFieldsList = Arrays.asList(StringUtils.split(projectFields, GenericShardQueryConfiguration.PARAM_VALUE_SEP));
            
            // Only set the projection fields if we were actually given some
            if (!projectFieldsList.isEmpty()) {
                config.setProjectFields(new HashSet<>(projectFieldsList));
                
                if (log.isDebugEnabled()) {
                    log.debug("Projection fields: " + projectFields);
                }
            }
        }
        
        // Get the list of blacklisted fields. May be null.
        String blacklistedFields = settings.findParameter(QueryParameters.BLACKLISTED_FIELDS).getParameterValue();
        if (null != blacklistedFields && blacklistedFields.trim().length() > 0) {
            List<String> blacklistedFieldsList = Arrays.asList(StringUtils.split(blacklistedFields, GenericShardQueryConfiguration.PARAM_VALUE_SEP));
            
            // Only set the blacklisted fields if we were actually given some
            if (!blacklistedFieldsList.isEmpty()) {
                config.setBlacklistedFields(new HashSet<>(blacklistedFieldsList));
                
                if (log.isDebugEnabled()) {
                    log.debug("Blacklisted fields: " + blacklistedFields);
                }
            }
        }
        
        // Ensure that the number of terms does not exceed our threshold
        DatawaveQueryAnalyzer modelAnalyzer = new DatawaveQueryAnalyzer();
        DatawaveTreeNode modelRoot = modelAnalyzer.parseJexlQuery(queryString);
        int termCount = modelAnalyzer.countTerms(modelRoot);
        if (termCount > getMaxTermThreshold()) {
            throw new IllegalArgumentException("The number of terms in the query exceeds the threshold ( " + termCount + " > " + getMaxTermThreshold() + " )");
        }
        
        // Overwrite projection and blacklist properties if the query model is being used
        if (null != queryModel) {
            if ((projectFields != null && !projectFields.trim().isEmpty()) || (blacklistedFields != null && !blacklistedFields.trim().isEmpty())) {
                // Use both reverse & forward mapping to update the projection fields in case the model is disjoint.
                Multimap<String,String> bothModels = invertMultimap(this.queryModel.getReverseQueryMapping());
                bothModels.putAll(this.queryModel.getForwardQueryMapping());
                if (projectFields != null && !projectFields.trim().isEmpty()) {
                    projectFields = this.queryModel.remapParameter(projectFields, bothModels);
                    log.debug("updating projection parameter to: " + projectFields);
                    List<String> projectFieldsList = Arrays.asList(StringUtils.split(projectFields, GenericShardQueryConfiguration.PARAM_VALUE_SEP));
                    config.setProjectFields(new HashSet<>(projectFieldsList));
                    settings.addParameter(QueryParameters.RETURN_FIELDS, projectFields);
                }
                
                if (blacklistedFields != null && !blacklistedFields.trim().isEmpty()) {
                    blacklistedFields = this.queryModel.remapParameter(blacklistedFields, bothModels);
                    log.debug("updating blacklist parameter to: " + blacklistedFields);
                    settings.addParameter(QueryParameters.BLACKLISTED_FIELDS, blacklistedFields);
                }
            }
            modelRoot = modelAnalyzer.applyCaseSensitivity(modelRoot, true, false);
            modelRoot = modelAnalyzer.applyFieldMapping(modelRoot, queryModel.getForwardQueryMapping(), this.metadataHelper);
            String updatedQuery = modelAnalyzer.rebuildQueryFromTree(modelRoot);
            if (log.isDebugEnabled()) {
                log.debug("Query before model: " + queryString);
                log.debug("Query after model: " + updatedQuery);
            }
            queryString = updatedQuery;
            config.setQueryString(queryString);
            this.setUnevaluatedFields(new ArrayList<>(queryModel.getUnevaluatedFields()));
        }
        
        final Date beginDate = settings.getBeginDate();
        if (null == beginDate) {
            throw new IllegalArgumentException("Begin date cannot be null");
        } else {
            config.setBeginDate(beginDate);
        }
        
        final Date endDate = settings.getEndDate();
        if (null == endDate) {
            throw new IllegalArgumentException("End date cannot be null");
        } else {
            config.setEndDate(endDate);
        }
        
        String section = "0) process query parameters";
        
        // Get the datatype set if specified
        String typeList = settings.findParameter(QueryParameters.DATATYPE_FILTER_SET).getParameterValue();
        HashSet<String> typeFilter = null;
        
        if (null != typeList && 0 != typeList.length()) {
            typeFilter = new HashSet<>();
            typeFilter.addAll(Arrays.asList(StringUtils.split(typeList, GenericShardQueryConfiguration.PARAM_VALUE_SEP)));
            
            if (!typeFilter.isEmpty()) {
                config.setDatatypeFilter(typeFilter);
                
                if (log.isDebugEnabled()) {
                    log.debug("Type Filter: " + typeFilter.toString());
                }
            }
        }
        
        // Get the MAX_RESULTS_OVERRIDE parameter if given
        if (null != settings.findParameter(QueryParameters.MAX_RESULTS_OVERRIDE)
                        && null != settings.findParameter(QueryParameters.MAX_RESULTS_OVERRIDE).getParameterValue()
                        && !settings.findParameter(QueryParameters.MAX_RESULTS_OVERRIDE).getParameterValue().isEmpty()) {
            try {
                long override = Long.parseLong(settings.findParameter(QueryParameters.MAX_RESULTS_OVERRIDE).getParameterValue());
                
                if (override < config.getMaxQueryResults()) {
                    config.setMaxQueryResults(override);
                    // this.maxresults is initially set to the value in the config, we are overriding it here for this instance
                    // of the query.
                    this.setMaxResults(override);
                }
            } catch (NumberFormatException nfe) {
                log.error(QueryParameters.MAX_RESULTS_OVERRIDE + " query parameter is not a valid number: "
                                + settings.findParameter(QueryParameters.MAX_RESULTS_OVERRIDE).getParameterValue() + ", using default value");
            }
        }
        
        if (log.isDebugEnabled()) {
            log.debug("Max Results: " + config.getMaxQueryResults());
        }
        
        // Get the INCLUDE_DATATYPE_AS_FIELD spring setting
        if (((null != settings.findParameter(QueryParameters.INCLUDE_DATATYPE_AS_FIELD)) && Boolean.valueOf(settings.findParameter(
                        QueryParameters.INCLUDE_DATATYPE_AS_FIELD).getParameterValue()))
                        || this.getIncludeDataTypeAsField()) {
            config.setIncludeDataTypeAsField(true);
        }
        
        // Get the INCLUDE_HIERARCHY_FIELDS spring setting
        if (((null != settings.findParameter(QueryParameters.INCLUDE_HIERARCHY_FIELDS)) && Boolean.valueOf(settings.findParameter(
                        QueryParameters.INCLUDE_HIERARCHY_FIELDS).getParameterValue()))
                        || this.getIncludeHierarchyFields()) {
            config.setIncludeHierarchyFields(true);
        }
        
        // Get the include.grouping.context = true/false spring setting
        if (((null != settings.findParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT)) && Boolean.valueOf(settings.findParameter(
                        QueryParameters.INCLUDE_GROUPING_CONTEXT).getParameterValue()))
                        || this.getIncludeGroupingContext()) {
            config.setIncludeGroupingContext(true);
        }
        
        // The analyzer provides the interface to rewrite the query as necessary
        DatawaveQueryAnalyzer analyzer = new DatawaveQueryAnalyzer();
        
        DatawaveTreeNode root = analyzer.parseJexlQuery(queryString);
        log.debug("parseTree: " + root.getContents());
        root = analyzer.applyCaseSensitivity(root, true, false);
        
        // Find unfielded terms, and fully qualify them with an OR of all fields found in the index
        root = fixUnfieldedQuery(config, root);
        if (root.getChildCount() == 0) {
            log.info("Unfielded query had zero matches in the index, returning no results, query: " + queryData.getQuery());
            return;
        }
        
        // root is the query to be used by the evaluating iterator in the end
        // optimizedQuery is the query to be used by the optimized query path
        DatawaveTreeNode optimizedQuery = analyzer.copyTree(root);
        
        // replace the functions nodes with corresponding index queries (if possible)
        // but only in the optimized query
        optimizedQuery = analyzer.replaceFunctionNodes(optimizedQuery, metadataHelper);
        
        // Drop those terms which reference non-existent fields, or throw exception if query
        // will return 0 or all results because of this
        try {
            optimizedQuery = analyzer.pruneMissingFields(optimizedQuery, config, metadataHelper);
        } catch (InvalidQueryException e) {
            log.info("Should we allow all or nothing? " + (isAllowAllOrNothingQuery() ? "Yes!" : "No!"));
            if (!allowAllOrNothingQuery) {
                throw e;
            }
        }
        
        // Get all the field names from the query, we will use this to determine which eventFields
        // in the query are indexed by looking at the metadata table. Using the optimized query for the list
        // as function nodes may have been turned into a field query.
        // The query term object contains the operator, whether its negated or not, and the literal to test against.
        Multimap<String,DatawaveTreeNode> optimizedFieldNameToNodeMap = analyzer.getFieldNameToNodeMap(optimizedQuery);
        Set<String> queryFieldNames = new HashSet<>(optimizedFieldNameToNodeMap.keySet());
        
        if (log.isDebugEnabled()) {
            if (log.isTraceEnabled()) {
                log.trace("fieldnames in query: " + new ArrayList<>(queryFieldNames));
            }
            
            log.debug("query: " + queryString);
            log.debug("get FieldNames: " + queryFieldNames);
            log.debug("credentials: " + auths);
        }
        
        // NOTE: It is best practice to always look field up in the DatawaveMetadata table
        // regardless of whether or not you plan to prune a field out of an optimized query later.
        // Fetch the mapping of fields to Types from the DatawaveMetadata table
        Multimap<String,Type<?>> indexedFieldsNormalizerMap = findIndexedTerms(config, metadataHelper, queryFieldNames);
        Multimap<String,Type<?>> allFieldsNormalizerMap = HashMultimap.create(indexedFieldsNormalizerMap);
        
        // save off the indexed field names derived from this map
        Set<String> indexedFieldNames = new HashSet<>(indexedFieldsNormalizerMap.keySet());
        Set<String> allFields = metadataHelper.getAllFields(config.getDatatypeFilter());
        
        // we need to normalize fields that appear as numeric values in a query
        Collection<String> unindexedNumerics = findUnindexedNumerics(root, allFields, indexedFieldNames);
        NumberType numberNormalizer = new NumberType();
        for (String unindexedNumeric : unindexedNumerics) {
            allFieldsNormalizerMap.put(unindexedNumeric, numberNormalizer);
        }
        
        // Store that map into the configuration
        config.setIndexedFieldsDataTypes(allFieldsNormalizerMap);
        
        // For each field, apply all the normalizers for that field to all of that field's terms
        // we do this on both forms of the query as the optimized query may have additional fields
        // to normalize based on the function replacement done above
        try {
            root = analyzer.normalizeFieldsInQuery(root, allFieldsNormalizerMap, metadataHelper.getMetadata());
            queryData.setQuery(analyzer.rebuildQueryFromTree(root));
            config.setQueryString(queryData.getQuery());
            
            optimizedQuery = analyzer.normalizeFieldsInQuery(optimizedQuery, indexedFieldsNormalizerMap, metadataHelper.getMetadata());
        } catch (Exception ex) {
            log.warn("could not normalizeFieldsInQuery, attempting to continue: " + queryString, ex);
        }
        
        // Loads the indexOnly fields as configured in the DatawaveMetadata table
        // Does internal caching from the metadata table and stores the result in the config object
        loadIndexOnlyFields(config, metadataHelper, optimizedFieldNameToNodeMap);
        
        // Mark fields in optimizedQuery which are indexOnly/unevaluated
        DatawaveQueryAnalyzer.markIndexOnlyFields(optimizedQuery, config.getUnevaluatedFields());
        
        boolean unsupportedOperatorSpecified = queryHasUnsupportedOperator(config, optimizedFieldNameToNodeMap);
        
        // If you encounter an unsupported operator exit.
        // we should probably do this much earlier. Previously if JEXL supported
        // it, we'd allow it to fall through in the FullTablescan and get picked up
        // there. But not we're not sure if we should honor it.
        if (unsupportedOperatorSpecified) {
            log.error("Encountered an Unsupported operator, throwing exception");
            throw new RuntimeException("Encountered an unsupported operator in the query.");
        }
        
        // Print out the current status of query parsing/handling
        if (log.isDebugEnabled()) {
            log.debug(" unsupportedOperators: " + unsupportedOperatorSpecified + " indexedFields: " + config.getIndexedFieldsDataTypesAsString());
        }
        
        // and now test whether
        if (analyzer.isOptimizedQuery(optimizedQuery, indexedFieldNames,
                        config.getUnevaluatedFields() != null ? config.getUnevaluatedFields() : Collections.<String> emptySet())) {
            log.debug("runServerQuery(), attempting optimized query branch.");
            
            RangeCalculator calc;
            
            // Get information from the global index for the indexed terms. The results object will contain the term
            // mapped to an object that contains the total count, and shards where this term is located.
            try {
                
                try {
                    // Use the indexStats table to remove fields that are too common
                    removeLowSelectivityFields(config, indexedFieldNames);
                    
                    // Send in the set of indexedFieldNames and the map of FieldName->nodes.
                    // only remove a field name if all occurrences of it in the query are negated.
                    removeNegatedFields(indexedFieldNames, optimizedFieldNameToNodeMap);
                    
                    // Lets do the range calculations (see RangeCalculator.execute)
                    calc = this.getTermIndexInformation(config, indexedFieldNames, optimizedQuery);
                    
                } catch (RangeExpansionException ree) {
                    QueryException qe = new QueryException(DatawaveErrorCode.RANGE_CALCULATION_ERROR, ree);
                    log.warn(qe);
                    throw new DoNotPerformOptimizedQueryException(qe);
                } catch (TooManyTermsException tmte) {
                    log.error(tmte.getMessage(), tmte);
                    throw tmte;
                }
                
                if (null == calc.getResult()) {
                    QueryException qe = new QueryException(DatawaveErrorCode.RANGECALCULATOR_RESULT_NULL);
                    log.debug(qe);
                    throw new DoNotPerformOptimizedQueryException(qe);
                } else if (calc.getResult().isEmpty()) {
                    // no results return empty results object
                    return;
                }
                
                if (null != calc.getResult() || !calc.getResult().isEmpty()) {
                    
                    optimizedQuery = calc.getQueryNode();
                    
                    // if we are using the returnUidMapper, then we could have duplicate events being returned.
                    // The evaluating iterator will dedup within the same document tree, however that is only if
                    // we do not have separate ranges within the same document tree. So lets collapse the ranges
                    // within a document if the returnUidMapper is set.
                    if (this.returnUidMapperClass != null) {
                        queryData.setRanges(collapseRangesWithinDocument(calc.getResult(), false));
                    } else {
                        queryData.setRanges(calc.getResult());
                    }
                    
                    config.setRanges(queryData.getRanges());
                    config.setQueryTree(optimizedQuery);
                    
                    if (log.isDebugEnabled()) {
                        log.debug("optimizedQuery from RangeCalc: " + optimizedQuery.getContents());
                        
                        if (log.isTraceEnabled()) {
                            log.trace("RangeCalculator, ranges: " + calc.getResult());
                        }
                    }
                } else {
                    // We didn't find anything in the index for this query. This may happen for an indexed term that has wildcards
                    // in unhandled locations.
                    
                    // Break out of here by throwing a named exception and do full scan
                    if (log.isDebugEnabled()) {
                        log.debug("runServerQuery(), RangeCalculator is empty.");
                    }
                    queryData.setRanges(new ArrayList<Range>(0));
                    config.setRanges(queryData.getRanges());
                }
            } catch (TableNotFoundException e) {
                log.error(this.getIndexTableName() + "not found", e);
                throw new RuntimeException(this.getIndexTableName() + "not found", e);
            } catch (org.apache.commons.jexl2.parser.ParseException e) {
                throw new RuntimeException("Error determining ranges for query: " + queryString, e);
            } catch (DoNotPerformOptimizedQueryException e) {
                log.warn("Could not perform an optimized query. Attempting to fallover to full-table scan");
            }
        }
        
        IteratorSetting scanIterator = null;
        
        // Determine if we should proceed with optimized query based on results from the global index
        if (null != queryData.getRanges() && !queryData.getRanges().isEmpty() && null != optimizedQuery) {
            if (log.isDebugEnabled()) {
                log.debug(" Performing optimized query");
            }
            
            // Set the EventOptimization for the boolean logic path
            scanIterator = new IteratorSetting(config.getBaseIteratorPriority() + 22, "eval", ShardEventOptimizationIterator.class.getName());
            
            queryData.addIterator(scanIterator);
            
            for (Map.Entry<String,String> option : metadataHelper.getMetadata().getOptions().entrySet()) {
                scanIterator.addOption(option.getKey(), option.getValue());
            }
            
            // For unevaluated field queries, we need to force down the BooleanLogic Path, which
            // takes a range of just Rows, and not down to the UID, so we
            // will need to trim the ranges if they have uids and dedup.
            if (!config.getUnevaluatedFields().isEmpty()) {
                
                // we will propagate values for unevaluated fields from the BooleanLogicIteratorJexl to the EvaluatingIteratorJexl, so
                // it is no longer necessary to replace unevaluated expressions.
                String evaluationQueryString = analyzer.rebuildQueryFromTree(root);
                
                config.setEvaluationQuery(evaluationQueryString);
                
                scanIterator.addOption(ShardEventOptimizationIterator.CONTAINS_UNEVALUATED_FIELDS, ShardEventOptimizationIterator.CONTAINS_UNEVALUATED_FIELDS);
                scanIterator.addOption(QueryParameters.UNEVALUATED_FIELDS, config.getUnevaluatedFieldsAsString());
                
                if (log.isDebugEnabled()) {
                    log.debug("Contains unevaluated field, adding notification for ShardEventOptimizationIterator to the options map.");
                }
            }
            
            // Create BatchScanner, set the ranges, and setup the iterators.
            
            // Setup the ReadAheadIterator if enabled
            if (config.getUseReadAheadIterator()) {
                if (log.isDebugEnabled()) {
                    log.debug("Enabling read ahead iterator with queue size: " + this.readAheadQueueSize + " and timeout: " + this.readAheadTimeOut);
                }
                
                scanIterator.addOption(ReadAheadIterator.QUEUE_SIZE, config.getReadAheadQueueSize().toString());
                scanIterator.addOption(ReadAheadIterator.TIMEOUT, config.getReadAheadTimeOut().toString());
            }
            
            // we need to provided a modified query to the BooleanLogicIterator
            // e.g. not unfielded terms, discrete ranges, consisting of only normalized, non-regex, non-function terms
            String fieldIndexQuery = analyzer.rebuildQueryFromTree(optimizedQuery);
            
            if (log.isDebugEnabled()) {
                log.debug("runServerQuery, FieldIndex Query: " + fieldIndexQuery);
            }
            
            config.setFieldIndexQuery(fieldIndexQuery);
            scanIterator.addOption(EnrichingIterator.QUERY, fieldIndexQuery);
            scanIterator.addOption(BooleanLogicIteratorJexl.FIELD_INDEX_QUERY, fieldIndexQuery);
            
            // For the optimized path, we can filter out events not in a set of datatypes above the entire iterator
            // stack
            // TODO Fix our iterators to efficiently filter out datatypes at the boolean logic level
            if (null != config.getDatatypeFilter() && config.getDatatypeFilter().size() > 0) {
                String typeFilterString = config.getDatatypeFilterAsString();
                
                if (log.isDebugEnabled()) {
                    log.debug("Setting DatatypeFilterIterator to: " + typeFilterString);
                }
                
                IteratorSetting datatypeFilterIterator = new IteratorSetting(config.getBaseIteratorPriority() + 23, "typeFilter",
                                DatatypeFilterIterator.class.getName());
                datatypeFilterIterator.addOption(DatatypeFilterIterator.DATATYPE_FILTER, typeFilterString);
                
                queryData.addIterator(datatypeFilterIterator);
            }
        } else {
            
            if (!config.getFullTableScanEnabled()) {
                throw new QueryException(DatawaveErrorCode.FULL_SCAN_DISABLED);
            }
            
            Multimap<String,DatawaveTreeNode> terms = analyzer.getFieldNameToNodeMap(root);
            
            // Check if the query contains any unevaluated/IndexOnly fields.
            // If it does we cannot run a full table scan.
            if (config.getUnevaluatedFields() != null && !config.getUnevaluatedFields().isEmpty()) {
                Set<String> problems = new HashSet<>(terms.keySet());
                problems.retainAll(config.getUnevaluatedFields());
                if (problems.size() > 0) {
                    throw new QueryException(DatawaveErrorCode.FULL_SCAN_IMPOSSIBLE);
                }
            }
            log.warn(" Performing FULL SCAN query: " + queryData.getQuery());
            
            // Set up a full scan using the date ranges from the query
            
            // The ranges are the start and end dates
            Collection<Range> r = getFullScanRange(config, terms);
            queryData.setRanges(r);
            
            if (log.isDebugEnabled()) {
                log.debug(" Ranges: count: " + queryData.getRanges().size() + ", " + queryData.getRanges().toString());
            }
            
            // For the non-optimized path, we can filter out events beneath the EvaluatingIterator
            if (null != config.getDatatypeFilter() && config.getDatatypeFilter().size() > 0) {
                String typeFilterString = config.getDatatypeFilterAsString();
                
                if (log.isDebugEnabled()) {
                    log.debug("Setting DatatypeFilterIterator to: " + typeFilterString);
                }
                
                IteratorSetting datatypeFilterIterator = new IteratorSetting(config.getBaseIteratorPriority() + 22, "typeFilter",
                                DatatypeFilterIterator.class.getName());
                datatypeFilterIterator.addOption(DatatypeFilterIterator.DATATYPE_FILTER, typeFilterString);
                // ShardScanIterator datatypeFilterIterator = new ShardScanIterator(22, DatatypeFilterIterator.class.getName(), "typeFilter");
                
                queryData.addIterator(datatypeFilterIterator);
            }
            
            // Set the evaluating iterator for the full-scan path
            scanIterator = new IteratorSetting(config.getBaseIteratorPriority() + 23, "eval", EvaluatingIterator.class.getName());
            
            for (Map.Entry<String,String> option : metadataHelper.getMetadata().getOptions().entrySet()) {
                scanIterator.addOption(option.getKey(), option.getValue());
            }
            
            queryData.addIterator(scanIterator);
        }
        // Load enrichers, filters, unevaluatedExpressions, and projection fields
        setCommonIteratorOptions(config, queryData, scanIterator, settings);
        
        config.setQueries(Collections.singleton(queryData).iterator());
    }
    
    /**
     * Collapse ranges that refer to documents within the same document tree (i.e. the same baseUid)
     *
     * @param ranges
     * @param reqireRangeOverlapToCollapse
     *            if true then ranges must overlap inorder to collapse, otherwise one range is created per shard, datatype, base uid combination
     * @return a collapsed set of ranges
     */
    protected List<Range> collapseRangesWithinDocument(Set<Range> ranges, boolean requireRangeOverlapToCollapse) {
        List<Range> newRanges = new ArrayList<>();
        Multimap<String,Range> rangesByUid = HashMultimap.create();
        
        // extract out the short circuited ranges (those containing a UID)
        for (Range range : ranges) {
            boolean mappedByUid = false;
            Key key = range.getStartKey();
            if (key != null) {
                Text cf = key.getColumnFamily();
                if (cf != null) {
                    String cfStr = cf.toString();
                    int index = cfStr.indexOf(EvaluatingIterator.NULL_BYTE_STRING);
                    if (index > -1) {
                        String shardId = key.getRow().toString();
                        String dataType = cfStr.substring(0, index);
                        String baseUid = UID.parseBase(cfStr.substring(index + 1)).getBaseUid();
                        StringBuilder builder = new StringBuilder();
                        builder.append(shardId).append(EvaluatingIterator.NULL_BYTE_STRING).append(dataType).append(EvaluatingIterator.NULL_BYTE_STRING)
                                        .append(baseUid);
                        String uid = builder.toString();
                        rangesByUid.put(uid, range);
                        mappedByUid = true;
                    }
                }
            }
            if (!mappedByUid) {
                newRanges.add(range);
            }
        }
        
        // now collapse the uid ranges
        for (String key : new TreeSet<>(rangesByUid.keySet())) {
            // create a new range out of the minimum start key and the maximum end key
            Range current = null;
            for (Range range : new TreeSet<>(rangesByUid.get(key))) {
                if (current == null) {
                    current = range;
                } else if (!requireRangeOverlapToCollapse) {
                    current = new Range(current.getStartKey(), current.isStartKeyInclusive(), range.getEndKey(), range.isEndKeyInclusive());
                } else if (range.getStartKey().compareTo(current.getEndKey()) == 0) {
                    if (!current.isEndKeyInclusive() && !range.isStartKeyInclusive()) {
                        newRanges.add(current);
                        current = range;
                    } else {
                        current = new Range(current.getStartKey(), current.isStartKeyInclusive(), range.getEndKey(), range.isEndKeyInclusive());
                    }
                } else if (range.getStartKey().compareTo(current.getEndKey()) < 0) {
                    current = new Range(current.getStartKey(), current.isStartKeyInclusive(), range.getEndKey(), range.isEndKeyInclusive());
                } else {
                    newRanges.add(current);
                    current = range;
                }
            }
            if (current != null) {
                newRanges.add(current);
            }
        }
        
        return newRanges;
    }
    
    /**
     * Load the common iterator options for both the optimized and non-optimized query paths. Said options include: Enrichers, filters, unevaluatedExpressions,
     * begin/end datetimes, indexed fields and their normalizers, non-event key column families, and the query string
     *
     * @param config
     * @param scanIterator
     * @throws QueryException
     */
    protected void setCommonIteratorOptions(GenericShardQueryConfiguration config, QueryData queryData, IteratorSetting scanIterator, Query settings)
                    throws QueryException {
        // Use enrichers if they are configured and enabled
        if (config.getUseEnrichers() && null != config.getEnricherClassNames() && !config.getEnricherClassNames().isEmpty()) {
            initializeEnrichers(config, scanIterator);
            
            if (null != config.getEnricherClassNames()) {
                log.debug("Enricher class names: " + config.getEnricherClassNames());
            }
        } else if (log.isDebugEnabled()) {
            log.debug("Not enriching results.");
        }
        
        // Use filters if they are configured and enabled
        if (config.getUseFilters() && null != config.getFilterClassNames() && !config.getFilterClassNames().isEmpty()) {
            initializeFilters(config, scanIterator);
            
            if (null != this.getFilterClassNames()) {
                log.debug("Filter class names: " + this.getFilterClassNames());
            }
        } else if (log.isDebugEnabled()) {
            log.debug("Not filtering results.");
            
        }
        
        if (null != config.getProjectFields() && !config.getProjectFields().isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Setting scan option: " + QueryParameters.RETURN_FIELDS + " to " + config.getProjectFieldsAsString());
            }
            
            scanIterator.addOption(QueryParameters.RETURN_FIELDS, config.getProjectFieldsAsString());
        }
        
        if (null != config.getBlacklistedFields() && !config.getBlacklistedFields().isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Setting scan option: " + QueryParameters.BLACKLISTED_FIELDS + " to " + config.getBlacklistedFieldsAsString());
            }
            
            scanIterator.addOption(QueryParameters.BLACKLISTED_FIELDS, config.getBlacklistedFieldsAsString());
        }
        
        // Set the start and end dates
        scanIterator.addOption(Constants.START_DATE, Long.toString(config.getBeginDate().getTime()));
        
        if (config.getBeginDate().getTime() == config.getEndDate().getTime()) { // for single day, extend endDate to the 1 second before midnight
            Date endDate = config.getEndDate();
            endDate.setTime(endDate.getTime() + 86399000);
            
            config.setEndDate(endDate);
        }
        
        if (log.isDebugEnabled()) {
            log.debug("beginDate: " + config.getBeginDate().getTime());
            log.debug("endDate:   " + config.getEndDate().getTime());
        }
        
        scanIterator.addOption(Constants.END_DATE, Long.toString(config.getEndDate().getTime()));
        
        // Set the query option. Having a non-null EvaluationQuery implies the optimized path
        if (null != config.getEvaluationQuery() && !config.getEvaluationQuery().isEmpty()) {
            // Setting the query back into the params map for full-scan path seems to be unnecessary
            scanIterator.addOption(EvaluatingIterator.QUERY_OPTION, config.getEvaluationQuery());
        } else {
            scanIterator.addOption(EvaluatingIterator.QUERY_OPTION, queryData.getQuery());
        }
        
        // Pass down the map of INDEXED_FIELDS:[normalizer,normalizer]...
        if (null != config.getIndexedFieldsDataTypes() && !config.getIndexedFieldsDataTypes().isEmpty()) {
            scanIterator.addOption(EvaluatingIterator.NORMALIZER_LIST, config.getIndexedFieldsDataTypesAsString());
        }
        
        // Set the list of nonEventKeyPrefixes
        if (null != config.getNonEventKeyPrefixes() && !config.getNonEventKeyPrefixes().isEmpty()) {
            scanIterator.addOption(QueryParameters.NON_EVENT_KEY_PREFIXES, config.getNonEventKeyPrefixesAsString());
        }
        
        // Specify a list of fields not to return from a query
        if (null != config.getBlacklistedFields() && !config.getBlacklistedFields().isEmpty()) {
            log.debug("Setting option on ScanIterator to use EvaluatingIterator.BLACKLISTED_FIELDS");
            scanIterator.addOption(QueryParameters.BLACKLISTED_FIELDS, config.getBlacklistedFieldsAsString());
        }
        
        // Include the EVENT_DATATYPE as a field
        if (config.getIncludeDataTypeAsField()) {
            scanIterator.addOption(QueryParameters.INCLUDE_DATATYPE_AS_FIELD, Boolean.toString(true));
        }
        
        // Include the hierarchy fields
        if (config.getIncludeHierarchyFields()) {
            scanIterator.addOption(QueryParameters.INCLUDE_CHILD_COUNT, Boolean.toString(true));
            scanIterator.addOption(QueryParameters.INCLUDE_PARENT, Boolean.toString(true));
        }
        
        // Include the grouping context
        if (config.getIncludeGroupingContext()) {
            scanIterator.addOption(QueryParameters.INCLUDE_GROUPING_CONTEXT, Boolean.toString(true));
        }
        
        // set the return uid mapper class if any
        if (config.getReturnUidMapperClass() != null) {
            scanIterator.addOption(QueryParameters.RETURN_UID_MAPPER, config.getReturnUidMapperClass());
        }
        
        // setup the uid mapper class if specified
        if (config.getUidMapperClass() != null) {
            scanIterator.addOption(ShardUidMappingIterator.UID_MAPPER, config.getUidMapperClass());
        }
    }
    
    public static Collection<Type<?>> combineNormalizers(Collection<Type<?>> dataTypes, GenericShardQueryConfiguration config, MetadataHelper metadataHelper) {
        Set<Type<?>> combined = new HashSet<>();
        
        if (null != dataTypes) {
            combined.addAll(dataTypes);
        }
        
        try {
            Collection<Type<?>> metadataDataTypes = metadataHelper.getDatatypesForField(null, config.getDatatypeFilter());
            if (null != metadataDataTypes) {
                combined.addAll(metadataDataTypes);
            }
        } catch (InstantiationException | TableNotFoundException | IllegalAccessException e) {
            log.error(e.getMessage());
        }
        
        return combined;
    }
    
    /**
     * Analyze the query and check for 'unfielded' values i.e. 'red' versus COLOR == 'red' unfielded values will be looked up in the global index to find all
     * matching FieldName designators and be OR'd into the query. i.e. COLOR == 'red' or INDICATOR == 'red' or NAME == 'red'
     *
     * @param config
     * @param analyzer
     * @param root
     * @param connection
     * @param auths
     * @return
     * @throws QueryException
     */
    protected DatawaveTreeNode fixUnfieldedQuery(GenericShardQueryConfiguration config, DatawaveTreeNode root) throws InstantiationException,
                    IllegalAccessException {
        
        log.debug("Checking query for _ANYFIELD_ values.");
        DatawaveQueryAnalyzer analyzer = new DatawaveQueryAnalyzer();
        Multimap<String,DatawaveTreeNode> fieldNameToNodeMap = analyzer.getFieldNameToNodeMap(root);
        
        if (fieldNameToNodeMap.containsKey(Constants.ANY_FIELD)) {
            
            // Set up all normalizers on config object.
            Collection<Type<?>> types = combineNormalizers(this.dataTypes, config, this.metadataHelper);
            config.setDataTypes(types);
            
            // Look unfielded values up in the global index, applies normalizers that are set in ShardQueryTable
            Map<String,Set<String>> unFieldedValueMapping = this.getFieldValueMappingsFromGlobalIndex(fieldNameToNodeMap, config);
            if (log.isDebugEnabled()) {
                log.debug("unfieldedValueMapping: " + unFieldedValueMapping);
            }
            
            // Apply the FieldNames to the values.
            // if there are no matches or it's an AND node which will always
            // return false, then we'll get a head node with no children.
            root = analyzer.fixANYFIELDValues(root, unFieldedValueMapping);
            
            // root has no children, exit early.
            if (root.getChildCount() == 0) {
                if (log.isDebugEnabled()) {
                    log.debug("Query with _ANYFIELD_ values, found, returning empty results.");
                }
                return root;
                // throw new QueryException("No results found in the Index for this unfielded query: "+queryString);
            }
            
            if (log.isDebugEnabled()) {
                log.debug(Constants.ANY_FIELD + " Value was detected, new query: " + analyzer.rebuildQueryFromTree(root));
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("No _ANYFIELD_ nodes: " + analyzer.rebuildQueryFromTree(root));
            }
        }
        
        return root;
    }
    
    /**
     * Remove any negated fields from the fields list, we don't want to lookup negated fields in the global index since that's wasted effort. However, they
     * needed to be looked up in metadata table to get their normalizer for the QueryEvaluator NOTE! the negations must be pure negations, if the field exists
     * as both a positive and negative in the query and you remove it, it's possible the query will not return the correct results.
     *
     * @param indexedFieldNames
     *            A set of fieldNames which are indexed, this method will update this set.
     * @param terms
     *            A mapping of terms to DatawaveTreeNodes
     */
    protected void removeNegatedFields(Set<String> indexedFieldNames, Multimap<String,DatawaveTreeNode> terms) {
        Set<String> pureNegations = new HashSet<>();
        
        for (String t : indexedFieldNames) {
            boolean allNegations = true;
            
            for (DatawaveTreeNode qt : terms.get(t)) {
                if (!qt.isNegated()) {
                    allNegations = false;
                    break;
                }
            }
            
            if (allNegations) {
                pureNegations.add(t);
            }
        }
        
        // remove the pure negations from the fields map, we don't want the global index to look them up.
        indexedFieldNames.removeAll(pureNegations);
    }
    
    /**
     * Merges the index only fields specified in the Metadata table with any fields configured on <code>this</code> and stores the result in the
     * <code>config</code> parameter
     *
     * @param config
     * @param metadataHelper
     * @throws TableNotFoundException
     * @throws ExecutionException
     */
    protected void loadIndexOnlyFields(GenericShardQueryConfiguration config, MetadataHelper metadataHelper, Multimap<String,DatawaveTreeNode> terms)
                    throws TableNotFoundException, ExecutionException {
        // DatawaveMetadataHelper internally caches the results for 24hrs
        Set<String> indexOnlyFields = Sets.newHashSet(metadataHelper.getIndexOnlyFields(config.getDatatypeFilter()));
        
        if (log.isTraceEnabled()) {
            log.trace("Complete list of unevaluated/index-only fields: " + indexOnlyFields);
        }
        
        // Allow users to still specify indexOnly fields via the Spring configuration that will
        // add to the list we calculated off of the metadata table
        if (null != this.getUnevaluatedFields() && this.getUnevaluatedFields().size() > 0) {
            if (log.isTraceEnabled()) {
                log.trace("Adding pre-configured unevaluated/index-only fields: " + this.getUnevaluatedFields());
            }
            
            indexOnlyFields.addAll(this.getUnevaluatedFields());
        }
        
        if (log.isTraceEnabled()) {
            log.trace("Removing all but the following query field names: " + terms.keySet());
        }
        
        // Get the set of index-only fields in the query
        indexOnlyFields.retainAll(terms.keySet());
        
        if (log.isDebugEnabled()) {
            log.debug("Setting unevaluated/index-only fields for this query to: " + indexOnlyFields);
        }
        
        // Sets the results on the config object
        config.setUnevaluatedFields(indexOnlyFields);
        this.unevaluatedFields = new ArrayList<>(indexOnlyFields);
    }
    
    /**
     * Check the query terms for unsupported operators and mark any QueryTerms that match a FieldName which should not be included in the QueryEvaluation (NOTE:
     * unevaluatedFields was added in for content queries because aggregating all keys for content takes a long time. Easier to let BooleanLogicIterator check
     * the FieldIndex keys and skip them when the Evaluator rechecks against original query.
     *
     * @param config
     * @param terms
     * @return True/false whether or not the query contains an unsupported operator
     */
    protected boolean queryHasUnsupportedOperator(GenericShardQueryConfiguration config, Multimap<String,DatawaveTreeNode> terms) {
        for (Entry<String,DatawaveTreeNode> entry : terms.entries()) {
            if (null == entry.getValue()) {
                continue;
            }
            
            int operator = JexlOperatorConstants.getJJTNodeType(entry.getValue().getOperator());
            if (!(operator == ParserTreeConstants.JJTEQNODE || operator == ParserTreeConstants.JJTNENODE || operator == ParserTreeConstants.JJTLENODE
                            || operator == ParserTreeConstants.JJTLTNODE || operator == ParserTreeConstants.JJTGENODE
                            || operator == ParserTreeConstants.JJTGTNODE || operator == ParserTreeConstants.JJTERNODE
                            || operator == ParserTreeConstants.JJTNRNODE || operator == ParserTreeConstants.JJTFUNCTIONNODE)) {
                
                if (log.isDebugEnabled()) {
                    log.debug("unsupportedOperator " + operator + "  on node: " + entry.getValue().getContents() + "  node type: " + entry.getValue().getType());
                }
                
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Set the scan iterator options on the {@link BatchScanner} to enable the enrichment classes
     *
     * @param config
     *            The ShardQueryConfiguration object
     * @param scanIterator
     *            The ShardScanIterator on which these options should be set
     * @throws QueryException
     */
    protected void initializeEnrichers(GenericShardQueryConfiguration config, IteratorSetting scanIterator) throws QueryException {
        // Sanity check
        if (!this.useEnrichers || this.getEnricherClassNames() == null || this.getEnricherClassNames().size() == 0) {
            return;
        }
        
        log.debug("Enriching results");
        
        scanIterator.addOption(EnrichingMaster.ENRICHMENT_ENABLED, "true");
        
        StringBuilder sb = new StringBuilder();
        for (String className : this.getEnricherClassNames()) {
            sb.append(className);
            sb.append(GenericShardQueryConfiguration.PARAM_VALUE_SEP);
        }
        
        // Sanity check
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
            
            scanIterator.addOption(EnrichingMaster.ENRICHMENT_CLASSES, sb.toString());
            
            sb.setLength(0);
            for (String field : config.getUnevaluatedFields()) {
                sb.append(field);
                sb.append(GenericShardQueryConfiguration.PARAM_VALUE_SEP);
            }
            
            if (sb.length() > 0) {
                sb.setLength(sb.length() - 1);
                
                scanIterator.addOption(EnrichingMaster.UNEVALUATED_FIELDS, sb.toString());
            }
        }
    }
    
    /**
     * Set the scan iterator options on the {@link BatchScanner} to enable the filtering classes
     *
     * @param config
     *            The ShardQueryConfiguration object
     * @param scanIterator
     *            The ShardScanIterator on which these options should be set
     * @throws QueryException
     */
    protected void initializeFilters(GenericShardQueryConfiguration config, IteratorSetting scanIterator) throws QueryException {
        // Sanity check
        if (!this.useFilters || this.getFilterClassNames() == null || this.getFilterClassNames().size() == 0) {
            return;
        }
        
        log.debug("Filtering results");
        
        scanIterator.addOption(QueryParameters.FILTERING_ENABLED, "true");
        
        // Build the filtering classes list
        StringBuilder sb = new StringBuilder();
        for (String className : this.getFilterClassNames()) {
            sb.append(className);
            sb.append(GenericShardQueryConfiguration.PARAM_VALUE_SEP);
        }
        
        // Sanity check
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
            
            scanIterator.addOption(QueryParameters.FILTERING_CLASSES, sb.toString());
            
            // Build unevaluatedFields list
            sb.setLength(0);
            for (String field : config.getUnevaluatedFields()) {
                sb.append(field);
                sb.append(GenericShardQueryConfiguration.PARAM_VALUE_SEP);
            }
            
            if (sb.length() > 0) {
                sb.setLength(sb.length() - 1);
                
                scanIterator.addOption(QueryParameters.UNEVALUATED_FIELDS, sb.toString());
            }
        }
    }
    
    public String getNonEventKeyColFams() {
        return nonEventKeyColFams;
    }
    
    public void setNonEventKeyColFams(String nonEventKeyColFams) {
        this.nonEventKeyColFams = nonEventKeyColFams;
    }
    
    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }
    
    @Override
    public abstract QueryLogicTransformer getTransformer(Query settings);
    
    /**
     * Removes fields with selectivity less than the configured minimum.
     *
     * This method does nothing if minSelectivity is less than 0 or never set.
     *
     * @param config
     * @param fields
     */
    protected void removeLowSelectivityFields(GenericShardQueryConfiguration config, Set<String> fields) {
        if (config.getMinSelectivity() == -1.0) {
            if (log.isDebugEnabled()) {
                log.debug("Minimum selectivity is not set-- skipping stats.");
            }
            
            return;
        }
        
        // We cannot remove index only fields
        Set<String> selectivityFields = new HashSet<>(fields);
        selectivityFields.removeAll(config.getUnevaluatedFields());
        
        if (log.isDebugEnabled()) {
            log.debug("Applying index statistics to cull low selectivity fields");
        }
        
        if (config.getConnector().tableOperations().exists(this.statsTable)) {
            IndexStatsClient stats = new IndexStatsClient(config.getConnector(), this.statsTable);
            Map<String,Double> fieldSelectivities;
            try {
                fieldSelectivities = stats.getStat(selectivityFields, config.getDatatypeFilter(), config.getBeginDate(), config.getEndDate());
            } catch (IOException e) {
                log.warn("Could not gather index statistics.", e);
                return;
            }
            for (Entry<String,Double> selectivity : fieldSelectivities.entrySet()) {
                if (selectivity.getValue() < config.getMinSelectivity()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Removing " + selectivity.getKey() + " from fields.");
                    }
                    
                    fields.remove(selectivity.getKey());
                }
            }
        } else {
            log.debug("Not gathering index stats because " + this.statsTable + " does not exist.");
        }
    }
    
    /**
     * Walks a query tree and finds clauses that have numeric values but unindexed fields.
     *
     * @return a set containing field names that have numeric values but are unindexed
     */
    public Set<String> findUnindexedNumerics(DatawaveTreeNode root, Set<String> allFields, Collection<String> indexedFields) {
        HashSet<String> fields = new HashSet<>();
        
        // let's do depth first
        Enumeration<?> children = root.children();
        while (children.hasMoreElements()) {
            DatawaveTreeNode node = (DatawaveTreeNode) children.nextElement();
            fields.addAll(findUnindexedNumerics(node, allFields, indexedFields));
        }
        
        // if we're at a range node, we need to try and parse both bounds
        if (!indexedFields.contains(root.getFieldName())) {
            if (root.isRangeNode()) {
                NumberFormat nf = NumberFormat.getInstance();
                String lower = root.getLowerBound();
                String upper = root.getUpperBound();
                try {
                    nf.parse(lower);
                    nf.parse(upper);
                    fields.add(root.getFieldName());
                } catch (ParseException e) {
                    log.warn("Failed to parse a possible numeric range. Lower: " + lower + ", Upper: " + upper, e);
                }
            } else {
                // Only modify the value if the "fieldName" is actually a fieldName
                if (root.getFieldValueLiteralType() != null && ASTNumberLiteral.class.isAssignableFrom(root.getFieldValueLiteralType())
                                && allFields.contains(root.getFieldName())) {
                    fields.add(root.getFieldName());
                }
            }
        }
        return fields;
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
    
    public void setBlacklistedFields(Set<String> blacklistedFields) {
        this.blacklistedFields = new HashSet<>(blacklistedFields);
    }
    
    public Set<String> getBlacklistedFields() {
        return this.blacklistedFields;
    }
    
    public String getBlacklistedFieldsString() {
        return org.apache.commons.lang.StringUtils.join(this.blacklistedFields, '/');
    }
    
    public boolean getIncludeGroupingContext() {
        return this.includeGroupingContext;
    }
    
    public void setIncludeGroupingContext(boolean opt) {
        this.includeGroupingContext = opt;
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
    
    public int getMaxTermExpansionThreshold() {
        return maxTermExpansionThreshold;
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
    
    public void setMaxTermExpansionThreshold(int maxTermExpansionThreshold) {
        this.maxTermExpansionThreshold = maxTermExpansionThreshold;
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
    
    public boolean isAllowAllOrNothingQuery() {
        return allowAllOrNothingQuery;
    }
    
    public void setAllowAllOrNothingQuery(boolean allowAllOrNothingQuery) {
        this.allowAllOrNothingQuery = allowAllOrNothingQuery;
    }
    
    public Map<String,QueryParser> getQuerySyntaxParsers() {
        return querySyntaxParsers;
    }
    
    public void setQuerySyntaxParsers(Map<String,QueryParser> querySyntaxParsers) {
        this.querySyntaxParsers = querySyntaxParsers;
    }
    
    public String getModelName() {
        return modelName;
    }
    
    public void setModelName(String modelName) {
        this.modelName = ((null != modelName) && !modelName.isEmpty()) ? modelName : null;
    }
    
    public String getModelTableName() {
        return modelTableName;
    }
    
    public void setModelTableName(String modelTableName) {
        this.modelTableName = ((null != modelTableName) && !modelTableName.isEmpty()) ? modelTableName : null;
    }
    
    public QueryModel getQueryModel() {
        return queryModel;
    }
    
    public void setQueryModel(QueryModel queryModel) {
        this.queryModel = queryModel;
    }
    
    protected Multimap<String,String> invertMultimap(Map<String,String> multi) {
        Multimap<String,String> inverse = HashMultimap.create();
        for (Entry<String,String> entry : multi.entrySet()) {
            inverse.put(entry.getValue(), entry.getKey());
        }
        return inverse;
    }
    
    public Collection<Type<?>> getDataTypes() {
        return dataTypes;
    }
    
    public void setDataTypes(Collection<Type<?>> dataTypes) {
        this.dataTypes = dataTypes;
    }
    
    public String getMandatoryQuerySyntax() {
        return mandatoryQuerySyntax;
    }
    
    public void setMandatoryQuerySyntax(String mandatoryQuerySyntax) {
        this.mandatoryQuerySyntax = mandatoryQuerySyntax;
    }
    
    /**
     * This method calls the base logic's close method, and then attempts to close all batch scanners tracked by the scanner factory, if it is not null. The
     * scanner factory can be null if a RunningQuery was unmarshalled from a distributed cache. The query logic in a RunneringQuery is transient, so the scanner
     * factory reference is lost.
     */
    @Override
    public void close() {
        super.close();
        if (scannerFactory == null) {
            log.debug("ScannerFactory is null; not closing it.");
        } else {
            int nClosed = 0;
            scannerFactory.lockdown();
            for (ScannerBase bs : Lists.newArrayList(scannerFactory.currentScanners())) {
                scannerFactory.close(bs);
                ++nClosed;
            }
            if (log.isDebugEnabled())
                log.debug("Cleaned up " + nClosed + " batch scanners associated with this query logic.");
        }
    }
    
    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> params = new TreeSet<>();
        params.add(QueryParameters.QUERY_SYNTAX);
        params.add(QueryParameters.PARAMETER_MODEL_NAME);
        params.add(QueryParameters.PARAMETER_MODEL_TABLE_NAME);
        params.add(QueryParameters.DATATYPE_FILTER_SET);
        params.add(QueryParameters.RETURN_FIELDS);
        params.add(QueryParameters.BLACKLISTED_FIELDS);
        params.add(QueryParameters.MAX_RESULTS_OVERRIDE);
        params.add(QueryParameters.INCLUDE_DATATYPE_AS_FIELD);
        params.add(QueryParameters.INCLUDE_HIERARCHY_FIELDS);
        params.add(QueryParameters.INCLUDE_GROUPING_CONTEXT);
        return params;
    }
    
}
