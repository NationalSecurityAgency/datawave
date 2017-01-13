package nsa.datawave.query.tables.shard;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import nsa.datawave.core.iterators.GlobalIndexFieldSummaryIterator;
import nsa.datawave.core.iterators.GlobalIndexTermMatchingIterator;
import nsa.datawave.core.iterators.filter.GlobalIndexDataTypeFilter;
import nsa.datawave.core.iterators.filter.GlobalIndexDateRangeFilter;
import nsa.datawave.data.type.Type;
import nsa.datawave.query.QueryParameters;
import nsa.datawave.query.config.GenericShardQueryConfiguration;
import nsa.datawave.query.config.ShardIndexQueryConfiguration;
import nsa.datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import nsa.datawave.query.language.tree.QueryNode;
import nsa.datawave.query.model.QueryModel;
import nsa.datawave.query.parser.DatawaveQueryAnalyzer;
import nsa.datawave.query.parser.DatawaveTreeNode;
import nsa.datawave.query.parser.JavaRegexAnalyzer;
import nsa.datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import nsa.datawave.query.parser.RangeCalculator;
import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.rewrite.iterator.UniqueColumnFamilyIterator;
import nsa.datawave.query.tables.ScannerFactory;
import nsa.datawave.query.tables.ShardQueryLogic;
import nsa.datawave.query.transformer.ShardIndexQueryTransformer;
import nsa.datawave.query.util.IteratorToSortedKeyValueIterator;
import nsa.datawave.query.util.MetadataHelper;
import nsa.datawave.query.util.MetadataHelperFactory;
import nsa.datawave.query.util.SortedKeyValueIteratorToIterator;
import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.configuration.GenericQueryConfiguration;
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
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;

/**
 * Query Table implementation that accepts a single term and returns information from the global index for that term. The response includes the number of
 * occurrences of the term by type by day.
 * 
 * Deprecated - see nsa.datawave.query.rewrite.discovery.DiscoveryLogic
 */
@Deprecated
public class ShardIndexQueryTable extends BaseQueryLogic<Entry<Key,org.apache.accumulo.core.data.Value>> {
    
    class TermInformation {
        // Maps type to date to count
        Map<String,HashMap<String,Long>> termInfo = new HashMap<>();
        
        void add(String type, String yyyymmdd, long count) {
            if (!termInfo.containsKey(type)) {
                termInfo.put(type, new HashMap<String,Long>());
            }
            Map<String,Long> dateAndCount = termInfo.get(type);
            if (!dateAndCount.containsKey(yyyymmdd)) {
                dateAndCount.put(yyyymmdd, count);
            } else {
                dateAndCount.put(yyyymmdd, count + dateAndCount.get(yyyymmdd));
            }
        }
        
        @Override
        public String toString() {
            return termInfo.toString();
        }
    }
    
    protected static final Logger log = Logger.getLogger(ShardIndexQueryTable.class);
    
    private String indexTableName;
    private String reverseIndexTableName;
    private Collection<Type<?>> dataTypes = null;
    private SimpleDateFormat shardDateFormatter = new SimpleDateFormat("yyyyMMdd");
    private boolean fullTableScanEnabled = true;
    private boolean allowLeadingWildcard = true;
    // If you want to show results with the field values separated out, then set this to true
    // Warning: setting this to true could result in very query results for certain regular exception queries
    private boolean returnIndexedValues = false;
    protected String modelName = "DATAWAVE";
    protected String modelTableName = "DatawaveMetadata";
    protected MetadataHelperFactory metadataHelperFactory;
    protected MetadataHelper metadataHelper;
    private String metadataTableName;
    private ScannerFactory scannerFactory;
    
    private QueryModel queryModel = null;
    
    public ShardIndexQueryTable() {
        super();
        shardDateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
    }
    
    public ShardIndexQueryTable(ShardIndexQueryTable other) {
        super(other);
        this.setIndexTableName(other.getIndexTableName());
        this.setReverseIndexTableName(other.getReverseIndexTableName());
        this.setFullTableScanEnabled(other.isFullTableScanEnabled());
        this.setAllowLeadingWildcard(other.isAllowLeadingWildcard());
        this.setReturnIndexedValues(other.isReturnIndexedValues());
        this.setQueryModel(other.queryModel);
        this.setModelName(other.modelName);
        this.setModelTableName(other.modelTableName);
        this.setMetadataHelperFactory(other.getMetadataHelperFactory());
        
        if (other.dataTypes != null) {
            List<Type<?>> copyDataTypes = new ArrayList<>(other.dataTypes);
            this.setDataTypes(copyDataTypes);
        }
    }
    
    @Override
    public ShardIndexQueryTable clone() {
        return new ShardIndexQueryTable(this);
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
    private void initializeMetadataHelper(Connector connector, String metadataTableName, Set<Authorizations> auths) throws TableNotFoundException,
                    ExecutionException {
        this.metadataHelper = this.metadataHelperFactory.createMetadataHelper();
        this.metadataHelper.initialize(connector, metadataTableName, auths);
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
    
    public String getMetadataTableName() {
        return metadataTableName;
    }
    
    public void setMetadataTableName(String metadataTableName) {
        this.metadataTableName = metadataTableName;
    }
    
    public MetadataHelperFactory getMetadataHelperFactory() {
        return metadataHelperFactory;
    }
    
    public void setMetadataHelperFactory(MetadataHelperFactory metadataHelperFactory) {
        this.metadataHelperFactory = metadataHelperFactory;
    }
    
    @Override
    public GenericQueryConfiguration initialize(Connector connection, Query settings, Set<Authorizations> auths) throws Exception {
        ShardIndexQueryConfiguration config = new ShardIndexQueryConfiguration(this);
        
        this.scannerFactory = new ScannerFactory(connection);
        
        if (null != this.getMetadataTableName() && !this.getMetadataTableName().trim().isEmpty()) {
            config.setMetadataTableName(this.getMetadataTableName());
        }
        
        initializeMetadataHelper(connection, config.getMetadataTableName(), auths);
        
        if (StringUtils.isEmpty(settings.getQuery())) {
            throw new IllegalArgumentException("Query cannot be null");
        }
        
        if (log.isDebugEnabled()) {
            log.debug("Query parameters set to " + settings.getParameters());
        }
        
        // Check if the default modelName and modelTableNames have been overriden by custom parameters.
        if (null != settings.findParameter(QueryParameters.PARAMETER_MODEL_NAME)
                        && !settings.findParameter(QueryParameters.PARAMETER_MODEL_NAME).getParameterValue().trim().isEmpty()) {
            modelName = settings.findParameter(QueryParameters.PARAMETER_MODEL_NAME).getParameterValue().trim();
        }
        if (null != settings.findParameter(QueryParameters.PARAMETER_MODEL_TABLE_NAME)
                        && !settings.findParameter(QueryParameters.PARAMETER_MODEL_TABLE_NAME).getParameterValue().trim().isEmpty()) {
            modelTableName = settings.findParameter(QueryParameters.PARAMETER_MODEL_TABLE_NAME).getParameterValue().trim();
        }
        
        this.queryModel = metadataHelper.getQueryModel(modelTableName, modelName, null);
        
        // get the data type filter set if any
        if (null != settings.findParameter(QueryParameters.DATATYPE_FILTER_SET)
                        && !settings.findParameter(QueryParameters.DATATYPE_FILTER_SET).getParameterValue().trim().isEmpty()) {
            Set<String> dataTypeFilter = new HashSet<>(Arrays.asList(StringUtils.split(settings.findParameter(QueryParameters.DATATYPE_FILTER_SET)
                            .getParameterValue().trim(), ShardIndexQueryConfiguration.PARAM_VALUE_SEP)));
            config.setDatatypeFilter(dataTypeFilter);
            if (log.isDebugEnabled()) {
                log.debug("Data type filter set to " + dataTypeFilter);
            }
        }
        
        // Set the connector
        config.setConnector(connection);
        
        // Set the auths
        config.setAuthorizations(auths);
        
        // set the table names
        if (getIndexTableName() != null)
            config.setIndexTableName(getIndexTableName());
        if (getReverseIndexTableName() != null)
            config.setReverseIndexTableName(getReverseIndexTableName());
        
        // Get the ranges
        config.setBeginDate(settings.getBeginDate());
        config.setEndDate(settings.getEndDate());
        
        if (null == config.getBeginDate() || null == config.getEndDate()) {
            config.setBeginDate(new Date(0));
            config.setEndDate(new Date(Long.MAX_VALUE));
            log.warn("Dates not specified, using entire date range");
        }
        
        // start with a trimmed version of the query, converted to JEXL
        LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
        parser.setAllowLeadingWildCard(this.isAllowLeadingWildcard());
        QueryNode node = parser.parse(settings.getQuery().trim());
        // TODO: Validate that this is a simple list of terms type of query
        config.setQueryString(node.getOriginalQuery());
        if (log.isDebugEnabled()) {
            log.debug("Original Query = " + settings.getQuery().trim());
            log.debug("JEXL Query = " + node.getOriginalQuery());
        }
        
        // Combine any Spring configured noramzliers with those in DatawaveMetadata table
        Collection<Type<?>> allDataTypes = ShardQueryLogic.combineNormalizers(this.dataTypes, config, metadataHelper);
        config.setDataTypes(allDataTypes);
        
        // get the normalized terms
        Set<String> literals = new HashSet<>();
        Set<String> patterns = new HashSet<>();
        normalizeQueryTerms(config.getQueryString(), config.getDataTypes(), literals, patterns);
        config.setNormalizedTerms(new ArrayList<>(literals));
        config.setNormalizedPatterns(new ArrayList<>(patterns));
        
        if (log.isDebugEnabled()) {
            log.debug("Normalized Literals = " + literals);
            log.debug("Normalized Patterns = " + patterns);
        }
        
        Set<Range> ranges = new HashSet<>();
        Set<Range> reverseRanges = new HashSet<>();
        
        for (String normalizedQueryTerm : literals) {
            ranges.add(getLiteralRange(normalizedQueryTerm));
            
        }
        for (String normalizedQueryTerm : patterns) {
            RangeDescription r = getRegexRange(normalizedQueryTerm, isFullTableScanEnabled());
            if (r.isForReverseIndex) {
                reverseRanges.add(r.range);
            } else {
                ranges.add(r.range);
            }
            
        }
        
        // if we got ranges for both tables or neither, then something has gone awry in our logic.
        if (ranges.isEmpty() == reverseRanges.isEmpty()) {
            log.error("We cannot process a discovery query against both the index and reverse indexes at the same time: " + literals + ", " + patterns);
            throw new IllegalStateException(
                            "We cannot process a discovery query against both the index and reverse indexes at the same time.  Please separate your query into separate discovery queries.");
        }
        
        // Set the ranges
        if (!ranges.isEmpty()) {
            config.setTableName(config.getIndexTableName());
            config.setRanges(ranges);
        } else {
            config.setTableName(config.getReverseIndexTableName());
            config.setRanges(reverseRanges);
        }
        if (log.isDebugEnabled()) {
            log.debug("Ranges for " + config.getTableName() + " : " + config.getRanges());
        }
        
        return config;
    }
    
    @Override
    public void setupQuery(GenericQueryConfiguration genericConfig) throws QueryException, TableNotFoundException, IOException {
        if (!genericConfig.getClass().getName().equals(ShardIndexQueryConfiguration.class.getName())) {
            throw new QueryException("Did not receive a ShardIndexQueryConfiguration instance!!");
        }
        
        ShardIndexQueryConfiguration config = (ShardIndexQueryConfiguration) genericConfig;
        
        // scan the table
        BatchScanner bs = configureBatchScanner(config, config.getTableName(), config.getRanges(), config.getNormalizedTerms(), config.getNormalizedPatterns(),
                        config.getTableName().equals(config.getReverseIndexTableName()), scannerFactory, this.returnIndexedValues);
        
        if (returnIndexedValues) {
            this.iterator = bs.iterator();
        } else {
            SortedKeyValueIterator<Key,Value> bsIterator = new IteratorToSortedKeyValueIterator(bs.iterator());
            GlobalIndexFieldSummaryIterator fieldSummaryIterator = new GlobalIndexFieldSummaryIterator();
            fieldSummaryIterator.init(bsIterator, new HashMap<String,String>(), null);
            fieldSummaryIterator.next();
            this.iterator = new SortedKeyValueIteratorToIterator(fieldSummaryIterator);
        }
        this.scanner = bs;
    }
    
    /**
     * scan a global index (shardIndex or shardReverseIndex) for the specified ranges and create a set of fieldname/TermInformation values. The Key/Values
     * scanned are trimmed based on a set of terms to match, and a set of data types (found in the config)
     *
     * @param config
     * @param tableName
     * @param ranges
     * @param patterns
     * @param reverseIndex
     * @return The scanner used
     * @throws TableNotFoundException
     */
    private static BatchScanner configureBatchScanner(GenericShardQueryConfiguration config, String tableName, Collection<Range> ranges,
                    Collection<String> literals, Collection<String> patterns, boolean reverseIndex, ScannerFactory scannerFactory, boolean returnIndexedValues)
                    throws TableNotFoundException {
        
        // if we have no ranges, then nothing to scan
        if (ranges.isEmpty()) {
            return null;
        }
        
        BatchScanner bs = scannerFactory.newScanner(tableName, config.getAuthorizations(), config.getNumQueryThreads(), config.getQuery());
        bs.setRanges(ranges);
        
        // The begin date from the query may be down to the second, for doing lookups in the index we want to use the day because
        // the times in the index table have been truncated to the day.
        Date begin = DateUtils.truncate(config.getBeginDate(), Calendar.DAY_OF_MONTH);
        
        // need to push the "end" to the next day to get around lexigraphic sorting with the "shard_partitionid"
        // the end range that is calculated should essentially fall at the very beginning of the next shard_partitionid, but
        // will have no "_paritionid" part which would include that extra day.
        Date end = RangeCalculator.getEndDateForIndexLookup(config.getEndDate());
        
        LongRange dateRange = new LongRange(begin.getTime(), end.getTime());
        
        configureGlobalIndexDateRangeFilter(config, bs, dateRange);
        configureGlobalIndexDataTypeFilter(config, bs, config.getDatatypeFilter());
        
        configureGlobalIndexTermMatchingIterator(config, bs, literals, patterns, reverseIndex);
        
        if (returnIndexedValues) {
            /*
             * If you're not getting data and you think you should be, make sure AgeOff isn't set on your system and that the age off iterator isn't set above
             * this one!
             */
            bs.addScanIterator(new IteratorSetting(config.getBaseIteratorPriority() + 50, UniqueColumnFamilyIterator.class));
        } else {
            bs.addScanIterator(new IteratorSetting(config.getBaseIteratorPriority() + 25, "fieldSummary", GlobalIndexFieldSummaryIterator.class));
        }
        
        return bs;
    }
    
    public static BatchScanner configureBatchScannerAndReturnMatches(GenericShardQueryConfiguration config, String tableName, Collection<Range> ranges,
                    Collection<String> literals, Collection<String> patterns, boolean reverseIndex, ScannerFactory scannerFactory)
                    throws TableNotFoundException {
        return configureBatchScanner(config, tableName, ranges, literals, patterns, reverseIndex, scannerFactory, true);
    }
    
    public static void normalizeQueryTerms(String query, Collection<Type<?>> dataTypes, Set<String> literals, Set<String> patterns) throws ParseException {
        // first split the query into its parts
        DatawaveQueryAnalyzer analyzer = new DatawaveQueryAnalyzer();
        DatawaveTreeNode root = analyzer.parseJexlQuery(query);
        Multimap<String,DatawaveTreeNode> fields = analyzer.getFieldNameToNodeMap(root);
        normalizeQueryTerms(fields, dataTypes, literals, patterns);
    }
    
    private static Set<Integer> regexTypes = new HashSet<>(Arrays.asList(new Integer[] {ParserTreeConstants.JJTERNODE, ParserTreeConstants.JJTNRNODE}));
    private static Set<Integer> literalTypes = new HashSet<>(Arrays.asList(new Integer[] {ParserTreeConstants.JJTEQNODE, ParserTreeConstants.JJTGENODE,
            ParserTreeConstants.JJTGTNODE, ParserTreeConstants.JJTLENODE, ParserTreeConstants.JJTLTNODE, ParserTreeConstants.JJTNENODE}));
    
    public static void normalizeQueryTerms(Multimap<String,DatawaveTreeNode> fields, Collection<Type<?>> dataTypes, Set<String> literals, Set<String> patterns) {
        for (DatawaveTreeNode node : fields.values()) {
            String value = node.getFieldValue();
            if (null == dataTypes) {
                if (regexTypes.contains(node.getType())) {
                    patterns.add(value);
                } else if (literalTypes.contains(node.getType())) {
                    literals.add(value);
                }
            } else {
                for (Type<?> n : dataTypes) {
                    try {
                        if (regexTypes.contains(node.getType())) {
                            patterns.add(n.normalizeRegex(value));
                        } else if (literalTypes.contains(node.getType())) {
                            literals.add(n.normalize(value));
                        }
                    } catch (Exception e) {
                        log.trace("Unable to normalize: " + value + " with normalizer " + n.getClass().getSimpleName());
                    }
                }
            }
        }
    }
    
    public static class RangeDescription {
        public Range range;
        public boolean isForReverseIndex = false;
    }
    
    /**
     * Get a range description for a specified query term which is a literal.
     *
     * @param normalizedQueryTerm
     * @return
     */
    public static Range getLiteralRange(String normalizedQueryTerm) {
        Text fieldValue = new Text(normalizedQueryTerm);
        return new Range(fieldValue);
    }
    
    /**
     * Get a range description for a specified query term, which could be a regex.
     *
     * @param normalizedQueryTerm
     * @return
     * @throws JavaRegexParseException
     */
    public static RangeDescription getRegexRange(String normalizedQueryTerm, boolean fullTableScanEnabled) throws JavaRegexParseException {
        log.debug("getRange: " + normalizedQueryTerm);
        
        RangeDescription rangeDesc = new RangeDescription();
        
        JavaRegexAnalyzer regex = new JavaRegexAnalyzer(normalizedQueryTerm);
        
        if (regex.isNgram()) {
            // If we have wildcards on both sides, then do a full table scan if allowed
            
            // if we require a full table scan but it is disabled, then bail
            if (fullTableScanEnabled) {
                log.error("Found wildcards on boths sides of a term and full table scan is disabled: " + normalizedQueryTerm);
                throw new IllegalArgumentException("Cannot query indexes for terms with wildcards on both ends because full table scan is disabled");
            }
            
            rangeDesc.range = new Range();
            
        } else if (regex.isLeadingLiteral()) {
            normalizedQueryTerm = regex.getLeadingLiteral();
            // either middle or trailing wildcard, truncate the field value at the wildcard location
            // for upper bound, tack on the upper bound UTF character
            rangeDesc.range = new Range(new Text(normalizedQueryTerm), new Text(normalizedQueryTerm + Constants.MAX_UNICODE_STRING));
            
        } else { // trailing literal
            // If we have a leading wildcard, reverse the term and use the global reverse index.
            
            StringBuilder buf = new StringBuilder(regex.getTrailingLiteral());
            normalizedQueryTerm = buf.reverse().toString();
            log.debug("Leading wildcard, normalizedFieldValue: " + normalizedQueryTerm);
            
            // set the upper and lower bounds
            rangeDesc.range = new Range(new Text(normalizedQueryTerm), new Text(normalizedQueryTerm + Constants.MAX_UNICODE_STRING));
            rangeDesc.isForReverseIndex = true;
        }
        
        return rangeDesc;
        
    }
    
    public boolean isFullTableScanEnabled() {
        return fullTableScanEnabled;
    }
    
    public void setFullTableScanEnabled(boolean fullTableScanEnabled) {
        this.fullTableScanEnabled = fullTableScanEnabled;
    }
    
    public boolean isAllowLeadingWildcard() {
        return allowLeadingWildcard;
    }
    
    public void setAllowLeadingWildcard(boolean allowLeadingWildcard) {
        this.allowLeadingWildcard = allowLeadingWildcard;
    }
    
    public boolean isReturnIndexedValues() {
        return returnIndexedValues;
    }
    
    public void setReturnIndexedValues(boolean returnIndexedValues) {
        this.returnIndexedValues = returnIndexedValues;
    }
    
    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }
    
    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return new ShardIndexQueryTransformer(this, settings, this.markingFunctions, this.responseObjectFactory, this.queryModel);
    }
    
    public static final void configureGlobalIndexDateRangeFilter(GenericQueryConfiguration config, ScannerBase bs, LongRange dateRange) {
        // Setup the GlobalIndexDateRangeFilter
        log.debug("Configuring GlobalIndexDateRangeFilter with " + dateRange);
        IteratorSetting cfg = new IteratorSetting(config.getBaseIteratorPriority() + 21, "dateFilter", GlobalIndexDateRangeFilter.class);
        cfg.addOption(Constants.START_DATE, Long.toString(dateRange.getMinimumLong()));
        cfg.addOption(Constants.END_DATE, Long.toString(dateRange.getMaximumLong()));
        bs.addScanIterator(cfg);
    }
    
    public static final void configureGlobalIndexDataTypeFilter(GenericQueryConfiguration config, ScannerBase bs, Collection<String> dataTypes) {
        configureGlobalIndexDataTypeFilter(config.getBaseIteratorPriority(), bs, dataTypes);
    }
    
    public static final void configureGlobalIndexDataTypeFilter(int basePriority, ScannerBase bs, Collection<String> dataTypes) {
        if (dataTypes == null || dataTypes.isEmpty()) {
            return;
        }
        log.debug("Configuring GlobalIndexDataTypeFilter with " + dataTypes);
        
        IteratorSetting cfg = new IteratorSetting(basePriority + 22, "dataTypeFilter", GlobalIndexDataTypeFilter.class);
        int i = 1;
        for (String dataType : dataTypes) {
            cfg.addOption(GlobalIndexDataTypeFilter.DATA_TYPE + i, dataType);
            i++;
        }
        bs.addScanIterator(cfg);
    }
    
    public static final void configureGlobalIndexTermMatchingIterator(GenericShardQueryConfiguration config, ScannerBase bs, Collection<String> literals,
                    Collection<String> patterns, boolean reverseIndex) {
        configureGlobalIndexTermMatchingIterator(config.getBaseIteratorPriority(), bs, literals, patterns, reverseIndex);
    }
    
    public static final void configureGlobalIndexTermMatchingIterator(int basePriority, ScannerBase bs, Collection<String> literals,
                    Collection<String> patterns, boolean reverseIndex) {
        if ((literals == null || literals.isEmpty()) && (patterns == null || patterns.isEmpty())) {
            return;
        }
        log.debug("Configuring GlobalIndexTermMatchingIterator with " + literals + " and " + patterns);
        
        IteratorSetting cfg = new IteratorSetting(basePriority + 23, "termMatcher", GlobalIndexTermMatchingIterator.class);
        
        int i = 1;
        if (patterns != null) {
            for (String pattern : patterns) {
                cfg.addOption(GlobalIndexTermMatchingIterator.PATTERN + i, pattern);
                i++;
            }
        }
        i = 1;
        if (literals != null) {
            for (String literal : literals) {
                cfg.addOption(GlobalIndexTermMatchingIterator.LITERAL + i, literal);
                i++;
            }
        }
        cfg.addOption(GlobalIndexTermMatchingIterator.REVERSE_INDEX, Boolean.toString(reverseIndex));
    }
    
    /**
     * Query model
     *
     * @return
     */
    public String getModelName() {
        return modelName;
    }
    
    public void setModelName(String modelName) {
        this.modelName = modelName;
    }
    
    public String getModelTableName() {
        return modelTableName;
    }
    
    public void setModelTableName(String modelTableName) {
        this.modelTableName = modelTableName;
    }
    
    public QueryModel getQueryModel() {
        return this.queryModel;
    }
    
    public void setQueryModel(QueryModel model) {
        this.queryModel = model;
    }
    
    public Collection<Type<?>> getDataTypes() {
        return dataTypes;
    }
    
    public void setDataTypes(Collection<Type<?>> dataTypes) {
        this.dataTypes = dataTypes;
    }
    
    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> params = new TreeSet<>();
        params.add(QueryParameters.PARAMETER_MODEL_NAME);
        params.add(QueryParameters.PARAMETER_MODEL_TABLE_NAME);
        params.add(QueryParameters.DATATYPE_FILTER_SET);
        return params;
    }
    
    @Override
    public Set<String> getRequiredQueryParameters() {
        // TODO Auto-generated method stub
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> getExampleQueries() {
        // TODO Auto-generated method stub
        return Collections.emptySet();
    }
    
}
