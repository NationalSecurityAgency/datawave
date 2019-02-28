package datawave.query.tables.edge;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import datawave.core.iterators.ColumnQualifierRangeIterator;
import datawave.core.iterators.ColumnRangeIterator;
import datawave.data.type.Type;
import datawave.query.Constants;
import datawave.query.QueryParameters;
import datawave.query.config.EdgeQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.iterator.filter.DateTypeFilter;
import datawave.query.iterator.filter.EdgeFilterIterator;
import datawave.query.iterator.filter.LoadDateFilter;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.EdgeTableRangeBuildingVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.QueryModelVisitor;
import datawave.query.model.edge.EdgeQueryModel;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.edge.contexts.VisitationContext;
import datawave.query.transformer.EdgeQueryTransformer;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.util.time.DateHelper;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.configuration.QueryData;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.StringReader;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

public class EdgeQueryLogic extends BaseQueryLogic<Entry<Key,Value>> {
    
    public static final String PRE_FILTER_DISABLE_KEYWORD = "__DISABLE_PREFILTER__";
    private static final int DEFAULT_SKIP_LIMIT = 10;
    private static final Logger log = Logger.getLogger(EdgeQueryLogic.class);
    
    protected boolean protobufEdgeFormat = true;
    protected EdgeQueryConfiguration config;
    
    protected Map<Integer,IteratorSetting> iteratorDiscriptors = new HashMap<>();
    protected int currentIteratorPriority;
    
    protected ScannerFactory scannerFactory;
    
    protected Set<Type<?>> blockedNormalizers = new HashSet<>();
    
    protected List<? extends Type<?>> dataTypes = null;
    protected List<? extends Type<?>> regexDataTypes = null;
    
    protected int queryThreads = 8;
    
    protected int dateFilterSkipLimit = DEFAULT_SKIP_LIMIT;
    
    private Collection<Range> ranges;
    
    protected HashMultimap<String,String> prefilterValues = null;
    
    private long maxQueryTerms = 10000;
    private long maxPrefilterValues = 100000;
    
    private String modelName = null;
    private String modelTableName = null;
    
    private EdgeQueryModel edgeQueryModel = null;
    
    private VisitationContext visitationContext;
    
    protected MetadataHelperFactory metadataHelperFactory = null;
    
    public EdgeQueryLogic() {
        super();
    }
    
    public EdgeQueryLogic(EdgeQueryLogic other) {
        super(other);
        setDataTypes(other.getDataTypes());
        setRegexDataTypes(other.getRegexDataTypes());
        setQueryThreads(other.getQueryThreads());
        setProtobufEdgeFormat(other.isProtobufEdgeFormat());
        setEdgeQueryModel(other.getEdgeQueryModel());
        setModelName(other.getModelName());
        setModelTableName(other.getModelTableName());
        setMetadataHelperFactory(other.getMetadataHelperFactory());
        visitationContext = other.visitationContext;
    }
    
    @Override
    public GenericQueryConfiguration initialize(Connector connection, Query settings, Set<Authorizations> auths) throws Exception {
        
        currentIteratorPriority = super.getBaseIteratorPriority() + 30;
        MetadataHelper metadataHelper = prepareMetadataHelper(connection, modelTableName, auths);
        
        EdgeQueryConfiguration cfg = setUpConfig(settings);
        
        cfg.setConnector(connection);
        cfg.setAuthorizations(auths);
        
        String queryString = settings.getQuery();
        if (null == queryString) {
            throw new IllegalArgumentException("Query cannot be null");
        } else {
            cfg.setQueryString(queryString);
        }
        cfg.setBeginDate(settings.getBeginDate());
        cfg.setEndDate(settings.getEndDate());
        
        scannerFactory = new ScannerFactory(connection);
        
        return cfg;
    }
    
    protected EdgeQueryConfiguration setUpConfig(Query settings) {
        return new EdgeQueryConfiguration(this, settings).parseParameters(settings);
    }
    
    /**
     * Loads the query model specified by the current configuration, to be applied to the incoming query.
     * 
     * @param helper
     * @param config
     */
    protected void loadQueryModel(MetadataHelper helper, EdgeQueryConfiguration config) {
        String model = config.getModelName() == null ? "" : config.getModelName();
        String modelTable = config.getModelTableName() == null ? "" : config.getModelTableName();
        if (null == getEdgeQueryModel() && (!model.isEmpty() && !modelTable.isEmpty())) {
            try {
                setEdgeQueryModel(new EdgeQueryModel(helper.getQueryModel(config.getModelTableName(), config.getModelName())));
            } catch (Throwable t) {
                log.error("Unable to load edgeQueryModel from metadata table", t);
            }
        }
    }
    
    /**
     * Get an instance of MetadataHelper for the given params
     * 
     * @param connection
     * @param metadataTableName
     * @param auths
     * @return MetadataHelper
     */
    protected MetadataHelper prepareMetadataHelper(Connector connection, String metadataTableName, Set<Authorizations> auths) {
        if (log.isTraceEnabled())
            log.trace("prepareMetadataHelper with " + connection);
        MetadataHelper metadataHelper = this.metadataHelperFactory.createMetadataHelper();
        // check to see if i need to initialize a new one
        if (metadataHelper.getMetadataTableName() != null && metadataTableName != null && !metadataTableName.equals(metadataHelper.getMetadataTableName())) {
            // initialize it
            metadataHelper.initialize(connection, metadataTableName, auths);
        } else if (metadataHelper.getAuths() == null || metadataHelper.getAuths().isEmpty()) {
            return metadataHelper.initialize(connection, metadataTableName, auths);
            // assumption is that it is already initialized. we shall see.....
        } else {
            if (log.isTraceEnabled())
                log.trace("the MetadataHelper did not need to be initialized:" + metadataHelper + " and " + metadataTableName + " and " + auths);
        }
        return metadataHelper.initialize(connection, metadataTableName, auths);
    }
    
    /**
     * Parses the Jexl Query string into an ASTJexlScript and then uses QueryModelVisitor to apply queryModel to the query string, and then rewrites the
     * translated ASTJexlScript back to a query string using JexlStringBuildingVisitor.
     * 
     * @param queryString
     * @return
     */
    protected String applyQueryModel(String queryString) {
        ASTJexlScript origScript = null;
        ASTJexlScript script = null;
        try {
            origScript = JexlASTHelper.parseJexlQuery(queryString);
            HashSet<String> allFields = new HashSet<>();
            allFields.addAll(getEdgeQueryModel().getAllInternalFieldNames());
            script = QueryModelVisitor.applyModel(origScript, getEdgeQueryModel(), allFields);
            return JexlStringBuildingVisitor.buildQuery(script);
            
        } catch (Throwable t) {
            throw new IllegalStateException("Edge query model could not be applied", t);
        }
    }
    
    /**
     * Are we querying the protobuf edge format
     * 
     * @return true if querying the protobuf edge format
     */
    public boolean isProtobufEdgeFormat() {
        return protobufEdgeFormat;
    }
    
    /**
     * Set whether we are querying the protobuf edge format. Default is true.
     * 
     * @param protobufedge
     */
    public void setProtobufEdgeFormat(boolean protobufedge) {
        this.protobufEdgeFormat = protobufedge;
    }
    
    /**
     * Parses JEXL in query string to create ranges and column family filters
     *
     * @param queryString
     *            jexl string for the query
     */
    protected QueryData configureRanges(String queryString) throws ParseException {
        queryString = EdgeQueryLogic.fixQueryString(queryString);
        QueryData qData = new QueryData();
        Parser parser = new Parser(new StringReader(";"));
        ASTJexlScript script;
        try {
            script = parser.parse(new StringReader(queryString), null);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid jexl supplied. " + e.getMessage());
        }
        
        EdgeTableRangeBuildingVisitor visitor = new EdgeTableRangeBuildingVisitor(config.includeStats(), dataTypes, config.getMaxQueryTerms(), regexDataTypes);
        
        visitationContext = (VisitationContext) script.jjtAccept(visitor, null);
        
        Set<Range> ranges = visitationContext.getRanges();
        qData.setRanges(ranges);
        return qData;
    }
    
    /**
     * Method to expand the values supplied for SOURCE or SINK into all of the permutations after normalizing
     *
     * @param query
     * @return
     */
    protected VisitationContext normalizeJexlQuery(String query, boolean getFullNormalizedQuery) {
        if (visitationContext == null) {
            throw new DatawaveFatalQueryException("Something went wrong running your query");
        }
        
        // These strings are going to be parsed again in the iterator but if there is a problem with
        // normalizing the query we want to fail here instead of over on the server side
        if (!visitationContext.getNormalizedQuery().equals("")) {
            try {
                
                JexlASTHelper.parseJexlQuery(visitationContext.getNormalizedQuery().toString());
                
            } catch (ParseException e) {
                log.error("Could not parse JEXL AST after performing transformations to run the query. Normalized Stats Query: "
                                + visitationContext.getNormalizedStatsQuery(), e);
                
                throw new DatawaveFatalQueryException("Something went wrong running your query", e);
            }
        }
        
        if (!visitationContext.getNormalizedStatsQuery().equals("")) {
            try {
                JexlASTHelper.parseJexlQuery(visitationContext.getNormalizedStatsQuery().toString());
            } catch (ParseException e) {
                log.error("Could not parse JEXL AST after performing transformations to run the query. Normalized Stats Query: "
                                + visitationContext.getNormalizedStatsQuery(), e);
                
                throw new DatawaveFatalQueryException("Something went wrong running your query", e);
            }
        }
        
        pruneAndSetPreFilterValues(visitationContext.getPreFilterValues());
        long termCount = visitationContext.getTermCount();
        if (termCount > config.getMaxQueryTerms()) {
            throw new IllegalArgumentException("Edge query max terms limit (" + config.getMaxQueryTerms() + ") exceeded: " + termCount + ".");
        }
        
        return visitationContext;
        
    }
    
    void pruneAndSetPreFilterValues(HashMultimap<String,String> prefilters) {
        HashMultimap<String,String> newMap = HashMultimap.create();
        long count = 0;
        for (String field : prefilters.keySet()) {
            Set<String> values = prefilters.get(field);
            if (values == null) {
                continue;
            }
            if (values.contains(PRE_FILTER_DISABLE_KEYWORD)) {
                continue;
            }
            if (values.size() < 1) {
                continue;
            }
            newMap.putAll(field, values);
            count++;
        }
        if (count <= config.getMaxPrefilterValues()) {
            if (count > 0) {
                prefilterValues = newMap;
            }
        } else {
            log.warn("Prefilter count exceeded threshold, ignoring...");
        }
    }
    
    protected void addIterator(QueryData qData, IteratorSetting iter) {
        iteratorDiscriptors.put(currentIteratorPriority, iter);
        qData.addIterator(iter);
        currentIteratorPriority++;
    }
    
    protected void addIterators(QueryData qData, List<IteratorSetting> iters) {
        for (IteratorSetting iter : iters) {
            log.debug("Adding iterator: " + iter);
            addIterator(qData, iter);
        }
    }
    
    public static String fixQueryString(String original) {
        String newQuery = original;
        // first fix uppercase operators
        newQuery = original.replaceAll("\\s+[Aa][Nn][Dd]\\s+", " and ");
        newQuery = newQuery.replaceAll("\\s+[Oo][Rr]\\s+", " or ");
        newQuery = newQuery.replaceAll("\\s+[Nn][Oo][Tt]\\s+", " not ");
        
        return newQuery;
    }
    
    /**
     * Create iterator to filter on event/activity date in the column qualifier or the load date in the value.
     * 
     * @param beginDate
     *            lower bound for date range filter
     * @param endDate
     *            upper bound for date range filter
     * @param priority
     *            priority to associate with this iterator
     * @param dateFilterType
     *            type of filtering (EVENT, LOAD, ACTIVITY, ACTIVITY_LOAD, ANY, ANY_LOAD)
     * @return created iterator (or null if no iterator needed, i.e. dates not specified)
     */
    public static IteratorSetting getDateFilter(Date beginDate, Date endDate, int priority, EdgeQueryConfiguration.dateType dateFilterType) {
        return getDateFilter(beginDate, endDate, priority, DEFAULT_SKIP_LIMIT, dateFilterType);
    }
    
    /**
     * Create iterator to filter on event/activity date in the column qualifier or the load date in the value.
     *
     * @param beginDate
     *            lower bound for date range filter
     * @param endDate
     *            upper bound for date range filter
     * @param priority
     *            priority to associate with this iterator
     * @param skipLimit
     *            amount of keys for the iterator to skip before calling seek
     * @param dateFilterType
     *            type of filtering (EVENT, LOAD, ACTIVITY, ACTIVITY_LOAD, ANY, ANY_LOAD)
     * @return created iterator (or null if no iterator needed, i.e. dates not specified)
     */
    public static IteratorSetting getDateFilter(Date beginDate, Date endDate, int priority, int skipLimit, EdgeQueryConfiguration.dateType dateFilterType) {
        IteratorSetting setting = null;
        if (null != beginDate && null != endDate) {
            log.debug("Creating daterange filter: " + beginDate + " " + endDate);
            Key beginDateKey = new Key(DateHelper.format(beginDate));
            Key endDateKey = new Key(DateHelper.format(endDate) + Constants.MAX_UNICODE_STRING);
            if ((dateFilterType == EdgeQueryConfiguration.dateType.EVENT) || (dateFilterType == EdgeQueryConfiguration.dateType.ACTIVITY)
                            || (dateFilterType == EdgeQueryConfiguration.dateType.ANY)) {
                setting = new IteratorSetting(priority, ColumnQualifierRangeIterator.class.getSimpleName() + "_" + priority, ColumnQualifierRangeIterator.class);
            } else if ((dateFilterType == EdgeQueryConfiguration.dateType.LOAD) || (dateFilterType == EdgeQueryConfiguration.dateType.ACTIVITY_LOAD)
                            || (dateFilterType == EdgeQueryConfiguration.dateType.ANY_LOAD)) {
                setting = new IteratorSetting(priority, LoadDateFilter.class.getSimpleName() + "_" + priority, LoadDateFilter.class);
                // we also want to set the date range type parameter when using the LoadDateFilter
                setting.addOption(EdgeQueryConfiguration.DATE_RANGE_TYPE, dateFilterType.name());
            } else {
                throw new IllegalStateException("Unexpected dateType");
            }
            
            Range range = new Range(beginDateKey, endDateKey);
            try {
                setting.addOption(ColumnRangeIterator.RANGE_NAME, ColumnRangeIterator.encodeRange(range));
                setting.addOption(ColumnRangeIterator.SKIP_LIMIT_NAME, Integer.toString(skipLimit));
            } catch (IOException ex) {
                throw new IllegalStateException("Exception caught attempting to configure encoded range iterator for date filtering.", ex);
            }
        }
        
        return setting;
    }
    
    /**
     * Create iterator to filter on date type.
     * 
     * @param priority
     *            priority to associate with this iterator
     * @param dateFilterType
     *            type of filtering (EVENT, LOAD, ACTIVITY, ACTIVITY_LOAD, ANY, ANY_LOAD)
     * @return created iterator
     */
    public static IteratorSetting getDateTypeFilter(int priority, EdgeQueryConfiguration.dateType dateFilterType) {
        IteratorSetting setting = null;
        log.debug("Creating dateType filter=" + dateFilterType);
        setting = new IteratorSetting(priority, DateTypeFilter.class.getSimpleName() + "_" + priority, DateTypeFilter.class);
        setting.addOption(EdgeQueryConfiguration.DATE_RANGE_TYPE, dateFilterType.name());
        
        return setting;
    }
    
    public static List<IteratorSetting> getDateBasedIterators(Date beginDate, Date endDate, int priority, int skipLimit,
                    EdgeQueryConfiguration.dateType dateFilterType) {
        List<IteratorSetting> settings = Lists.newArrayList();
        
        // the following iterator will filter out edges outside of our date range
        // @note only returns an iterator if both beginDate and endDate are non-null
        // @note if a load date iterator is returned then it filters both on date range and date type (whereas the date range iterator only filters on date)
        IteratorSetting iter = getDateFilter(beginDate, endDate, priority, skipLimit, dateFilterType);
        if (iter != null) {
            settings.add(iter);
            priority++;
        }
        
        // if we have a load date iterator (from above call) then no further iterator needed as it filters out by date type and by date range
        // but if we have no iterator or only date range iterator then we still may need a date type filter
        // @note we won't get an iterator in the above call if either/both dates are null regardless of dateFilterType
        if ((iter == null)
                        || ((dateFilterType != EdgeQueryConfiguration.dateType.LOAD) && (dateFilterType != EdgeQueryConfiguration.dateType.ACTIVITY_LOAD) && (dateFilterType != EdgeQueryConfiguration.dateType.ANY_LOAD))) {
            if ((dateFilterType != EdgeQueryConfiguration.dateType.ANY) && (dateFilterType != EdgeQueryConfiguration.dateType.ANY_LOAD)) {
                // of the edges remaining we only want the correct type (activity date or event date)
                iter = getDateTypeFilter(priority, dateFilterType);
                if (iter != null) {
                    settings.add(iter);
                    priority++;
                }
            }
        }
        
        return settings;
    }
    
    /**
     * Create set of iterators to filter and combine appropriate edges based on specified date type.
     * 
     * @param beginDate
     *            lower bound for date range filter
     * @param endDate
     *            upper bound for date range filter
     * @param priority
     *            priority to associate with the first created iterator; subsequent iterators will have increasing 1-up values
     * @param dateFilterType
     *            type of filtering (EVENT, LOAD, ACTIVITY, ACTIVITY_LOAD, ANY, ANY_LOAD)
     * @return created iterators
     *
     *         There can now be one or more edges with matching keys other than a date type distinction in the column qualifier. Old-style edges have no date
     *         type marking but always used event date. New-style edges may indicate an event date-based edge, both, or activity date-based edge. Hence, based
     *         on whether the query is for event date-based edges or activity date-based edges the appropriate types of edges must be returned. Additionally,
     *         the results should combine like-type edges that only differ in the date type. Consequently, this function creates one or more iterators to
     *         perform the appropriate filtering/combining.
     */
    public static List<IteratorSetting> getDateBasedIterators(Date beginDate, Date endDate, int priority, EdgeQueryConfiguration.dateType dateFilterType) {
        List<IteratorSetting> settings = Lists.newArrayList();
        
        // the following iterator will filter out edges outside of our date range
        // @note only returns an iterator if both beginDate and endDate are non-null
        // @note if a load date iterator is returned then it filters both on date range and date type (whereas the date range iterator only filters on date)
        IteratorSetting iter = getDateFilter(beginDate, endDate, priority, dateFilterType);
        if (iter != null) {
            settings.add(iter);
            priority++;
        }
        
        // if we have a load date iterator (from above call) then no further iterator needed as it filters out by date type and by date range
        // but if we have no iterator or only date range iterator then we still may need a date type filter
        // @note we won't get an iterator in the above call if either/both dates are null regardless of dateFilterType
        if ((iter == null)
                        || ((dateFilterType != EdgeQueryConfiguration.dateType.LOAD) && (dateFilterType != EdgeQueryConfiguration.dateType.ACTIVITY_LOAD) && (dateFilterType != EdgeQueryConfiguration.dateType.ANY_LOAD))) {
            if ((dateFilterType != EdgeQueryConfiguration.dateType.ANY) && (dateFilterType != EdgeQueryConfiguration.dateType.ANY_LOAD)) {
                // of the edges remaining we only want the correct type (activity date or event date)
                iter = getDateTypeFilter(priority, dateFilterType);
                if (iter != null) {
                    settings.add(iter);
                    priority++;
                }
            }
        }
        
        return settings;
    }
    
    protected String serializePrefilter() {
        
        String retVal = null;
        if (prefilterValues != null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(prefilterValues);
                oos.close();
            } catch (IOException ex) {
                log.error("Some error encoding the prefilters...", ex);
            }
            retVal = new String(Base64.encodeBase64(baos.toByteArray()));
        }
        
        return retVal;
    }
    
    @Override
    public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
        config = (EdgeQueryConfiguration) configuration;
        prefilterValues = null;
        EdgeQueryConfiguration.dateType dateFilterType = ((EdgeQueryConfiguration) configuration).getDateRangeType();
        
        log.debug("Performing edge table query: " + config.getQueryString());
        
        boolean includeStats = ((EdgeQueryConfiguration) configuration).includeStats();
        
        String queryString = config.getQueryString();
        String normalizedQuery = null;
        String statsNormalizedQuery = null;
        
        queryString = fixQueryString(queryString);
        QueryData qData = configureRanges(queryString);
        setRanges(qData.getRanges());
        
        VisitationContext context = null;
        try {
            context = normalizeJexlQuery(queryString, false);
            normalizedQuery = context.getNormalizedQuery().toString();
            statsNormalizedQuery = context.getNormalizedStatsQuery().toString();
            log.debug("Jexl after normalizing SOURCE and SINK: " + normalizedQuery);
        } catch (JexlException ex) {
            log.error("Error parsing user query.", ex);
        }
        
        if ((null == normalizedQuery || normalizedQuery.equals("")) && qData.getRanges().size() < 1) {
            throw new IllegalStateException("Query string is empty after initial processing, no ranges or filters can be generated to execute.");
        }
        
        addIterators(qData, getDateBasedIterators(config.getBeginDate(), config.getEndDate(), currentIteratorPriority, dateFilterSkipLimit, dateFilterType));
        
        if (!normalizedQuery.equals("")) {
            log.debug("Query being sent to the filter iterator: " + normalizedQuery);
            IteratorSetting edgeIteratorSetting = new IteratorSetting(currentIteratorPriority, EdgeFilterIterator.class.getSimpleName() + "_"
                            + currentIteratorPriority, EdgeFilterIterator.class);
            edgeIteratorSetting.addOption(EdgeFilterIterator.JEXL_OPTION, normalizedQuery);
            edgeIteratorSetting.addOption(EdgeFilterIterator.PROTOBUF_OPTION, "TRUE");
            
            if (!statsNormalizedQuery.equals("")) {
                edgeIteratorSetting.addOption(EdgeFilterIterator.JEXL_STATS_OPTION, statsNormalizedQuery);
            }
            if (prefilterValues != null) {
                String value = serializePrefilter();
                edgeIteratorSetting.addOption(EdgeFilterIterator.PREFILTER_WHITELIST, value);
            }
            
            if (includeStats) {
                edgeIteratorSetting.addOption(EdgeFilterIterator.INCLUDE_STATS_OPTION, "TRUE");
            } else {
                edgeIteratorSetting.addOption(EdgeFilterIterator.INCLUDE_STATS_OPTION, "FALSE");
            }
            
            addIterator(qData, edgeIteratorSetting);
        }
        
        log.debug("Configuring connection: tableName: " + config.getTableName() + ", auths: " + config.getAuthorizations());
        
        BatchScanner scanner = createBatchScanner(config);
        
        log.debug("Using the following ranges: " + qData.getRanges());
        
        if (context != null && context.isHasAllCompleteColumnFamilies()) {
            for (Text columnFamily : context.getColumnFamilies()) {
                scanner.fetchColumnFamily(columnFamily);
            }
            
        }
        
        scanner.setRanges(qData.getRanges());
        
        addCustomFilters(qData, currentIteratorPriority);
        
        for (IteratorSetting setting : qData.getSettings()) {
            scanner.addScanIterator(setting);
        }
        
        this.scanner = scanner;
        iterator = scanner.iterator();
    }
    
    protected BatchScanner createBatchScanner(GenericQueryConfiguration config) {
        EdgeQueryConfiguration conf = (EdgeQueryConfiguration) config;
        try {
            return scannerFactory.newScanner(config.getTableName(), config.getAuthorizations(), conf.getNumQueryThreads(), conf.getQuery());
        } catch (TableNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }
    
    @Override
    public void close() {
        super.close();
        
        if (null != scannerFactory) {
            scannerFactory.lockdown();
            for (ScannerBase scanner : scannerFactory.currentScanners()) {
                scanner.close();
            }
        }
    }
    
    /**
     * Configures a column filters for the logic scanner to apply custom logic
     *
     * @param data
     *            the QueryData for the query logic to be configured
     * @param priority
     *            the priority for the first of iterator filters
     */
    protected void addCustomFilters(QueryData data, int priority) {}
    
    @Override
    public Priority getConnectionPriority() {
        return Priority.NORMAL;
    }
    
    @Override
    public EdgeQueryLogic clone() {
        return new EdgeQueryLogic(this);
    }
    
    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return new EdgeQueryTransformer(settings, this.markingFunctions, this.responseObjectFactory);
    }
    
    public List<? extends Type<?>> getDataTypes() {
        return dataTypes;
    }
    
    public void setDataTypes(List<? extends Type<?>> dataTypes) {
        this.dataTypes = dataTypes;
    }
    
    public List<? extends Type<?>> getRegexDataTypes() {
        return regexDataTypes;
    }
    
    public void setRegexDataTypes(List<? extends Type<?>> regexDataTypes) {
        this.regexDataTypes = regexDataTypes;
    }
    
    public int getQueryThreads() {
        return queryThreads;
    }
    
    public void setQueryThreads(int queryThreads) {
        this.queryThreads = queryThreads;
    }
    
    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> params = new TreeSet<>();
        params.add(datawave.webservice.query.QueryParameters.QUERY_BEGIN);
        params.add(datawave.webservice.query.QueryParameters.QUERY_END);
        params.add(QueryParameters.DATATYPE_FILTER_SET);
        params.add(EdgeQueryConfiguration.INCLUDE_STATS);
        params.add(EdgeQueryConfiguration.DATE_RANGE_TYPE);
        return params;
    }
    
    public Set<Type<?>> getBlockedNormalizers() {
        return blockedNormalizers;
    }
    
    public void setBlockedNormalizers(Set<Type<?>> blockedNormalizers) {
        this.blockedNormalizers = blockedNormalizers;
    }
    
    public Collection<Range> getRanges() {
        return ranges;
    }
    
    public void setRanges(Collection<Range> ranges) {
        this.ranges = ranges;
    }
    
    public long getMaxQueryTerms() {
        return maxQueryTerms;
    }
    
    public void setMaxQueryTerms(long maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
    }
    
    public long getMaxPrefilterValues() {
        return maxPrefilterValues;
    }
    
    public void setMaxPrefilterValues(long maxPrefilterValues) {
        this.maxPrefilterValues = maxPrefilterValues;
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
    
    public EdgeQueryModel getEdgeQueryModel() {
        return this.edgeQueryModel;
    }
    
    public void setEdgeQueryModel(EdgeQueryModel model) {
        this.edgeQueryModel = model;
    }
    
    public String getModelName() {
        return this.modelName;
    }
    
    public void setModelName(String modelName) {
        this.modelName = modelName;
    }
    
    public String getModelTableName() {
        return this.modelTableName;
    }
    
    public void setModelTableName(String modelTableName) {
        this.modelTableName = modelTableName;
    }
    
    public MetadataHelperFactory getMetadataHelperFactory() {
        return metadataHelperFactory;
    }
    
    public void setMetadataHelperFactory(MetadataHelperFactory metadataHelperFactory) {
        if (log.isTraceEnabled())
            log.trace("setting MetadataHelperFactory on " + this + " - " + this.getClass() + " to " + metadataHelperFactory + " - "
                            + metadataHelperFactory.getClass());
        this.metadataHelperFactory = metadataHelperFactory;
    }
    
    public int getDateFilterSkipLimit() {
        return dateFilterSkipLimit;
    }
    
    public void setDateFilterSkipLimit(int dateFilterSkipLimit) {
        this.dateFilterSkipLimit = dateFilterSkipLimit;
    }
}
