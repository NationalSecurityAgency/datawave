package datawave.query.tables;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.data.type.Type;
import datawave.marking.MarkingFunctions;
import datawave.query.CloseableIterable;
import datawave.query.Constants;
import datawave.query.DocumentSerialization;
import datawave.query.QueryParameters;
import datawave.query.cardinality.CardinalityConfiguration;
import datawave.query.config.IndexHole;
import datawave.query.config.Profile;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.enrich.DataEnricher;
import datawave.query.enrich.EnrichingMaster;
import datawave.query.index.lookup.CreateUidsIterator;
import datawave.query.index.lookup.IndexInfo;
import datawave.query.index.lookup.UidIntersector;
import datawave.query.iterator.QueryOptions;
import datawave.query.language.parser.ParseException;
import datawave.query.language.parser.QueryParser;
import datawave.query.language.tree.QueryNode;
import datawave.query.model.QueryModel;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.planner.MetadataHelperQueryModelProvider;
import datawave.query.planner.QueryModelProvider;
import datawave.query.planner.QueryPlanner;
import datawave.query.scheduler.PushdownScheduler;
import datawave.query.scheduler.Scheduler;
import datawave.query.scheduler.SequentialScheduler;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.transformer.EventQueryDataDecoratorTransformer;
import datawave.query.transformer.GroupingTransform;
import datawave.query.transformer.UniqueTransform;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.query.util.QueryStopwatch;
import datawave.util.StringUtils;
import datawave.util.time.TraceStopwatch;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.configuration.QueryData;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.query.logic.WritesQueryMetrics;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <h1>Overview</h1> QueryTable implementation that works with the JEXL grammar. This QueryTable uses the DATAWAVE metadata, global index, and sharded event
 * table to return results based on the query. The runServerQuery method is the main method that is called from the web service, and it contains the logic used
 * to run the queries against ACCUMULO. Example queries:
 *
 * <pre>
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
 * </pre>
 *
 * We are not supporting all of the operators that JEXL supports at this time. We are supporting the following operators:
 * 
 * <pre>
 *  ==, !=, &gt;, &ge;, &lt;, &le;, =~, !~, and the reserved word 'null'
 * </pre>
 *
 * Custom functions can be created and registered with the Jexl engine. The functions can be used in the queries in conjunction with other supported operators.
 * A sample function has been created, called between, and is bound to the 'f' namespace. An example using this function is : "f:between(LATITUDE,60.0, 70.0)"
 * 
 * <h1>Constraints on Query Structure</h1> Queries that are sent to this class need to be formatted such that there is a space on either side of the operator.
 * We are rewriting the query in some cases and the current implementation is expecting a space on either side of the operator.
 * 
 * <h1>Notes on Optimization</h1> Queries that meet any of the following criteria will perform a full scan of the events in the sharded event table:
 *
 * <pre>
 *  1. An 'or' conjunction exists in the query but not all of the terms are indexed.
 *  2. No indexed terms exist in the query
 *  3. An unsupported operator exists in the query
 * </pre>
 *
 * <h1>Notes on Features</h1>
 *
 * <pre>
 *  1. If there is no type specified for a field in the metadata table, then it defaults to using the NoOpType. The default
 *     can be overriden by calling setDefaultType()
 *  2. We support fields that are indexed, but not in the event. An example of this is for text documents, where the text is tokenized
 *     and indexed, but the tokens are not stored with the event.
 *  3. We support fields that have term frequency records in the shard table containing lists of offsets.  This would be for tokens
 *     parsed out of content and can subsequently be used in various content functions such as 'within' or 'adjacent' or 'phrase'.
 *  4. We support the ability to define a list of {@link DataEnricher}s to add additional information to returned events. Found events are
 *     passed through the {@link EnrichingMaster} which passes the event through each configured data enricher class. Only the value
 *     can be modified. The key *cannot* be modified through this interface (as it could break the sorted order). Enriching must be enabled
 *     by setting {@link ShardQueryConfiguration#useEnrichers} to true and providing a list of {@link datawave.query.enrich.DataEnricher} class names in
 *     {@link ShardQueryConfiguration#enricherClassNames}.
 *  5. A list of {@link datawave.query.index.lookup.DataTypeFilter}s can be specified to remove found Events before they are returned to the user.
 *     These data filters can return a true/false value on whether the Event should be returned to the user or discarded. Additionally,
 *     the filter can return a {@code Map<String, Object>} that can be passed into a JexlContext (provides the necessary information for Jexl to
 *     evaluate an Event based on information not already present in the Event or information that doesn't need to be returned with the Event.
 *     Filtering must be enabled by setting {@link ShardQueryConfiguration#useFilters} to true and providing a list of {@link datawave.query.index.lookup.DataTypeFilter} class
 *     names in {@link ShardQueryConfiguration#filterClassNames}.
 *  6. Projection can be accomplished by setting the {@link QueryParameters RETURN_FIELDS} parameter to a '/'-separated list of field names.
 * 
 * </pre>
 * 
 * @see datawave.query.enrich
 */
public class ShardQueryLogic extends BaseQueryLogic<Entry<Key,Value>> {
    
    public static final String NULL_BYTE = "\0";
    public static final Class<? extends ShardQueryConfiguration> tableConfigurationType = ShardQueryConfiguration.class;
    protected static final Logger log = ThreadConfigurableLogger.getLogger(ShardQueryLogic.class);
    static final ListeningExecutorService reloader = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    private static Cache<String,QueryModel> queryModelMap = CacheBuilder.newBuilder().maximumSize(100).concurrencyLevel(100)
                    .expireAfterAccess(24, TimeUnit.HOURS).build();
    protected Class<? extends SortedKeyValueIterator<Key,Value>> createUidsIteratorClass = CreateUidsIterator.class;
    protected UidIntersector uidIntersector = new IndexInfo();
    protected CloseableIterable<QueryData> queries = null;
    protected QueryModel queryModel = null;
    protected ScannerFactory scannerFactory = null;
    protected Scheduler scheduler = null;
    protected EventQueryDataDecoratorTransformer eventQueryDataDecoratorTransformer = null;
    private ShardQueryConfiguration config;
    protected MetadataHelperFactory metadataHelperFactory = null;
    protected DateIndexHelperFactory dateIndexHelperFactory = null;
    protected Function<String,String> queryMacroFunction;
    protected Map<String,Profile> configuredProfiles = Maps.newHashMap();
    protected Profile selectedProfile = null;
    protected Map<String,List<String>> primaryToSecondaryFieldMap = Collections.emptyMap();
    // Map of syntax names to QueryParser classes
    private Map<String,QueryParser> querySyntaxParsers = new HashMap<>();
    private Set<String> mandatoryQuerySyntax = null;
    private QueryPlanner planner = null;
    private QueryParser parser = null;
    
    private CardinalityConfiguration cardinalityConfiguration = null;
    
    /**
     * Basic constructor
     */
    public ShardQueryLogic() {
        super();
        if (log.isTraceEnabled())
            log.trace("Creating ShardQueryLogic: " + System.identityHashCode(this));
    }
    
    /**
     * Copy constructor
     *
     * @param other
     *            - another ShardQueryLogic object
     */
    public ShardQueryLogic(ShardQueryLogic other) {
        super(other);
        
        if (log.isTraceEnabled())
            log.trace("Creating Cloned ShardQueryLogic: " + System.identityHashCode(this) + " from " + System.identityHashCode(other));
        
        // Set ShardQueryConfiguration variables
        this.config = ShardQueryConfiguration.create(other);
        this.setQuerySyntaxParsers(other.getQuerySyntaxParsers());
        this.setMandatoryQuerySyntax(other.getMandatoryQuerySyntax());
        this.setQueryPlanner(other.getQueryPlanner().clone());
        this.setCreateUidsIteratorClass(other.getCreateUidsIteratorClass());
        this.setUidIntersector(other.getUidIntersector());
        
        this.setQueries(other.getQueries());
        this.setParser(other.getParser());
        this.setQueryModel(other.getQueryModel());
        this.setScannerFactory(other.getScannerFactory());
        this.setScheduler(other.getScheduler());
        this.setEventQueryDataDecoratorTransformer(other.getEventQueryDataDecoratorTransformer());
        
        log.trace("copy CTOR setting metadataHelperFactory to " + other.getMetadataHelperFactory());
        this.setMetadataHelperFactory(other.getMetadataHelperFactory());
        this.setDateIndexHelperFactory(other.getDateIndexHelperFactory());
        this.setQueryMacroFunction(other.getQueryMacroFunction());
        this.setCardinalityConfiguration(other.getCardinalityConfiguration());
        this.setConfiguredProfiles(other.getConfiguredProfiles());
        this.setSelectedProfile(other.getSelectedProfile());
        this.setPrimaryToSecondaryFieldMap(other.getPrimaryToSecondaryFieldMap());
        
        if (other.eventQueryDataDecoratorTransformer != null) {
            this.eventQueryDataDecoratorTransformer = new EventQueryDataDecoratorTransformer(other.eventQueryDataDecoratorTransformer);
        }
    }
    
    public static BatchScanner createBatchScanner(ShardQueryConfiguration config, ScannerFactory scannerFactory, QueryData qd) throws TableNotFoundException {
        final BatchScanner bs = scannerFactory.newScanner(config.getShardTableName(), config.getAuthorizations(), config.getNumQueryThreads(),
                        config.getQuery());
        
        if (log.isTraceEnabled()) {
            log.trace("Running with " + config.getAuthorizations() + " and " + config.getNumQueryThreads() + " threads: " + qd);
        }
        
        bs.setRanges(qd.getRanges());
        
        for (IteratorSetting cfg : qd.getSettings()) {
            bs.addScanIterator(cfg);
        }
        
        return bs;
    }
    
    @Override
    public GenericQueryConfiguration initialize(Connector connection, Query settings, Set<Authorizations> auths) throws Exception {
        
        this.config = ShardQueryConfiguration.create(this, settings);
        if (log.isTraceEnabled())
            log.trace("Initializing ShardQueryLogic: " + System.identityHashCode(this) + '('
                            + (this.getSettings() == null ? "empty" : this.getSettings().getId()) + ')');
        this.config.setExpandFields(true);
        this.config.setExpandValues(true);
        initialize(config, connection, settings, auths);
        return config;
    }
    
    @Override
    public String getPlan(Connector connection, Query settings, Set<Authorizations> auths, boolean expandFields, boolean expandValues) throws Exception {
        
        this.config = ShardQueryConfiguration.create(this, settings);
        if (log.isTraceEnabled())
            log.trace("Initializing ShardQueryLogic for plan: " + System.identityHashCode(this) + '('
                            + (this.getSettings() == null ? "empty" : this.getSettings().getId()) + ')');
        this.config.setExpandFields(expandFields);
        this.config.setExpandValues(expandValues);
        initialize(config, connection, settings, auths);
        return config.getQueryString();
    }
    
    protected String expandQueryMacros(String query) throws ParseException {
        log.trace("query macros are :" + this.queryMacroFunction);
        if (this.queryMacroFunction != null) {
            query = this.queryMacroFunction.apply(query);
        }
        return query;
    }
    
    public String getJexlQueryString(Query settings) throws ParseException {
        // queryString should be JEXl after all query parsers are applied
        String queryString;
        String originalQuery = settings.getQuery();
        
        originalQuery = this.expandQueryMacros(originalQuery);
        
        // Determine query syntax (i.e. JEXL, LUCENE, etc.)
        String querySyntax = settings.findParameter(QueryParameters.QUERY_SYNTAX).getParameterValue();
        
        // enforce mandatoryQuerySyntax if set
        if (null != this.mandatoryQuerySyntax) {
            if (org.apache.commons.lang.StringUtils.isEmpty(querySyntax)) {
                throw new IllegalStateException("Must specify one of the following syntax options: " + this.mandatoryQuerySyntax);
            } else {
                if (!this.mandatoryQuerySyntax.contains(querySyntax)) {
                    throw new IllegalStateException("Syntax not supported, must be one of the following: " + this.mandatoryQuerySyntax + ", submitted: "
                                    + querySyntax);
                }
            }
        }
        
        QueryParser querySyntaxParser = getParser();
        
        if (org.apache.commons.lang.StringUtils.isBlank(querySyntax)) {
            // Default to the class's query parser when one is not provided
            // Falling back to Jexl when one is not set on this class
            if (null == querySyntaxParser) {
                querySyntax = "JEXL";
            }
        } else if (!"JEXL".equals(querySyntax)) {
            if (null == querySyntaxParsers) {
                throw new IllegalStateException("Query syntax parsers not configured");
            }
            
            querySyntaxParser = querySyntaxParsers.get(querySyntax);
            
            if (null == querySyntaxParser) {
                // No parser was specified, try to default to the parser on the
                // class
                querySyntaxParser = getParser();
                
                if (null == querySyntaxParser) {
                    throw new IllegalArgumentException("QueryParser not configured for syntax: " + querySyntax);
                }
            }
        }
        
        if (null == originalQuery) {
            throw new IllegalArgumentException("Query cannot be null");
        } else {
            if ("JEXL".equals(querySyntax)) {
                queryString = originalQuery;
            } else {
                QueryNode node = querySyntaxParser.parse(originalQuery);
                queryString = node.getOriginalQuery();
                if (log.isTraceEnabled()) {
                    log.trace("luceneQueryString: " + originalQuery + " --> jexlQueryString: " + queryString);
                }
            }
        }
        return queryString;
    }
    
    public void initialize(ShardQueryConfiguration config, Connector connection, Query settings, Set<Authorizations> auths) throws Exception {
        // Set the connector and the authorizations into the config object
        config.setConnector(connection);
        config.setAuthorizations(auths);
        config.setMaxScannerBatchSize(getMaxScannerBatchSize());
        config.setMaxIndexBatchSize(getMaxIndexBatchSize());
        
        setScannerFactory(new ScannerFactory(config));
        
        String jexlQueryString = getJexlQueryString(settings);
        
        if (null == jexlQueryString) {
            throw new IllegalArgumentException("Query cannot be null");
        } else {
            config.setQueryString(jexlQueryString);
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
        
        loadQueryParameters(config, settings);
        
        MetadataHelper metadataHelper = prepareMetadataHelper(connection, this.getMetadataTableName(), auths, config.isRawTypes());
        
        DateIndexHelper dateIndexHelper = prepareDateIndexHelper(connection, this.getDateIndexTableName(), auths);
        if (config.isDateIndexTimeTravel()) {
            dateIndexHelper.setTimeTravel(config.isDateIndexTimeTravel());
        }
        
        QueryPlanner queryPlanner = getQueryPlanner();
        if (queryPlanner instanceof DefaultQueryPlanner) {
            DefaultQueryPlanner currentQueryPlanner = (DefaultQueryPlanner) queryPlanner;
            
            currentQueryPlanner.setMetadataHelper(metadataHelper);
            currentQueryPlanner.setDateIndexHelper(dateIndexHelper);
            
            QueryModelProvider queryModelProvider = currentQueryPlanner.getQueryModelProviderFactory().createQueryModelProvider();
            if (queryModelProvider instanceof MetadataHelperQueryModelProvider) {
                ((MetadataHelperQueryModelProvider) queryModelProvider).setMetadataHelper(metadataHelper);
                ((MetadataHelperQueryModelProvider) queryModelProvider).setConfig(config);
            }
            
            if (null != queryModelProvider.getQueryModel()) {
                queryModel = queryModelProvider.getQueryModel();
                
            }
        }
        
        if (this.queryModel == null)
            loadQueryModel(metadataHelper, config);
        
        getQueryPlanner().setCreateUidsIteratorClass(createUidsIteratorClass);
        getQueryPlanner().setUidIntersector(uidIntersector);
        
        validateConfiguration(config);
        
        if (getCardinalityConfiguration() != null && (!config.getBlacklistedFields().isEmpty() || !config.getProjectFields().isEmpty())) {
            // Ensure that fields used for resultCardinalities are returned. They will be removed in the DocumentTransformer.
            // Modify the projectFields and blacklistFields only for this stage, then return to the original values.
            // Not advisable to create a copy of the config object due to the embedded timers.
            Set<String> originalBlacklistedFields = new HashSet<>(config.getBlacklistedFields());
            Set<String> originalProjectFields = new HashSet<>(config.getProjectFields());
            
            // either projectFields or blacklistedFields can be used, but not both
            // this will be caught when loadQueryParameters is called
            if (!config.getBlacklistedFields().isEmpty()) {
                config.setBlacklistedFields(getCardinalityConfiguration().getRevisedBlacklistFields(queryModel, originalBlacklistedFields));
            }
            if (!config.getProjectFields().isEmpty()) {
                config.setProjectFields(getCardinalityConfiguration().getRevisedProjectFields(queryModel, originalProjectFields));
            }
            
            this.queries = getQueryPlanner().process(config, jexlQueryString, settings, this.getScannerFactory());
            
            config.setBlacklistedFields(originalBlacklistedFields);
            config.setProjectFields(originalProjectFields);
        } else {
            this.queries = getQueryPlanner().process(config, jexlQueryString, settings, this.getScannerFactory());
        }
        
        TraceStopwatch stopwatch = config.getTimers().newStartedStopwatch("ShardQueryLogic - Get iterator of queries");
        
        config.setQueries(this.queries.iterator());
        
        config.setQueryString(getQueryPlanner().getPlannedScript());
        
        stopwatch.stop();
    }
    
    /**
     * Validate that the configuration is in a consistent state
     *
     * @throws IllegalArgumentException
     *             when config constraints are violated
     */
    protected void validateConfiguration(ShardQueryConfiguration config) {
        // do not allow disabling track sizes unless page size is no more than 1
        if (!config.isTrackSizes() && this.getMaxPageSize() > 1) {
            throw new IllegalArgumentException("trackSizes cannot be disabled with a page size greater than 1");
        }
    }
    
    protected MetadataHelper prepareMetadataHelper(Connector connection, String metadataTableName, Set<Authorizations> auths) {
        return prepareMetadataHelper(connection, metadataTableName, auths, false);
    }
    
    protected MetadataHelper prepareMetadataHelper(Connector connection, String metadataTableName, Set<Authorizations> auths, boolean rawTypes) {
        if (log.isTraceEnabled())
            log.trace("prepareMetadataHelper with " + connection);
        return metadataHelperFactory.createMetadataHelper(connection, metadataTableName, auths, rawTypes);
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
    
    public DateIndexHelperFactory getDateIndexHelperFactory() {
        return dateIndexHelperFactory;
    }
    
    public void setDateIndexHelperFactory(DateIndexHelperFactory dateIndexHelperFactory) {
        this.dateIndexHelperFactory = dateIndexHelperFactory;
    }
    
    private DateIndexHelper prepareDateIndexHelper(Connector connection, String dateIndexTableName, Set<Authorizations> auths) {
        DateIndexHelper dateIndexHelper = this.dateIndexHelperFactory.createDateIndexHelper();
        return dateIndexHelper.initialize(connection, dateIndexTableName, auths, getDateIndexThreads(), getCollapseDatePercentThreshold());
    }
    
    @Override
    public void setupQuery(GenericQueryConfiguration genericConfig) throws Exception {
        if (!ShardQueryConfiguration.class.isAssignableFrom(genericConfig.getClass())) {
            throw new QueryException("Did not receive a ShardQueryConfiguration instance!!");
        }
        
        ShardQueryConfiguration config = (ShardQueryConfiguration) genericConfig;
        
        final QueryStopwatch timers = config.getTimers();
        TraceStopwatch stopwatch = timers.newStartedStopwatch("ShardQueryLogic - Setup Query");
        
        // Ensure we have all of the information needed to run a query
        if (!config.canRunQuery()) {
            log.warn("The given query '" + config + "' could not be run, most likely due to not matching any records in the global index.");
            
            // Stub out an iterator to correctly present "no results"
            this.iterator = new Iterator<Map.Entry<Key,Value>>() {
                @Override
                public boolean hasNext() {
                    return false;
                }
                
                @Override
                public Map.Entry<Key,Value> next() {
                    return null;
                }
                
                @Override
                public void remove() {
                    return;
                }
            };
            
            this.scanner = null;
            
            stopwatch.stop();
            
            log.info(getStopwatchHeader(config));
            List<String> timings = timers.summarizeAsList();
            for (String timing : timings) {
                log.info(timing);
            }
            
            return;
        }
        
        // Instantiate the scheduler for the queries
        this.scheduler = getScheduler(config, scannerFactory);
        
        this.scanner = null;
        this.iterator = this.scheduler.iterator();
        
        if (!config.isSortedUIDs()) {
            this.iterator = new DedupingIterator(this.iterator);
        }
        
        stopwatch.stop();
        
        log.info(getStopwatchHeader(config));
        List<String> timings = timers.summarizeAsList();
        for (String timing : timings) {
            log.info(timing);
        }
    }
    
    protected String getStopwatchHeader(ShardQueryConfiguration config) {
        return "ShardQueryLogic: " + config.getQueryString() + ", [" + config.getBeginDate() + ", " + config.getEndDate() + "]";
    }
    
    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        MarkingFunctions markingFunctions = this.getMarkingFunctions();
        ResponseObjectFactory responseObjectFactory = this.getResponseObjectFactory();
        
        boolean reducedInSettings = false;
        String reducedResponseStr = settings.findParameter(QueryOptions.REDUCED_RESPONSE).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(reducedResponseStr)) {
            reducedInSettings = Boolean.parseBoolean(reducedResponseStr);
        }
        boolean reduced = (this.isReducedResponse() || reducedInSettings);
        DocumentTransformer transformer = new DocumentTransformer(this, settings, markingFunctions, responseObjectFactory, reduced);
        transformer.setEventQueryDataDecoratorTransformer(eventQueryDataDecoratorTransformer);
        transformer.setContentFieldNames(getConfig().getContentFieldNames());
        transformer.setLogTimingDetails(this.getLogTimingDetails());
        transformer.setCardinalityConfiguration(cardinalityConfiguration);
        transformer.setPrimaryToSecondaryFieldMap(primaryToSecondaryFieldMap);
        transformer.setQm(queryModel);
        if (getConfig() != null) {
            transformer.setProjectFields(getConfig().getProjectFields());
            transformer.setBlacklistedFields(getConfig().getBlacklistedFields());
            if (getConfig().getUniqueFields() != null && !getConfig().getUniqueFields().isEmpty()) {
                transformer.addTransform(new UniqueTransform(this, getConfig().getUniqueFields()));
            }
            if (getConfig().getGroupFields() != null && !getConfig().getGroupFields().isEmpty()) {
                transformer.addTransform(new GroupingTransform(this, getConfig().getGroupFields()));
            }
        }
        
        return transformer;
    }
    
    protected void loadQueryParameters(ShardQueryConfiguration config, Query settings) throws QueryException {
        TraceStopwatch stopwatch = config.getTimers().newStartedStopwatch("ShardQueryLogic - Parse query parameters");
        boolean rawDataOnly = false;
        String rawDataOnlyStr = settings.findParameter(QueryParameters.RAW_DATA_ONLY).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(rawDataOnlyStr)) {
            rawDataOnly = Boolean.valueOf(rawDataOnlyStr);
            // if the master option raw.data.only is set, then set all of the transforming options appropriately.
            // note that if any of these other options are set, then it overrides the settings here
            if (rawDataOnly) {
                // set the grouping context to trye to ensure we get the full field names
                this.setIncludeGroupingContext(true);
                config.setIncludeGroupingContext(true);
                // set the hierarchy fields to false as they are generated fields
                this.setIncludeHierarchyFields(false);
                config.setIncludeHierarchyFields(false);
                // set the datatype field to false as it is a generated field
                this.setIncludeDataTypeAsField(false);
                config.setIncludeDataTypeAsField(false);
                // do not include the record id
                this.setIncludeRecordId(false);
                config.setIncludeRecordId(false);
                // set the hit list to false as it is a generated field
                this.setHitList(false);
                config.setHitList(false);
                // set the raw types to true to avoid any type transformations of the values
                config.setRawTypes(true);
                // do not filter masked values
                this.setFilterMaskedValues(false);
                config.setFilterMaskedValues(false);
                // do not reduce the response
                this.setReducedResponse(false);
                config.setReducedResponse(false);
                // clear the content field names to prevent content field transformations (see DocumentTransformer)
                this.setContentFieldNames(Collections.EMPTY_LIST);
                // clear the model name to avoid field name translations
                this.setModelName(null);
                config.setModelName(null);
            }
        }
        
        // Get the datatype set if specified
        String typeList = settings.findParameter(QueryParameters.DATATYPE_FILTER_SET).getParameterValue().trim();
        
        if (org.apache.commons.lang.StringUtils.isNotBlank(typeList)) {
            HashSet<String> typeFilter = new HashSet<>();
            typeFilter.addAll(Arrays.asList(StringUtils.split(typeList, Constants.PARAM_VALUE_SEP)));
            
            if (log.isDebugEnabled()) {
                log.debug("Type Filter: " + typeFilter);
            }
            
            config.setDatatypeFilter(typeFilter);
        }
        
        // Get the list of fields to project up the stack. May be null.
        String projectFields = settings.findParameter(QueryParameters.RETURN_FIELDS).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(projectFields)) {
            List<String> projectFieldsList = Arrays.asList(StringUtils.split(projectFields, Constants.PARAM_VALUE_SEP));
            
            // Only set the projection fields if we were actually given some
            if (!projectFieldsList.isEmpty()) {
                config.setProjectFields(new HashSet<>(projectFieldsList));
                
                if (log.isDebugEnabled()) {
                    final int maxLen = 100;
                    // Trim down the projection if it's stupid long
                    projectFields = maxLen < projectFields.length() ? projectFields.substring(0, maxLen) + "[TRUNCATED]" : projectFields;
                    log.debug("Projection fields: " + projectFields);
                }
            }
        }
        
        // if the TRANFORM_CONTENT_TO_UID is false, then unset the list of content field names preventing the DocumentTransformer from
        // transforming them.
        String transformContentStr = settings.findParameter(QueryParameters.TRANFORM_CONTENT_TO_UID).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(transformContentStr)) {
            if (!Boolean.valueOf(transformContentStr)) {
                setContentFieldNames(Collections.EMPTY_LIST);
            }
        }
        
        // Get the list of blacklisted fields. May be null.
        String tBlacklistedFields = settings.findParameter(QueryParameters.BLACKLISTED_FIELDS).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(tBlacklistedFields)) {
            List<String> blacklistedFieldsList = Arrays.asList(StringUtils.split(tBlacklistedFields, Constants.PARAM_VALUE_SEP));
            
            // Only set the blacklisted fields if we were actually given some
            if (!blacklistedFieldsList.isEmpty()) {
                if (!config.getProjectFields().isEmpty()) {
                    throw new QueryException("Whitelist and blacklist projection options are mutually exclusive");
                }
                
                config.setBlacklistedFields(new HashSet<>(blacklistedFieldsList));
                
                if (log.isDebugEnabled()) {
                    log.debug("Blacklisted fields: " + tBlacklistedFields);
                }
            }
        }
        
        // Get the LIMIT_FIELDS parameter if given
        String limitFields = settings.findParameter(QueryParameters.LIMIT_FIELDS).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(limitFields)) {
            List<String> limitFieldsList = Arrays.asList(StringUtils.split(limitFields, Constants.PARAM_VALUE_SEP));
            
            // Only set the limit fields if we were actually given some
            if (!limitFieldsList.isEmpty()) {
                config.setLimitFields(new HashSet<>(limitFieldsList));
            }
        }
        
        String limitFieldsPreQueryEvaluation = settings.findParameter(QueryOptions.LIMIT_FIELDS_PRE_QUERY_EVALUATION).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(limitFieldsPreQueryEvaluation)) {
            Boolean limitFieldsPreQueryEvaluationValue = Boolean.parseBoolean(limitFieldsPreQueryEvaluation);
            this.setLimitFieldsPreQueryEvaluation(limitFieldsPreQueryEvaluationValue);
            config.setLimitFieldsPreQueryEvaluation(limitFieldsPreQueryEvaluationValue);
        }
        
        String limitFieldsField = settings.findParameter(QueryOptions.LIMIT_FIELDS_FIELD).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(limitFieldsField)) {
            this.setLimitFieldsField(limitFieldsField);
            config.setLimitFieldsField(limitFieldsField);
        }
        
        // Get the GROUP_FIELDS parameter if given
        String groupFields = settings.findParameter(QueryParameters.GROUP_FIELDS).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(groupFields)) {
            List<String> groupFieldsList = Arrays.asList(StringUtils.split(groupFields, Constants.PARAM_VALUE_SEP));
            
            // Only set the group fields if we were actually given some
            if (!groupFieldsList.isEmpty()) {
                this.setGroupFields(new HashSet<>(groupFieldsList));
                config.setGroupFields(new HashSet<>(groupFieldsList));
                config.setProjectFields(new HashSet<>(groupFieldsList));
            }
        }
        
        String groupFieldsBatchSizeString = settings.findParameter(QueryParameters.GROUP_FIELDS_BATCH_SIZE).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(groupFieldsBatchSizeString)) {
            int groupFieldsBatchSize = Integer.parseInt(groupFieldsBatchSizeString);
            this.setGroupFieldsBatchSize(groupFieldsBatchSize);
            config.setGroupFieldsBatchSize(groupFieldsBatchSize);
        }
        
        // Get the UNIQUE_FIELDS parameter if given
        String uniqueFields = settings.findParameter(QueryParameters.UNIQUE_FIELDS).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(uniqueFields)) {
            List<String> uniqueFieldsList = Arrays.asList(StringUtils.split(uniqueFields, Constants.PARAM_VALUE_SEP));
            
            // Only set the unique fields if we were actually given some
            if (!uniqueFieldsList.isEmpty()) {
                this.setUniqueFields(new HashSet<>(uniqueFieldsList));
                config.setUniqueFields(new HashSet<>(uniqueFieldsList));
            }
        }
        
        // Get the HIT_LIST parameter if given
        String hitListString = settings.findParameter(QueryParameters.HIT_LIST).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(hitListString)) {
            Boolean hitListBool = Boolean.parseBoolean(hitListString);
            config.setHitList(hitListBool);
        }
        
        // Get the TYPE_METADATA_IN_HDFS parameter if given
        String typeMetadataInHdfsString = settings.findParameter(QueryParameters.TYPE_METADATA_IN_HDFS).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(typeMetadataInHdfsString)) {
            Boolean typeMetadataInHdfsBool = Boolean.parseBoolean(typeMetadataInHdfsString);
            config.setTypeMetadataInHdfs(typeMetadataInHdfsBool);
        }
        
        // Get the BYPASS_ACCUMULO parameter if given
        String bypassAccumuloString = settings.findParameter(BYPASS_ACCUMULO).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(bypassAccumuloString)) {
            Boolean bypassAccumuloBool = Boolean.parseBoolean(bypassAccumuloString);
            config.setBypassAccumulo(bypassAccumuloBool);
        }
        
        // Get the DATE_INDEX_TIME_TRAVEL parameter if given
        String dateIndexTimeTravelString = settings.findParameter(QueryOptions.DATE_INDEX_TIME_TRAVEL).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(dateIndexTimeTravelString)) {
            Boolean dateIndexTimeTravel = Boolean.parseBoolean(dateIndexTimeTravelString);
            config.setDateIndexTimeTravel(dateIndexTimeTravel);
        }
        
        // get the RAW_TYPES parameter if given
        String rawTypesString = settings.findParameter(QueryParameters.RAW_TYPES).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(rawTypesString)) {
            Boolean rawTypesBool = Boolean.parseBoolean(rawTypesString);
            config.setRawTypes(rawTypesBool);
            // if raw types are going to be replaced in the type metadata, we cannot use the hdfs-cached typemetadata
            // these properties are mutually exclusive
            if (rawTypesBool) {
                config.setTypeMetadataInHdfs(false);
            }
        }
        
        // Get the FILTER_MASKED_VALUES spring setting
        String filterMaskedValuesStr = settings.findParameter(QueryParameters.FILTER_MASKED_VALUES).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(filterMaskedValuesStr)) {
            Boolean filterMaskedValuesBool = Boolean.parseBoolean(filterMaskedValuesStr);
            this.setFilterMaskedValues(filterMaskedValuesBool);
            config.setFilterMaskedValues(filterMaskedValuesBool);
        }
        
        // Get the INCLUDE_DATATYPE_AS_FIELD spring setting
        String includeDatatypeAsFieldStr = settings.findParameter(QueryParameters.INCLUDE_DATATYPE_AS_FIELD).getParameterValue().trim();
        if (((org.apache.commons.lang.StringUtils.isNotBlank(includeDatatypeAsFieldStr) && Boolean.valueOf(includeDatatypeAsFieldStr)))
                        || (this.getIncludeDataTypeAsField() && !rawDataOnly)) {
            config.setIncludeDataTypeAsField(true);
        }
        
        // Get the INCLUDE_RECORD_ID spring setting
        String includeRecordIdStr = settings.findParameter(QueryParameters.INCLUDE_RECORD_ID).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(includeRecordIdStr)) {
            boolean includeRecordIdBool = Boolean.parseBoolean(includeRecordIdStr) && !rawDataOnly;
            this.setIncludeRecordId(includeRecordIdBool);
            config.setIncludeRecordId(includeRecordIdBool);
        }
        
        // Get the INCLUDE_HIERARCHY_FIELDS spring setting
        String includeHierarchyFieldsStr = settings.findParameter(QueryParameters.INCLUDE_HIERARCHY_FIELDS).getParameterValue().trim();
        if (((org.apache.commons.lang.StringUtils.isNotBlank(includeHierarchyFieldsStr) && Boolean.valueOf(includeHierarchyFieldsStr)))
                        || (this.getIncludeHierarchyFields() && !rawDataOnly)) {
            config.setIncludeHierarchyFields(true);
            
            final Map<String,String> options = this.getHierarchyFieldOptions();
            config.setHierarchyFieldOptions(options);
        }
        
        // Get the query profile to allow us to select the tune profile of the query
        String queryProfile = settings.findParameter(QueryParameters.QUERY_PROFILE).getParameterValue().trim();
        if ((org.apache.commons.lang.StringUtils.isNotBlank(queryProfile))) {
            
            selectedProfile = configuredProfiles.get(queryProfile);
            
            if (null == selectedProfile) {
                throw new QueryException(QueryParameters.QUERY_PROFILE + " has been specified but " + queryProfile + " is not a selectable profile");
            }
            
        }
        
        // Get the include.grouping.context = true/false spring setting
        String includeGroupingContextStr = settings.findParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT).getParameterValue().trim();
        if (((org.apache.commons.lang.StringUtils.isNotBlank(includeGroupingContextStr) && Boolean.valueOf(includeGroupingContextStr)))
                        || (this.getIncludeGroupingContext() && !rawDataOnly)) {
            config.setIncludeGroupingContext(true);
        }
        
        // Check if the default modelName and modelTableNames have been overridden by custom parameters.
        String parameterModelName = settings.findParameter(QueryParameters.PARAMETER_MODEL_NAME).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(parameterModelName)) {
            this.setModelName(parameterModelName);
        }
        
        config.setModelName(this.getModelName());
        
        String parameterModelTableName = settings.findParameter(QueryParameters.PARAMETER_MODEL_TABLE_NAME).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(parameterModelTableName)) {
            this.setModelTableName(parameterModelTableName);
        }
        
        if (null != config.getModelName() && null == config.getModelTableName()) {
            throw new IllegalArgumentException(QueryParameters.PARAMETER_MODEL_NAME + " has been specified but " + QueryParameters.PARAMETER_MODEL_TABLE_NAME
                            + " is missing. Both are required to use a model");
        }
        
        configureDocumentAggregation(settings);
        
        config.setLimitTermExpansionToModel(this.isExpansionLimitedToModelContents());
        
        String reducedResponseStr = settings.findParameter(QueryOptions.REDUCED_RESPONSE).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(reducedResponseStr)) {
            Boolean reducedResponseValue = Boolean.parseBoolean(reducedResponseStr);
            this.setReducedResponse(reducedResponseValue);
            config.setReducedResponse(reducedResponseValue);
        }
        
        final String postProcessingClasses = settings.findParameter(QueryOptions.POSTPROCESSING_CLASSES).getParameterValue().trim();
        
        final String postProcessingOptions = settings.findParameter(QueryOptions.POSTPROCESSING_OPTIONS).getParameterValue().trim();
        
        // build the post p
        if (org.apache.commons.lang.StringUtils.isNotBlank(postProcessingClasses)) {
            
            List<String> filterClasses = config.getFilterClassNames();
            if (null == filterClasses) {
                filterClasses = new ArrayList<>();
            }
            
            for (String fClassName : StringUtils.splitIterable(postProcessingClasses, ',', true)) {
                filterClasses.add(fClassName);
            }
            config.setFilterClassNames(filterClasses);
            
            final Map<String,String> options = this.getFilterOptions();
            if (null != options) {
                config.putFilterOptions(options);
            }
            
            if (org.apache.commons.lang.StringUtils.isNotBlank(postProcessingOptions)) {
                for (String filterOptionStr : StringUtils.splitIterable(postProcessingOptions, ',', true)) {
                    if (org.apache.commons.lang.StringUtils.isNotBlank(filterOptionStr)) {
                        final String filterValueString = settings.findParameter(filterOptionStr).getParameterValue().trim();
                        if (org.apache.commons.lang.StringUtils.isNotBlank(filterValueString)) {
                            config.putFilterOptions(filterOptionStr, filterValueString);
                        }
                    }
                }
            }
        }
        
        String tCompressServerSideResults = settings.findParameter(QueryOptions.COMPRESS_SERVER_SIDE_RESULTS).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(tCompressServerSideResults)) {
            boolean compress = Boolean.parseBoolean(tCompressServerSideResults);
            config.setCompressServerSideResults(compress);
        }
        
        // Configure index-only filter functions to be enabled if not already set to such a state
        config.setIndexOnlyFilterFunctionsEnabled(this.isIndexOnlyFilterFunctionsEnabled());
        
        // Set the ReturnType for Documents coming out of the iterator stack
        config.setReturnType(DocumentSerialization.getReturnType(settings));
        
        QueryLogicTransformer transformer = getTransformer(settings);
        if (transformer instanceof WritesQueryMetrics) {
            String logTimingDetailsStr = settings.findParameter(QueryOptions.LOG_TIMING_DETAILS).getParameterValue().trim();
            if (org.apache.commons.lang.StringUtils.isNotBlank(logTimingDetailsStr)) {
                setLogTimingDetails(Boolean.valueOf(logTimingDetailsStr));
            }
            if (getLogTimingDetails()) {
                // we have to collect the timing details on the iterator stack in order to log them
                setCollectTimingDetails(true);
            } else {
                
                String collectTimingDetailsStr = settings.findParameter(QueryOptions.COLLECT_TIMING_DETAILS).getParameterValue().trim();
                if (org.apache.commons.lang.StringUtils.isNotBlank(collectTimingDetailsStr)) {
                    setCollectTimingDetails(Boolean.valueOf(collectTimingDetailsStr));
                }
            }
        } else {
            // if the transformer can not process the timing metrics, then turn them off
            setLogTimingDetails(false);
            setCollectTimingDetails(false);
        }
        
        stopwatch.stop();
        
        if (null != selectedProfile) {
            selectedProfile.configure(this);
            selectedProfile.configure(config);
            selectedProfile.configure(planner);
        }
    }
    
    void configureDocumentAggregation(Query settings) {
        Parameter disabledIndexOnlyDocument = settings.findParameter(QueryOptions.DISABLE_DOCUMENTS_WITHOUT_EVENTS);
        if (null != disabledIndexOnlyDocument) {
            final String disabledIndexOnlyDocumentStr = disabledIndexOnlyDocument.getParameterValue().trim();
            if (org.apache.commons.lang.StringUtils.isNotBlank(disabledIndexOnlyDocumentStr)) {
                Boolean disabledIndexOnlyDocuments = Boolean.parseBoolean(disabledIndexOnlyDocumentStr);
                setDisableIndexOnlyDocuments(disabledIndexOnlyDocuments);
            }
        }
    }
    
    /**
     * Loads a query Model
     *
     * @param helper
     * @param config
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws TableNotFoundException
     * @throws ExecutionException
     */
    protected void loadQueryModel(MetadataHelper helper, ShardQueryConfiguration config) throws InstantiationException, IllegalAccessException,
                    TableNotFoundException, ExecutionException {
        TraceStopwatch modelWatch = config.getTimers().newStartedStopwatch("ShardQueryLogic - Loading the query model");
        
        int cacheKeyCode = new HashCodeBuilder().append(config.getDatatypeFilter()).append(config.getModelName()).hashCode();
        
        if (config.getCacheModel()) {
            queryModel = queryModelMap.getIfPresent(String.valueOf(cacheKeyCode));
        }
        if (null == queryModel && (null != config.getModelName() && null != config.getModelTableName())) {
            
            queryModel = helper.getQueryModel(config.getModelTableName(), config.getModelName(), helper.getIndexOnlyFields(config.getDatatypeFilter()));
            
            if (config.getCacheModel()) {
                
                queryModelMap.put(String.valueOf(cacheKeyCode), queryModel);
            }
            
        }
        config.setQueryModel(queryModel);
        
        modelWatch.stop();
        
    }
    
    protected Scheduler getScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory) {
        if (config.getSequentialScheduler()) {
            return new SequentialScheduler(config, scannerFactory);
        } else {
            return new PushdownScheduler(config, scannerFactory, this.metadataHelperFactory);
        }
    }
    
    public EventQueryDataDecoratorTransformer getEventQueryDataDecoratorTransformer() {
        return eventQueryDataDecoratorTransformer;
    }
    
    public void setEventQueryDataDecoratorTransformer(EventQueryDataDecoratorTransformer eventQueryDataDecoratorTransformer) {
        this.eventQueryDataDecoratorTransformer = eventQueryDataDecoratorTransformer;
    }
    
    @Override
    public ShardQueryLogic clone() {
        return new ShardQueryLogic(this);
    }
    
    @Override
    public void close() {
        
        super.close();
        
        log.debug("Closing ShardQueryLogic: " + System.identityHashCode(this));
        
        if (null == scannerFactory) {
            log.debug("ScannerFactory was never initialized because, therefore there are no connections to close: " + System.identityHashCode(this));
        } else {
            log.debug("Closing ShardQueryLogic scannerFactory: " + System.identityHashCode(this));
            try {
                int nClosed = 0;
                scannerFactory.lockdown();
                for (ScannerBase bs : Lists.newArrayList(scannerFactory.currentScanners())) {
                    scannerFactory.close(bs);
                    ++nClosed;
                }
                if (log.isDebugEnabled()) {
                    log.debug("Cleaned up " + nClosed + " batch scanners associated with this query logic.");
                }
                
                nClosed = 0;
                
                for (ScannerSession bs : Lists.newArrayList(scannerFactory.currentSessions())) {
                    scannerFactory.close(bs);
                    ++nClosed;
                }
                
                if (log.isDebugEnabled()) {
                    log.debug("Cleaned up " + nClosed + " scanner sessions.");
                }
                
            } catch (Exception e) {
                log.error("Caught exception trying to close scannerFactory", e);
            }
            
        }
        
        if (null != this.planner) {
            try {
                log.debug("Closing ShardQueryLogic planner: " + System.identityHashCode(this) + '('
                                + (this.getSettings() == null ? "empty" : this.getSettings().getId()) + ')');
                this.planner.close(getConfig(), this.getSettings());
            } catch (Exception e) {
                log.error("Caught exception trying to close QueryPlanner", e);
            }
        }
        
        if (null != this.queries) {
            try {
                log.debug("Closing ShardQueryLogic queries: " + System.identityHashCode(this));
                this.queries.close();
            } catch (IOException e) {
                log.error("Caught exception trying to close CloseableIterable of queries", e);
            }
        }
        
        if (null != this.scheduler) {
            try {
                log.debug("Closing ShardQueryLogic scheduler: " + System.identityHashCode(this));
                this.scheduler.close();
                
                ScanSessionStats stats = this.scheduler.getSchedulerStats();
                
                if (null != stats) {
                    stats.logSummary(log);
                }
                
            } catch (IOException e) {
                log.error("Caught exception trying to close Scheduler", e);
            }
        }
        
    }
    
    @Override
    public ShardQueryConfiguration getConfig() {
        if (config == null) {
            config = ShardQueryConfiguration.create();
        }
        
        return config;
    }
    
    public void setConfig(ShardQueryConfiguration config) {
        this.config = config;
    }
    
    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }
    
    public boolean getFilterMaskedValues() {
        return getConfig().getFilterMaskedValues();
    }
    
    public void setFilterMaskedValues(boolean filterMaskedValues) {
        getConfig().setFilterMaskedValues(filterMaskedValues);
    }
    
    public boolean getIncludeDataTypeAsField() {
        return getConfig().getIncludeDataTypeAsField();
    }
    
    public void setIncludeDataTypeAsField(boolean includeDataTypeAsField) {
        getConfig().setIncludeDataTypeAsField(includeDataTypeAsField);
    }
    
    public boolean getIncludeRecordId() {
        return getConfig().getIncludeRecordId();
    }
    
    public void setIncludeRecordId(boolean includeRecordId) {
        getConfig().setIncludeRecordId(includeRecordId);
    }
    
    public boolean getIncludeHierarchyFields() {
        return getConfig().getIncludeHierarchyFields();
    }
    
    public void setIncludeHierarchyFields(boolean includeHierarchyFields) {
        getConfig().setIncludeHierarchyFields(includeHierarchyFields);
    }
    
    public List<String> getDocumentPermutations() {
        return getConfig().getDocumentPermutations();
    }
    
    public void setDocumentPermutations(List<String> documentPermutations) {
        getConfig().setDocumentPermutations(documentPermutations);
    }
    
    public Map<String,String> getHierarchyFieldOptions() {
        return getConfig().getHierarchyFieldOptions();
    }
    
    public void setHierarchyFieldOptions(final Map<String,String> options) {
        getConfig().setHierarchyFieldOptions(options);
    }
    
    public Set<String> getBlacklistedFields() {
        return getConfig().getBlacklistedFields();
    }
    
    public void setBlacklistedFields(Set<String> blacklistedFields) {
        getConfig().setBlacklistedFields(blacklistedFields);
    }
    
    public Set<String> getLimitFields() {
        return getConfig().getLimitFields();
    }
    
    public void setLimitFields(Set<String> limitFields) {
        getConfig().setLimitFields(limitFields);
    }
    
    public boolean isLimitFieldsPreQueryEvaluation() {
        return getConfig().isLimitFieldsPreQueryEvaluation();
    }
    
    public void setLimitFieldsPreQueryEvaluation(boolean limitFieldsPreQueryEvaluation) {
        getConfig().setLimitFieldsPreQueryEvaluation(limitFieldsPreQueryEvaluation);
    }
    
    public String getLimitFieldsField() {
        return getConfig().getLimitFieldsField();
    }
    
    public void setLimitFieldsField(String limitFieldsField) {
        getConfig().setLimitFieldsField(limitFieldsField);
    }
    
    public Set<String> getGroupFields() {
        return getConfig().getGroupFields();
    }
    
    public void setGroupFields(Set<String> groupFields) {
        getConfig().setGroupFields(groupFields);
    }
    
    public void setGroupFieldsBatchSize(int groupFieldsBatchSize) {
        getConfig().setGroupFieldsBatchSize(groupFieldsBatchSize);
    }
    
    public int getGroupFieldsBatchSize() {
        return getConfig().getGroupFieldsBatchSize();
    }
    
    public Set<String> getUniqueFields() {
        return getConfig().getUniqueFields();
    }
    
    public void setUniqueFields(Set<String> uniqueFields) {
        getConfig().setUniqueFields(uniqueFields);
    }
    
    public String getBlacklistedFieldsString() {
        return getConfig().getBlacklistedFieldsAsString();
    }
    
    public boolean getIncludeGroupingContext() {
        return getConfig().getIncludeGroupingContext();
    }
    
    public void setIncludeGroupingContext(boolean opt) {
        getConfig().setIncludeGroupingContext(opt);
    }
    
    public boolean isReducedResponse() {
        return getConfig().isReducedResponse();
    }
    
    public void setReducedResponse(boolean reducedResponse) {
        getConfig().setReducedResponse(reducedResponse);
    }
    
    public boolean isDisableEvaluation() {
        return getConfig().isDisableEvaluation();
    }
    
    public void setDisableEvaluation(boolean disableEvaluation) {
        getConfig().setDisableEvaluation(disableEvaluation);
    }
    
    public boolean disableIndexOnlyDocuments() {
        return getConfig().isDisableIndexOnlyDocuments();
    }
    
    public void setDisableIndexOnlyDocuments(boolean disableIndexOnlyDocuments) {
        getConfig().setDisableIndexOnlyDocuments(disableIndexOnlyDocuments);
    }
    
    public boolean isHitList() {
        return getConfig().isHitList();
    }
    
    public void setHitList(boolean hitList) {
        getConfig().setHitList(hitList);
    }
    
    public boolean isTypeMetadataInHdfs() {
        return getConfig().isTypeMetadataInHdfs();
    }
    
    public void setTypeMetadataInHdfs(boolean typeMetadataInHdfs) {
        getConfig().setTypeMetadataInHdfs(typeMetadataInHdfs);
    }
    
    public int getEventPerDayThreshold() {
        return getConfig().getEventPerDayThreshold();
    }
    
    public void setEventPerDayThreshold(int eventPerDayThreshold) {
        getConfig().setEventPerDayThreshold(eventPerDayThreshold);
    }
    
    public int getShardsPerDayThreshold() {
        return getConfig().getShardsPerDayThreshold();
    }
    
    public void setShardsPerDayThreshold(int shardsPerDayThreshold) {
        getConfig().setShardsPerDayThreshold(shardsPerDayThreshold);
    }
    
    public int getMaxTermThreshold() {
        return getConfig().getMaxTermThreshold();
    }
    
    public void setMaxTermThreshold(int maxTermThreshold) {
        getConfig().setMaxTermThreshold(maxTermThreshold);
    }
    
    public int getMaxDepthThreshold() {
        return getConfig().getMaxDepthThreshold();
    }
    
    public void setMaxDepthThreshold(int maxDepthThreshold) {
        getConfig().setMaxDepthThreshold(maxDepthThreshold);
    }
    
    public int getMaxUnfieldedExpansionThreshold() {
        return getConfig().getMaxUnfieldedExpansionThreshold();
    }
    
    public void setMaxUnfieldedExpansionThreshold(int maxUnfieldedExpansionThreshold) {
        getConfig().setMaxUnfieldedExpansionThreshold(maxUnfieldedExpansionThreshold);
    }
    
    public int getMaxValueExpansionThreshold() {
        return getConfig().getMaxValueExpansionThreshold();
    }
    
    public void setMaxValueExpansionThreshold(int maxValueExpansionThreshold) {
        getConfig().setMaxValueExpansionThreshold(maxValueExpansionThreshold);
    }
    
    public int getMaxOrExpansionThreshold() {
        return getConfig().getMaxOrExpansionThreshold();
    }
    
    public void setMaxOrExpansionThreshold(int maxOrExpansionThreshold) {
        getConfig().setMaxOrExpansionThreshold(maxOrExpansionThreshold);
    }
    
    public int getMaxOrExpansionFstThreshold() {
        return getConfig().getMaxOrExpansionFstThreshold();
    }
    
    public void setMaxOrRangeThreshold(int maxOrRangeThreshold) {
        this.config.setMaxOrRangeThreshold(maxOrRangeThreshold);
    }
    
    public int getMaxOrRangeThreshold() {
        return this.config.getMaxOrRangeThreshold();
    }
    
    public void setMaxOrExpansionFstThreshold(int maxOrExpansionFstThreshold) {
        getConfig().setMaxOrExpansionFstThreshold(maxOrExpansionFstThreshold);
    }
    
    public int getMaxRangesPerRangeIvarator() {
        return getConfig().getMaxRangesPerRangeIvarator();
    }
    
    public void setMaxRangesPerRangeIvarator(int maxRangesPerRangeIvarator) {
        this.config.setMaxRangesPerRangeIvarator(maxRangesPerRangeIvarator);
    }
    
    public int getMaxOrRangeIvarators() {
        return this.config.getMaxOrRangeIvarators();
    }
    
    public void setMaxOrRangeIvarators(int maxOrRangeIvarators) {
        this.config.setMaxOrRangeIvarators(maxOrRangeIvarators);
    }
    
    public long getYieldThresholdMs() {
        return getConfig().getYieldThresholdMs();
    }
    
    public void setYieldThresholdMs(long yieldThresholdMs) {
        getConfig().setYieldThresholdMs(yieldThresholdMs);
    }
    
    public boolean isCleanupShardsAndDaysQueryHints() {
        return getConfig().isCleanupShardsAndDaysQueryHints();
    }
    
    public void setCleanupShardsAndDaysQueryHints(boolean cleanupShardsAndDaysQueryHints) {
        getConfig().setCleanupShardsAndDaysQueryHints(cleanupShardsAndDaysQueryHints);
    }
    
    public String getDateIndexTableName() {
        return getConfig().getDateIndexTableName();
    }
    
    public void setDateIndexTableName(String dateIndexTableName) {
        getConfig().setDateIndexTableName(dateIndexTableName);
    }
    
    public String getDefaultDateTypeName() {
        return getConfig().getDefaultDateTypeName();
    }
    
    public void setDefaultDateTypeName(String defaultDateTypeName) {
        getConfig().setDefaultDateTypeName(defaultDateTypeName);
    }
    
    public String getMetadataTableName() {
        return getConfig().getMetadataTableName();
    }
    
    public void setMetadataTableName(String metadataTableName) {
        getConfig().setMetadataTableName(metadataTableName);
    }
    
    public String getIndexTableName() {
        return getConfig().getIndexTableName();
    }
    
    public void setIndexTableName(String indexTableName) {
        getConfig().setIndexTableName(indexTableName);
    }
    
    public String getIndexStatsTableName() {
        return getConfig().getIndexStatsTableName();
    }
    
    public void setIndexStatsTableName(String indexStatsTableName) {
        getConfig().setIndexStatsTableName(indexStatsTableName);
    }
    
    public String getModelTableName() {
        return getConfig().getModelTableName();
    }
    
    public void setModelTableName(String modelTableName) {
        getConfig().setModelTableName(modelTableName);
    }
    
    public String getModelName() {
        return getConfig().getModelName();
    }
    
    public void setModelName(String modelName) {
        getConfig().setModelName(modelName);
    }
    
    public int getQueryThreads() {
        return getConfig().getNumQueryThreads();
    }
    
    public void setQueryThreads(int queryThreads) {
        getConfig().setNumQueryThreads(queryThreads);
    }
    
    public int getIndexLookupThreads() {
        return getConfig().getNumIndexLookupThreads();
    }
    
    public void setIndexLookupThreads(int indexLookupThreads) {
        getConfig().setNumIndexLookupThreads(indexLookupThreads);
    }
    
    public int getDateIndexThreads() {
        return getConfig().getNumDateIndexThreads();
    }
    
    public void setDateIndexThreads(int indexThreads) {
        getConfig().setNumDateIndexThreads(indexThreads);
    }
    
    public int getMaxDocScanTimeout() {
        return getConfig().getMaxDocScanTimeout();
    }
    
    public void setMaxDocScanTimeout(int maxDocScanTimeout) {
        getConfig().setMaxDocScanTimeout(maxDocScanTimeout);
    }
    
    public float getCollapseDatePercentThreshold() {
        return getConfig().getCollapseDatePercentThreshold();
    }
    
    public void setCollapseDatePercentThreshold(float collapseDatePercentThreshold) {
        getConfig().setCollapseDatePercentThreshold(collapseDatePercentThreshold);
    }
    
    public List<String> getEnricherClassNames() {
        return getConfig().getEnricherClassNames();
    }
    
    public void setEnricherClassNames(List<String> enricherClassNames) {
        getConfig().setEnricherClassNames(enricherClassNames);
    }
    
    public boolean isUseEnrichers() {
        return getConfig().getUseEnrichers();
    }
    
    public void setUseEnrichers(boolean useEnrichers) {
        getConfig().setUseEnrichers(useEnrichers);
    }
    
    public boolean isTldQuery() {
        return getConfig().isTldQuery();
    }
    
    public void setIsTldQuery(boolean isTldQuery) {
        getConfig().setTldQuery(isTldQuery);
    }
    
    public boolean isExpandAllTerms() {
        return getConfig().isExpandAllTerms();
    }
    
    public void setExpandAllTerms(boolean expandAllTerms) {
        getConfig().setExpandAllTerms(expandAllTerms);
    }
    
    public List<String> getFilterClassNames() {
        return getConfig().getFilterClassNames();
    }
    
    public void setFilterClassNames(List<String> filterClassNames) {
        getConfig().setFilterClassNames(filterClassNames);
    }
    
    public List<String> getIndexFilteringClassNames() {
        return getConfig().getIndexFilteringClassNames();
    }
    
    public void setIndexFilteringClassNames(List<String> classNames) {
        getConfig().setIndexFilteringClassNames(classNames);
    }
    
    public Map<String,String> getFilterOptions() {
        return getConfig().getFilterOptions();
    }
    
    public void setFilterOptions(final Map<String,String> options) {
        getConfig().putFilterOptions(options);
    }
    
    public boolean isUseFilters() {
        return getConfig().getUseFilters();
    }
    
    public void setUseFilters(boolean useFilters) {
        getConfig().setUseFilters(useFilters);
    }
    
    public String getReverseIndexTableName() {
        return getConfig().getReverseIndexTableName();
    }
    
    public void setReverseIndexTableName(String reverseIndexTableName) {
        getConfig().setReverseIndexTableName(reverseIndexTableName);
    }
    
    public Set<String> getUnevaluatedFields() {
        return getConfig().getUnevaluatedFields();
    }
    
    public void setUnevaluatedFields(String unevaluatedFieldList) {
        getConfig().setUnevaluatedFields(unevaluatedFieldList);
    }
    
    public void setUnevaluatedFields(Collection<String> unevaluatedFields) {
        getConfig().setUnevaluatedFields(unevaluatedFields);
    }
    
    public Class<? extends Type<?>> getDefaultType() {
        return getConfig().getDefaultType();
    }
    
    public void setDefaultType(Class<? extends Type<?>> defaultType) {
        getConfig().setDefaultType(defaultType);
    }
    
    @SuppressWarnings("unchecked")
    public void setDefaultType(String className) {
        getConfig().setDefaultType(className);
    }
    
    public boolean isFullTableScanEnabled() {
        return getConfig().getFullTableScanEnabled();
    }
    
    public void setFullTableScanEnabled(boolean fullTableScanEnabled) {
        getConfig().setFullTableScanEnabled(fullTableScanEnabled);
    }
    
    public List<IvaratorCacheDirConfig> getIvaratorCacheDirConfigs() {
        return getConfig().getIvaratorCacheDirConfigs();
    }
    
    public void setIvaratorCacheDirConfigs(List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs) {
        getConfig().setIvaratorCacheDirConfigs(ivaratorCacheDirConfigs);
    }
    
    public String getIvaratorFstHdfsBaseURIs() {
        return getConfig().getIvaratorFstHdfsBaseURIs();
    }
    
    public void setIvaratorFstHdfsBaseURIs(String ivaratorFstHdfsBaseURIs) {
        getConfig().setIvaratorFstHdfsBaseURIs(ivaratorFstHdfsBaseURIs);
    }
    
    public int getIvaratorCacheBufferSize() {
        return getConfig().getIvaratorCacheBufferSize();
    }
    
    public void setIvaratorCacheBufferSize(int ivaratorCacheBufferSize) {
        getConfig().setIvaratorCacheBufferSize(ivaratorCacheBufferSize);
    }
    
    public long getIvaratorCacheScanPersistThreshold() {
        return getConfig().getIvaratorCacheScanPersistThreshold();
    }
    
    public void setIvaratorCacheScanPersistThreshold(long ivaratorCacheScanPersistThreshold) {
        getConfig().setIvaratorCacheScanPersistThreshold(ivaratorCacheScanPersistThreshold);
    }
    
    public long getIvaratorCacheScanTimeout() {
        return getConfig().getIvaratorCacheScanTimeout();
    }
    
    public void setIvaratorCacheScanTimeout(long ivaratorCacheScanTimeout) {
        getConfig().setIvaratorCacheScanTimeout(ivaratorCacheScanTimeout);
    }
    
    public void setIvaratorCacheScanTimeoutMinutes(long hdfsCacheScanTimeoutMinutes) {
        getConfig().setIvaratorCacheScanTimeout(hdfsCacheScanTimeoutMinutes * 1000 * 60);
    }
    
    public String getHdfsSiteConfigURLs() {
        return getConfig().getHdfsSiteConfigURLs();
    }
    
    public void setHdfsSiteConfigURLs(String hadoopConfigURLs) {
        getConfig().setHdfsSiteConfigURLs(hadoopConfigURLs);
    }
    
    public String getHdfsFileCompressionCodec() {
        return getConfig().getHdfsFileCompressionCodec();
    }
    
    public void setHdfsFileCompressionCodec(String hdfsFileCompressionCodec) {
        getConfig().setHdfsFileCompressionCodec(hdfsFileCompressionCodec);
    }
    
    public String getZookeeperConfig() {
        return getConfig().getZookeeperConfig();
    }
    
    public void setZookeeperConfig(String zookeeperConfig) {
        getConfig().setZookeeperConfig(zookeeperConfig);
    }
    
    public int getMaxFieldIndexRangeSplit() {
        return getConfig().getMaxFieldIndexRangeSplit();
    }
    
    public void setMaxFieldIndexRangeSplit(int maxFieldIndexRangeSplit) {
        getConfig().setMaxFieldIndexRangeSplit(maxFieldIndexRangeSplit);
    }
    
    public int getIvaratorMaxOpenFiles() {
        return getConfig().getIvaratorMaxOpenFiles();
    }
    
    public void setIvaratorMaxOpenFiles(int ivaratorMaxOpenFiles) {
        getConfig().setIvaratorMaxOpenFiles(ivaratorMaxOpenFiles);
    }
    
    public int getIvaratorNumRetries() {
        return getConfig().getIvaratorNumRetries();
    }
    
    public void setIvaratorNumRetries(int ivaratorNumRetries) {
        getConfig().setIvaratorNumRetries(ivaratorNumRetries);
    }
    
    public int getMaxIvaratorSources() {
        return getConfig().getMaxIvaratorSources();
    }
    
    public void setMaxIvaratorSources(int maxIvaratorSources) {
        getConfig().setMaxIvaratorSources(maxIvaratorSources);
    }
    
    public int getMaxEvaluationPipelines() {
        return getConfig().getMaxEvaluationPipelines();
    }
    
    public void setMaxEvaluationPipelines(int maxEvaluationPipelines) {
        getConfig().setMaxEvaluationPipelines(maxEvaluationPipelines);
    }
    
    public int getMaxPipelineCachedResults() {
        return getConfig().getMaxPipelineCachedResults();
    }
    
    public void setMaxPipelineCachedResults(int maxCachedResults) {
        getConfig().setMaxPipelineCachedResults(maxCachedResults);
    }
    
    public double getMinimumSelectivity() {
        return getConfig().getMinSelectivity();
    }
    
    public void setMinimumSelectivity(double d) {
        getConfig().setMinSelectivity(d);
    }
    
    public Map<String,QueryParser> getQuerySyntaxParsers() {
        return querySyntaxParsers;
    }
    
    public void setQuerySyntaxParsers(Map<String,QueryParser> querySyntaxParsers) {
        this.querySyntaxParsers = querySyntaxParsers;
    }
    
    public QueryParser getParser() {
        return parser;
    }
    
    public void setParser(QueryParser parser) {
        this.parser = parser;
    }
    
    public QueryPlanner getQueryPlanner() {
        if (null == planner) {
            planner = new DefaultQueryPlanner();
        }
        
        return planner;
    }
    
    public void setQueryPlanner(QueryPlanner planner) {
        this.planner = planner;
    }
    
    public Class<? extends SortedKeyValueIterator<Key,Value>> getCreateUidsIteratorClass() {
        return createUidsIteratorClass;
    }
    
    public void setCreateUidsIteratorClass(Class<? extends SortedKeyValueIterator<Key,Value>> createUidsIteratorClass) {
        this.createUidsIteratorClass = createUidsIteratorClass;
    }
    
    public UidIntersector getUidIntersector() {
        return uidIntersector;
    }
    
    public void setUidIntersector(UidIntersector uidIntersector) {
        this.uidIntersector = uidIntersector;
    }
    
    public List<String> getContentFieldNames() {
        return getConfig().getContentFieldNames();
    }
    
    public void setContentFieldNames(List<String> contentFieldNames) {
        getConfig().setContentFieldNames(contentFieldNames);
    }
    
    public CloseableIterable<QueryData> getQueries() {
        return queries;
    }
    
    public void setQueries(CloseableIterable<QueryData> queries) {
        this.queries = queries;
    }
    
    public QueryModel getQueryModel() {
        return queryModel;
    }
    
    public void setQueryModel(QueryModel queryModel) {
        log.debug("Setting a cached query model");
        this.queryModel = queryModel;
    }
    
    public ScannerFactory getScannerFactory() {
        return scannerFactory;
    }
    
    public void setScannerFactory(ScannerFactory scannerFactory) {
        log.debug("Setting scanner factory on ShardQueryLogic: " + System.identityHashCode(this) + ".setScannerFactory("
                        + System.identityHashCode(scannerFactory) + ')');
        this.scannerFactory = scannerFactory;
    }
    
    public Scheduler getScheduler() {
        return scheduler;
    }
    
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }
    
    public int getMaxScannerBatchSize() {
        return getConfig().getMaxScannerBatchSize();
    }
    
    public void setMaxScannerBatchSize(final int size) {
        getConfig().setMaxScannerBatchSize(size);
    }
    
    public int getMaxIndexBatchSize() {
        return getConfig().getMaxIndexBatchSize();
    }
    
    public void setMaxIndexBatchSize(final int size) {
        getConfig().setMaxIndexBatchSize(size);
    }
    
    public boolean getCompressServerSideResults() {
        return getConfig().isCompressServerSideResults();
    }
    
    public boolean isCompressServerSideResults() {
        return getConfig().isCompressServerSideResults();
    }
    
    public void setCompressServerSideResults(boolean compressServerSideResults) {
        getConfig().setCompressServerSideResults(compressServerSideResults);
    }
    
    /**
     * Returns a value indicating whether index-only filter functions (e.g., #INCLUDE, #EXCLUDE) should be enabled. If true, the use of such filters can
     * potentially consume a LOT of memory.
     * 
     * @return true, if index-only filter functions should be enabled.
     */
    public boolean isIndexOnlyFilterFunctionsEnabled() {
        return getConfig().isIndexOnlyFilterFunctionsEnabled();
    }
    
    /**
     * Sets a value indicating whether index-only filter functions (e.g., #INCLUDE and #EXCLUDE) should be enabled. If true, the use of such filters can
     * potentially consume a LOT of memory.
     * 
     * @param enabled
     *            indicates whether index-only filter functions (e.g., <i>filter:includeRegex()</i> and <i>not(filter:includeRegex())</i>) should be enabled
     */
    public void setIndexOnlyFilterFunctionsEnabled(boolean enabled) {
        getConfig().setIndexOnlyFilterFunctionsEnabled(enabled);
    }
    
    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> optionalParams = new TreeSet<>();
        optionalParams.add(QueryParameters.QUERY_SYNTAX);
        optionalParams.add(QueryParameters.PARAMETER_MODEL_NAME);
        optionalParams.add(QueryParameters.PARAMETER_MODEL_TABLE_NAME);
        optionalParams.add(QueryParameters.DATATYPE_FILTER_SET);
        optionalParams.add(QueryParameters.RETURN_FIELDS);
        optionalParams.add(QueryParameters.BLACKLISTED_FIELDS);
        optionalParams.add(QueryParameters.FILTER_MASKED_VALUES);
        optionalParams.add(QueryParameters.INCLUDE_DATATYPE_AS_FIELD);
        optionalParams.add(QueryParameters.INCLUDE_GROUPING_CONTEXT);
        optionalParams.add(QueryParameters.RAW_DATA_ONLY);
        optionalParams.add(QueryParameters.TRANFORM_CONTENT_TO_UID);
        optionalParams.add(QueryOptions.REDUCED_RESPONSE);
        optionalParams.add(QueryOptions.POSTPROCESSING_CLASSES);
        optionalParams.add(QueryOptions.COMPRESS_SERVER_SIDE_RESULTS);
        optionalParams.add(QueryOptions.HIT_LIST);
        optionalParams.add(QueryOptions.TYPE_METADATA_IN_HDFS);
        optionalParams.add(QueryOptions.DATE_INDEX_TIME_TRAVEL);
        optionalParams.add(QueryParameters.LIMIT_FIELDS);
        optionalParams.add(QueryParameters.GROUP_FIELDS);
        optionalParams.add(QueryParameters.UNIQUE_FIELDS);
        optionalParams.add(QueryOptions.LOG_TIMING_DETAILS);
        return optionalParams;
    }
    
    @Override
    public Set<String> getRequiredQueryParameters() {
        Set<String> requiredParams = new TreeSet<>();
        requiredParams.add(datawave.webservice.query.QueryParameters.QUERY_STRING);
        requiredParams.add(datawave.webservice.query.QueryParameters.QUERY_NAME);
        requiredParams.add(datawave.webservice.query.QueryParameters.QUERY_AUTHORIZATIONS);
        requiredParams.add(datawave.webservice.query.QueryParameters.QUERY_LOGIC_NAME);
        requiredParams.add(datawave.webservice.query.QueryParameters.QUERY_BEGIN);
        requiredParams.add(datawave.webservice.query.QueryParameters.QUERY_END);
        return requiredParams;
    }
    
    @Override
    public Set<String> getExampleQueries() {
        return Collections.emptySet();
    }
    
    public Set<String> getMandatoryQuerySyntax() {
        return mandatoryQuerySyntax;
    }
    
    public void setMandatoryQuerySyntax(Set<String> mandatoryQuerySyntax) {
        this.mandatoryQuerySyntax = mandatoryQuerySyntax;
    }
    
    public List<String> getRealmSuffixExclusionPatterns() {
        return getConfig().getRealmSuffixExclusionPatterns();
    }
    
    public void setRealmSuffixExclusionPatterns(List<String> realmSuffixExclusionPatterns) {
        getConfig().setRealmSuffixExclusionPatterns(realmSuffixExclusionPatterns);
    }
    
    /**
     * @return
     */
    public String getAccumuloPassword() {
        return getConfig().getAccumuloPassword();
    }
    
    public void setAccumuloPassword(String password) {
        getConfig().setAccumuloPassword(password);
    }
    
    public boolean isExpansionLimitedToModelContents() {
        return getConfig().isExpansionLimitedToModelContents();
    }
    
    public void setLimitTermExpansionToModel(boolean shouldLimitTermExpansionToModel) {
        getConfig().setLimitTermExpansionToModel(shouldLimitTermExpansionToModel);
    }
    
    public boolean getSequentialScheduler() {
        return getConfig().getSequentialScheduler();
    }
    
    public void setSequentialScheduler(boolean sequentialScheduler) {
        getConfig().setSequentialScheduler(sequentialScheduler);
    }
    
    public boolean getCollapseUids() {
        return getConfig().getCollapseUids();
    }
    
    public void setCollapseUids(boolean collapseUids) {
        getConfig().setCollapseUids(collapseUids);
    }
    
    public int getCollapseUidsThreshold() {
        return this.config.getCollapseUidsThreshold();
    }
    
    public void setCollapseUidsThreshold(int collapseUidsThreshold) {
        this.config.setCollapseUidsThreshold(collapseUidsThreshold);
    }
    
    public long getMaxIndexScanTimeMillis() {
        return getConfig().getMaxIndexScanTimeMillis();
    }
    
    public void setMaxIndexScanTimeMillis(long maxTime) {
        getConfig().setMaxIndexScanTimeMillis(maxTime);
    }
    
    public Function getQueryMacroFunction() {
        return queryMacroFunction;
    }
    
    public void setQueryMacroFunction(Function queryMacroFunction) {
        this.queryMacroFunction = queryMacroFunction;
    }
    
    public boolean getLimitAnyFieldLookups() {
        return getConfig().getLimitAnyFieldLookups();
    }
    
    public void setLimitAnyFieldLookups(boolean limitAnyFieldLookups) {
        getConfig().setLimitAnyFieldLookups(limitAnyFieldLookups);
    }
    
    public boolean getSpeculativeScanning() {
        return getConfig().getSpeculativeScanning();
    }
    
    public void setSpeculativeScanning(boolean speculativeScanning) {
        getConfig().setSpeculativeScanning(speculativeScanning);
    }
    
    public boolean getAllowShortcutEvaluation() {
        return getConfig().getAllowShortcutEvaluation();
    }
    
    public void setAllowShortcutEvaluation(boolean allowShortcutEvaluation) {
        getConfig().setAllowShortcutEvaluation(allowShortcutEvaluation);
    }
    
    public boolean isAllowFieldIndexEvaluation() {
        return getConfig().isAllowFieldIndexEvaluation();
    }
    
    public void setAllowFieldIndexEvaluation(boolean allowFieldIndexEvaluation) {
        getConfig().setAllowFieldIndexEvaluation(allowFieldIndexEvaluation);
    }
    
    public boolean isAllowTermFrequencyLookup() {
        return getConfig().isAllowTermFrequencyLookup();
    }
    
    public void setAllowTermFrequencyLookup(boolean allowTermFrequencyLookup) {
        getConfig().setAllowTermFrequencyLookup(allowTermFrequencyLookup);
    }
    
    public boolean getAccrueStats() {
        return getConfig().getAccrueStats();
    }
    
    public void setAccrueStats(final boolean accrueStats) {
        getConfig().setAccrueStats(accrueStats);
    }
    
    public Boolean getCollectTimingDetails() {
        return getConfig().getCollectTimingDetails();
    }
    
    public void setCollectTimingDetails(Boolean collectTimingDetails) {
        getConfig().setCollectTimingDetails(collectTimingDetails);
    }
    
    public Boolean getLogTimingDetails() {
        return getConfig().getLogTimingDetails();
    }
    
    public void setLogTimingDetails(Boolean logTimingDetails) {
        getConfig().setLogTimingDetails(logTimingDetails);
    }
    
    public String getStatsdHost() {
        return getConfig().getStatsdHost();
    }
    
    public void setStatsdHost(String statsdHost) {
        getConfig().setStatsdHost(statsdHost);
    }
    
    public int getStatsdPort() {
        return getConfig().getStatsdPort();
    }
    
    public void setStatsdPort(int statsdPort) {
        getConfig().setStatsdPort(statsdPort);
    }
    
    public int getStatsdMaxQueueSize() {
        return getConfig().getStatsdMaxQueueSize();
    }
    
    public void setStatsdMaxQueueSize(int statsdMaxQueueSize) {
        getConfig().setStatsdMaxQueueSize(statsdMaxQueueSize);
    }
    
    public boolean getSendTimingToStatsd() {
        return getConfig().getSendTimingToStatsd();
    }
    
    public void setSendTimingToStatsd(boolean sendTimingToStatsd) {
        getConfig().setSendTimingToStatsd(sendTimingToStatsd);
    }
    
    public boolean getCacheModel() {
        return getConfig().getCacheModel();
    }
    
    public void setCacheModel(boolean cacheModel) {
        getConfig().setCacheModel(cacheModel);
    }
    
    public List<IndexHole> getIndexHoles() {
        return getConfig().getIndexHoles();
    }
    
    public void setIndexHoles(List<IndexHole> indexHoles) {
        getConfig().setIndexHoles(indexHoles);
    }
    
    public CardinalityConfiguration getCardinalityConfiguration() {
        return cardinalityConfiguration;
    }
    
    public void setCardinalityConfiguration(CardinalityConfiguration cardinalityConfiguration) {
        this.cardinalityConfiguration = cardinalityConfiguration;
    }
    
    public Map<String,Profile> getConfiguredProfiles() {
        return this.configuredProfiles;
    }
    
    public void setConfiguredProfiles(Map<String,Profile> configuredProfiles) {
        this.configuredProfiles.putAll(configuredProfiles);
    }
    
    public boolean getBackoffEnabled() {
        return getConfig().getBackoffEnabled();
    }
    
    public void setBackoffEnabled(boolean backoffEnabled) {
        getConfig().setBackoffEnabled(backoffEnabled);
    }
    
    public boolean getUnsortedUIDsEnabled() {
        return getConfig().getUnsortedUIDsEnabled();
    }
    
    public void setUnsortedUIDsEnabled(boolean unsortedUIDsEnabled) {
        getConfig().setUnsortedUIDsEnabled(unsortedUIDsEnabled);
    }
    
    public boolean isDebugMultithreadedSources() {
        return getConfig().isDebugMultithreadedSources();
    }
    
    public void setDebugMultithreadedSources(boolean debugMultithreadedSources) {
        getConfig().setDebugMultithreadedSources(debugMultithreadedSources);
    }
    
    public boolean isSortGeoWaveQueryRanges() {
        return getConfig().isSortGeoWaveQueryRanges();
    }
    
    public void setSortGeoWaveQueryRanges(boolean sortGeoWaveQueryRanges) {
        getConfig().setSortGeoWaveQueryRanges(sortGeoWaveQueryRanges);
    }
    
    public int getNumRangesToBuffer() {
        return getConfig().getNumRangesToBuffer();
    }
    
    public void setNumRangesToBuffer(int numRangesToBuffer) {
        getConfig().setNumRangesToBuffer(numRangesToBuffer);
    }
    
    public long getRangeBufferTimeoutMillis() {
        return getConfig().getRangeBufferTimeoutMillis();
    }
    
    public void setRangeBufferTimeoutMillis(long rangeBufferTimeoutMillis) {
        getConfig().setRangeBufferTimeoutMillis(rangeBufferTimeoutMillis);
    }
    
    public long getRangeBufferPollMillis() {
        return getConfig().getRangeBufferPollMillis();
    }
    
    public void setRangeBufferPollMillis(long rangeBufferPollMillis) {
        getConfig().setRangeBufferPollMillis(rangeBufferPollMillis);
    }
    
    public int getGeometryMaxExpansion() {
        return getConfig().getGeometryMaxExpansion();
    }
    
    public void setGeometryMaxExpansion(int geometryMaxExpansion) {
        getConfig().setGeometryMaxExpansion(geometryMaxExpansion);
    }
    
    public int getPointMaxExpansion() {
        return getConfig().getPointMaxExpansion();
    }
    
    public void setPointMaxExpansion(int pointMaxExpansion) {
        getConfig().setPointMaxExpansion(pointMaxExpansion);
    }
    
    public int getGeoWaveMaxEnvelopes() {
        return getConfig().getGeoWaveMaxEnvelopes();
    }
    
    public void setGeoWaveMaxEnvelopes(int geoWaveMaxEnvelopes) {
        getConfig().setGeoWaveMaxEnvelopes(geoWaveMaxEnvelopes);
    }
    
    public long getBeginDateCap() {
        return getConfig().getBeginDateCap();
    }
    
    public void setBeginDateCap(long beginDateCap) {
        getConfig().setBeginDateCap(beginDateCap);
    }
    
    public boolean isFailOutsideValidDateRange() {
        return getConfig().isFailOutsideValidDateRange();
    }
    
    public void setFailOutsideValidDateRange(boolean failOutsideValidDateRange) {
        getConfig().setFailOutsideValidDateRange(failOutsideValidDateRange);
    }
    
    public Map<String,List<String>> getPrimaryToSecondaryFieldMap() {
        return primaryToSecondaryFieldMap;
    }
    
    public void setPrimaryToSecondaryFieldMap(Map<String,List<String>> primaryToSecondaryFieldMap) {
        this.primaryToSecondaryFieldMap = primaryToSecondaryFieldMap;
    }
    
    public boolean isTrackSizes() {
        return getConfig().isTrackSizes();
    }
    
    public void setTrackSizes(boolean trackSizes) {
        getConfig().setTrackSizes(trackSizes);
    }
    
    public Profile getSelectedProfile() {
        return this.selectedProfile;
    }
    
    public void setSelectedProfile(Profile profile) {
        this.selectedProfile = profile;
    }
    
    public Query getSettings() {
        return getConfig().getQuery();
    }
    
    public void setSettings(Query settings) {
        getConfig().setQuery(settings);
    }
}
