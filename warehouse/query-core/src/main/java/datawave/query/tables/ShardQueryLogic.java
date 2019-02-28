package datawave.query.tables;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
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
    protected ShardQueryConfiguration config = ShardQueryConfiguration.create();
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
        setBaseIteratorPriority(100);
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
        initialize(config, connection, settings, auths);
        return config;
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
        MetadataHelper metadataHelper = this.metadataHelperFactory.createMetadataHelper();
        // check to see if i need to initialize a new one
        if (metadataHelper.getMetadataTableName() != null && metadataTableName != null && !metadataTableName.equals(metadataHelper.getMetadataTableName())) {
            // initialize it
            metadataHelper.initialize(connection, metadataTableName, auths, rawTypes);
        } else if (metadataHelper.getAuths() == null || metadataHelper.getAuths().isEmpty()) {
            return metadataHelper.initialize(connection, metadataTableName, auths, rawTypes);
            // assumption is that it is already initialized. we shall see.....
        } else {
            if (log.isTraceEnabled())
                log.trace("the MetadataHelper did not need to be initialized:" + metadataHelper + " and " + metadataTableName + " and " + auths);
        }
        return metadataHelper.initialize(connection, metadataTableName, auths, rawTypes);
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
        transformer.setContentFieldNames(config.getContentFieldNames());
        transformer.setLogTimingDetails(this.getLogTimingDetails());
        transformer.setCardinalityConfiguration(cardinalityConfiguration);
        transformer.setPrimaryToSecondaryFieldMap(primaryToSecondaryFieldMap);
        transformer.setQm(queryModel);
        if (config != null) {
            transformer.setProjectFields(config.getProjectFields());
            transformer.setBlacklistedFields(config.getBlacklistedFields());
            if (config.getUniqueFields() != null && !config.getUniqueFields().isEmpty()) {
                transformer.addTransform(new UniqueTransform(this, config.getUniqueFields()));
            }
            if (config.getGroupFields() != null && !config.getGroupFields().isEmpty()) {
                transformer.addTransform(new GroupingTransform(this, config.getGroupFields()));
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
                this.planner.close(this.config, this.getSettings());
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
    
    public ShardQueryConfiguration getConfig() {
        return this.config;
    }
    
    public void setConfig(ShardQueryConfiguration config) {
        this.config = config;
    }
    
    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }
    
    public boolean getFilterMaskedValues() {
        return this.config.getFilterMaskedValues();
    }
    
    public void setFilterMaskedValues(boolean filterMaskedValues) {
        this.config.setFilterMaskedValues(filterMaskedValues);
    }
    
    public boolean getIncludeDataTypeAsField() {
        return this.config.getIncludeDataTypeAsField();
    }
    
    public void setIncludeDataTypeAsField(boolean includeDataTypeAsField) {
        this.config.setIncludeDataTypeAsField(includeDataTypeAsField);
    }
    
    public boolean getIncludeRecordId() {
        return this.config.getIncludeRecordId();
    }
    
    public void setIncludeRecordId(boolean includeRecordId) {
        this.config.setIncludeRecordId(includeRecordId);
    }
    
    public boolean getIncludeHierarchyFields() {
        return this.config.getIncludeHierarchyFields();
    }
    
    public void setIncludeHierarchyFields(boolean includeHierarchyFields) {
        this.config.setIncludeHierarchyFields(includeHierarchyFields);
    }
    
    public List<String> getDocumentPermutations() {
        return this.config.getDocumentPermutations();
    }
    
    public void setDocumentPermutations(List<String> documentPermutations) {
        this.config.setDocumentPermutations(documentPermutations);
    }
    
    public Map<String,String> getHierarchyFieldOptions() {
        return this.config.getHierarchyFieldOptions();
    }
    
    public void setHierarchyFieldOptions(final Map<String,String> options) {
        this.config.setHierarchyFieldOptions(options);
    }
    
    public Set<String> getBlacklistedFields() {
        return this.config.getBlacklistedFields();
    }
    
    public void setBlacklistedFields(Set<String> blacklistedFields) {
        this.config.setBlacklistedFields(blacklistedFields);
    }
    
    public Set<String> getLimitFields() {
        return this.config.getLimitFields();
    }
    
    public void setLimitFields(Set<String> limitFields) {
        this.config.setLimitFields(limitFields);
    }
    
    public boolean isLimitFieldsPreQueryEvaluation() {
        return this.config.isLimitFieldsPreQueryEvaluation();
    }
    
    public void setLimitFieldsPreQueryEvaluation(boolean limitFieldsPreQueryEvaluation) {
        this.config.setLimitFieldsPreQueryEvaluation(limitFieldsPreQueryEvaluation);
    }
    
    public String getLimitFieldsField() {
        return this.config.getLimitFieldsField();
    }
    
    public void setLimitFieldsField(String limitFieldsField) {
        this.config.setLimitFieldsField(limitFieldsField);
    }
    
    public Set<String> getGroupFields() {
        return this.config.getGroupFields();
    }
    
    public void setGroupFields(Set<String> groupFields) {
        this.config.setGroupFields(groupFields);
    }
    
    public void setGroupFieldsBatchSize(int groupFieldsBatchSize) {
        this.config.setGroupFieldsBatchSize(groupFieldsBatchSize);
    }
    
    public int getGroupFieldsBatchSize() {
        return this.config.getGroupFieldsBatchSize();
    }
    
    public Set<String> getUniqueFields() {
        return this.config.getUniqueFields();
    }
    
    public void setUniqueFields(Set<String> uniqueFields) {
        this.config.setUniqueFields(uniqueFields);
    }
    
    public String getBlacklistedFieldsString() {
        return this.config.getBlacklistedFieldsAsString();
    }
    
    public boolean getIncludeGroupingContext() {
        return this.config.getIncludeGroupingContext();
    }
    
    public void setIncludeGroupingContext(boolean opt) {
        this.config.setIncludeGroupingContext(opt);
    }
    
    public boolean isReducedResponse() {
        return this.config.isReducedResponse();
    }
    
    public void setReducedResponse(boolean reducedResponse) {
        this.config.setReducedResponse(reducedResponse);
    }
    
    public boolean isDisableEvaluation() {
        return this.config.isDisableEvaluation();
    }
    
    public void setDisableEvaluation(boolean disableEvaluation) {
        this.config.setDisableEvaluation(disableEvaluation);
    }
    
    public boolean disableIndexOnlyDocuments() {
        return this.config.isDisableIndexOnlyDocuments();
    }
    
    public void setDisableIndexOnlyDocuments(boolean disableIndexOnlyDocuments) {
        this.config.setDisableIndexOnlyDocuments(disableIndexOnlyDocuments);
    }
    
    public boolean isHitList() {
        return this.config.isHitList();
    }
    
    public void setHitList(boolean hitList) {
        this.config.setHitList(hitList);
    }
    
    public boolean isTypeMetadataInHdfs() {
        return this.config.isTypeMetadataInHdfs();
    }
    
    public void setTypeMetadataInHdfs(boolean typeMetadataInHdfs) {
        this.config.setTypeMetadataInHdfs(typeMetadataInHdfs);
    }
    
    public int getEventPerDayThreshold() {
        return this.config.getEventPerDayThreshold();
    }
    
    public void setEventPerDayThreshold(int eventPerDayThreshold) {
        this.config.setEventPerDayThreshold(eventPerDayThreshold);
    }
    
    public int getShardsPerDayThreshold() {
        return this.config.getShardsPerDayThreshold();
    }
    
    public void setShardsPerDayThreshold(int shardsPerDayThreshold) {
        this.config.setShardsPerDayThreshold(shardsPerDayThreshold);
    }
    
    public int getMaxTermThreshold() {
        return this.config.getMaxTermThreshold();
    }
    
    public void setMaxTermThreshold(int maxTermThreshold) {
        this.config.setMaxTermThreshold(maxTermThreshold);
    }
    
    public int getMaxDepthThreshold() {
        return this.config.getMaxDepthThreshold();
    }
    
    public void setMaxDepthThreshold(int maxDepthThreshold) {
        this.config.setMaxDepthThreshold(maxDepthThreshold);
    }
    
    public int getMaxUnfieldedExpansionThreshold() {
        return this.config.getMaxUnfieldedExpansionThreshold();
    }
    
    public void setMaxUnfieldedExpansionThreshold(int maxUnfieldedExpansionThreshold) {
        this.config.setMaxUnfieldedExpansionThreshold(maxUnfieldedExpansionThreshold);
    }
    
    public int getMaxValueExpansionThreshold() {
        return this.config.getMaxValueExpansionThreshold();
    }
    
    public void setMaxValueExpansionThreshold(int maxValueExpansionThreshold) {
        this.config.setMaxValueExpansionThreshold(maxValueExpansionThreshold);
    }
    
    public int getMaxOrExpansionThreshold() {
        return this.config.getMaxOrExpansionThreshold();
    }
    
    public void setMaxOrExpansionThreshold(int maxOrExpansionThreshold) {
        this.config.setMaxOrExpansionThreshold(maxOrExpansionThreshold);
    }
    
    public int getMaxOrExpansionFstThreshold() {
        return this.config.getMaxOrExpansionFstThreshold();
    }
    
    public void setMaxOrExpansionFstThreshold(int maxOrExpansionFstThreshold) {
        this.config.setMaxOrExpansionFstThreshold(maxOrExpansionFstThreshold);
    }
    
    public long getYieldThresholdMs() {
        return this.config.getYieldThresholdMs();
    }
    
    public void setYieldThresholdMs(long yieldThresholdMs) {
        this.config.setYieldThresholdMs(yieldThresholdMs);
    }
    
    public boolean isCleanupShardsAndDaysQueryHints() {
        return this.config.isCleanupShardsAndDaysQueryHints();
    }
    
    public void setCleanupShardsAndDaysQueryHints(boolean cleanupShardsAndDaysQueryHints) {
        this.config.setCleanupShardsAndDaysQueryHints(cleanupShardsAndDaysQueryHints);
    }
    
    public String getDateIndexTableName() {
        return this.config.getDateIndexTableName();
    }
    
    public void setDateIndexTableName(String dateIndexTableName) {
        this.config.setDateIndexTableName(dateIndexTableName);
    }
    
    public String getDefaultDateTypeName() {
        return this.config.getDefaultDateTypeName();
    }
    
    public void setDefaultDateTypeName(String defaultDateTypeName) {
        this.config.setDefaultDateTypeName(defaultDateTypeName);
    }
    
    public String getMetadataTableName() {
        return this.config.getMetadataTableName();
    }
    
    public void setMetadataTableName(String metadataTableName) {
        this.config.setMetadataTableName(metadataTableName);
    }
    
    public String getIndexTableName() {
        return this.config.getIndexTableName();
    }
    
    public void setIndexTableName(String indexTableName) {
        this.config.setIndexTableName(indexTableName);
    }
    
    public String getIndexStatsTableName() {
        return this.config.getIndexStatsTableName();
    }
    
    public void setIndexStatsTableName(String indexStatsTableName) {
        this.config.setIndexStatsTableName(indexStatsTableName);
    }
    
    @Override
    public String getTableName() {
        if (null == getConfig()) {
            return this.tableName;
        }
        return this.config.getTableName();
    }
    
    @Override
    public void setTableName(String tableName) {
        this.tableName = tableName;
        // Null check required due to a super constructor call that attempts to set
        // the tableName prior to ShardQueryConfig initialization
        if (null != getConfig()) {
            this.config.setTableName(tableName);
        }
    }
    
    public String getModelTableName() {
        return this.config.getModelTableName();
    }
    
    public void setModelTableName(String modelTableName) {
        this.config.setModelTableName(modelTableName);
    }
    
    public String getModelName() {
        return this.config.getModelName();
    }
    
    public void setModelName(String modelName) {
        this.config.setModelName(modelName);
    }
    
    public int getQueryThreads() {
        return this.config.getNumQueryThreads();
    }
    
    public void setQueryThreads(int queryThreads) {
        this.config.setNumQueryThreads(queryThreads);
    }
    
    public int getIndexLookupThreads() {
        return this.config.getNumIndexLookupThreads();
    }
    
    public void setIndexLookupThreads(int indexLookupThreads) {
        this.config.setNumIndexLookupThreads(indexLookupThreads);
    }
    
    public int getDateIndexThreads() {
        return this.config.getNumDateIndexThreads();
    }
    
    public void setDateIndexThreads(int indexThreads) {
        this.config.setNumDateIndexThreads(indexThreads);
    }
    
    public int getMaxDocScanTimeout() {
        return this.config.getMaxDocScanTimeout();
    }
    
    public void setMaxDocScanTimeout(int maxDocScanTimeout) {
        this.config.setMaxDocScanTimeout(maxDocScanTimeout);
    }
    
    public float getCollapseDatePercentThreshold() {
        return this.config.getCollapseDatePercentThreshold();
    }
    
    public void setCollapseDatePercentThreshold(float collapseDatePercentThreshold) {
        this.config.setCollapseDatePercentThreshold(collapseDatePercentThreshold);
    }
    
    public List<String> getEnricherClassNames() {
        return this.config.getEnricherClassNames();
    }
    
    public void setEnricherClassNames(List<String> enricherClassNames) {
        this.config.setEnricherClassNames(enricherClassNames);
    }
    
    public boolean isUseEnrichers() {
        return this.config.getUseEnrichers();
    }
    
    public void setUseEnrichers(boolean useEnrichers) {
        this.config.setUseEnrichers(useEnrichers);
    }
    
    public boolean isTldQuery() {
        return config.isTldQuery();
    }
    
    public void setIsTldQuery(boolean isTldQuery) {
        config.setTldQuery(isTldQuery);
    }
    
    public boolean isExpandAllTerms() {
        return this.config.isExpandAllTerms();
    }
    
    public void setExpandAllTerms(boolean expandAllTerms) {
        this.config.setExpandAllTerms(expandAllTerms);
    }
    
    public List<String> getFilterClassNames() {
        return this.config.getFilterClassNames();
    }
    
    public void setFilterClassNames(List<String> filterClassNames) {
        this.config.setFilterClassNames(filterClassNames);
    }
    
    public List<String> getIndexFilteringClassNames() {
        return this.config.getIndexFilteringClassNames();
    }
    
    public void setIndexFilteringClassNames(List<String> classNames) {
        this.config.setIndexFilteringClassNames(classNames);
    }
    
    public Map<String,String> getFilterOptions() {
        return this.config.getFilterOptions();
    }
    
    public void setFilterOptions(final Map<String,String> options) {
        this.config.putFilterOptions(options);
    }
    
    public boolean isUseFilters() {
        return this.config.getUseFilters();
    }
    
    public void setUseFilters(boolean useFilters) {
        this.config.setUseFilters(useFilters);
    }
    
    public String getReverseIndexTableName() {
        return this.config.getReverseIndexTableName();
    }
    
    public void setReverseIndexTableName(String reverseIndexTableName) {
        this.config.setReverseIndexTableName(reverseIndexTableName);
    }
    
    public Set<String> getUnevaluatedFields() {
        return this.config.getUnevaluatedFields();
    }
    
    public void setUnevaluatedFields(String unevaluatedFieldList) {
        this.config.setUnevaluatedFields(unevaluatedFieldList);
    }
    
    public void setUnevaluatedFields(Collection<String> unevaluatedFields) {
        this.config.setUnevaluatedFields(unevaluatedFields);
    }
    
    public Class<? extends Type<?>> getDefaultType() {
        return this.config.getDefaultType();
    }
    
    public void setDefaultType(Class<? extends Type<?>> defaultType) {
        this.config.setDefaultType(defaultType);
    }
    
    @SuppressWarnings("unchecked")
    public void setDefaultType(String className) {
        this.config.setDefaultType(className);
    }
    
    public boolean isFullTableScanEnabled() {
        return this.config.getFullTableScanEnabled();
    }
    
    public void setFullTableScanEnabled(boolean fullTableScanEnabled) {
        this.config.setFullTableScanEnabled(fullTableScanEnabled);
    }
    
    public List<String> getIvaratorCacheBaseURIsAsList() {
        return this.config.getIvaratorCacheBaseURIsAsList();
    }
    
    public String getIvaratorCacheBaseURIs() {
        return this.config.getIvaratorCacheBaseURIs();
    }
    
    public void setIvaratorCacheBaseURIs(String ivaratorCacheBaseURIs) {
        this.config.setIvaratorCacheBaseURIs(ivaratorCacheBaseURIs);
    }
    
    public String getIvaratorFstHdfsBaseURIs() {
        return this.config.getIvaratorFstHdfsBaseURIs();
    }
    
    public void setIvaratorFstHdfsBaseURIs(String ivaratorFstHdfsBaseURIs) {
        this.config.setIvaratorFstHdfsBaseURIs(ivaratorFstHdfsBaseURIs);
    }
    
    public int getIvaratorCacheBufferSize() {
        return this.config.getIvaratorCacheBufferSize();
    }
    
    public void setIvaratorCacheBufferSize(int ivaratorCacheBufferSize) {
        this.config.setIvaratorCacheBufferSize(ivaratorCacheBufferSize);
    }
    
    public long getIvaratorCacheScanPersistThreshold() {
        return this.config.getIvaratorCacheScanPersistThreshold();
    }
    
    public void setIvaratorCacheScanPersistThreshold(long ivaratorCacheScanPersistThreshold) {
        this.config.setIvaratorCacheScanPersistThreshold(ivaratorCacheScanPersistThreshold);
    }
    
    public long getIvaratorCacheScanTimeout() {
        return this.config.getIvaratorCacheScanTimeout();
    }
    
    public void setIvaratorCacheScanTimeout(long ivaratorCacheScanTimeout) {
        this.config.setIvaratorCacheScanTimeout(ivaratorCacheScanTimeout);
    }
    
    public void setIvaratorCacheScanTimeoutMinutes(long hdfsCacheScanTimeoutMinutes) {
        this.config.setIvaratorCacheScanTimeout(hdfsCacheScanTimeoutMinutes * 1000 * 60);
    }
    
    public String getHdfsSiteConfigURLs() {
        return this.config.getHdfsSiteConfigURLs();
    }
    
    public void setHdfsSiteConfigURLs(String hadoopConfigURLs) {
        this.config.setHdfsSiteConfigURLs(hadoopConfigURLs);
    }
    
    public String getHdfsFileCompressionCodec() {
        return this.config.getHdfsFileCompressionCodec();
    }
    
    public void setHdfsFileCompressionCodec(String hdfsFileCompressionCodec) {
        this.config.setHdfsFileCompressionCodec(hdfsFileCompressionCodec);
    }
    
    public String getZookeeperConfig() {
        return this.config.getZookeeperConfig();
    }
    
    public void setZookeeperConfig(String zookeeperConfig) {
        this.config.setZookeeperConfig(zookeeperConfig);
    }
    
    public int getMaxFieldIndexRangeSplit() {
        return this.config.getMaxFieldIndexRangeSplit();
    }
    
    public void setMaxFieldIndexRangeSplit(int maxFieldIndexRangeSplit) {
        this.config.setMaxFieldIndexRangeSplit(maxFieldIndexRangeSplit);
    }
    
    public int getIvaratorMaxOpenFiles() {
        return this.config.getIvaratorMaxOpenFiles();
    }
    
    public void setIvaratorMaxOpenFiles(int ivaratorMaxOpenFiles) {
        this.config.setIvaratorMaxOpenFiles(ivaratorMaxOpenFiles);
    }
    
    public int getMaxIvaratorSources() {
        return this.config.getMaxIvaratorSources();
    }
    
    public void setMaxIvaratorSources(int maxIvaratorSources) {
        this.config.setMaxIvaratorSources(maxIvaratorSources);
    }
    
    public int getMaxEvaluationPipelines() {
        return this.config.getMaxEvaluationPipelines();
    }
    
    public void setMaxEvaluationPipelines(int maxEvaluationPipelines) {
        this.config.setMaxEvaluationPipelines(maxEvaluationPipelines);
    }
    
    public int getMaxPipelineCachedResults() {
        return this.config.getMaxPipelineCachedResults();
    }
    
    public void setMaxPipelineCachedResults(int maxCachedResults) {
        this.config.setMaxPipelineCachedResults(maxCachedResults);
    }
    
    public double getMinimumSelectivity() {
        return this.config.getMinSelectivity();
    }
    
    public void setMinimumSelectivity(double d) {
        this.config.setMinSelectivity(d);
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
        return this.config.getContentFieldNames();
    }
    
    public void setContentFieldNames(List<String> contentFieldNames) {
        this.config.setContentFieldNames(contentFieldNames);
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
        return this.config.getMaxScannerBatchSize();
    }
    
    public void setMaxScannerBatchSize(final int size) {
        this.config.setMaxScannerBatchSize(size);
    }
    
    public int getMaxIndexBatchSize() {
        return this.config.getMaxIndexBatchSize();
    }
    
    public void setMaxIndexBatchSize(final int size) {
        this.config.setMaxIndexBatchSize(size);
    }
    
    public boolean getCompressServerSideResults() {
        return this.config.isCompressServerSideResults();
    }
    
    public boolean isCompressServerSideResults() {
        return this.config.isCompressServerSideResults();
    }
    
    public void setCompressServerSideResults(boolean compressServerSideResults) {
        this.config.setCompressServerSideResults(compressServerSideResults);
    }
    
    /**
     * Returns a value indicating whether index-only filter functions (e.g., #INCLUDE, #EXCLUDE) should be enabled. If true, the use of such filters can
     * potentially consume a LOT of memory.
     * 
     * @return true, if index-only filter functions should be enabled.
     */
    public boolean isIndexOnlyFilterFunctionsEnabled() {
        return this.config.isIndexOnlyFilterFunctionsEnabled();
    }
    
    /**
     * Sets a value indicating whether index-only filter functions (e.g., #INCLUDE and #EXCLUDE) should be enabled. If true, the use of such filters can
     * potentially consume a LOT of memory.
     * 
     * @param enabled
     *            indicates whether index-only filter functions (e.g., <i>filter:includeRegex()</i> and <i>not(filter:includeRegex())</i>) should be enabled
     */
    public void setIndexOnlyFilterFunctionsEnabled(boolean enabled) {
        this.config.setIndexOnlyFilterFunctionsEnabled(enabled);
    }
    
    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> params = new TreeSet<>();
        params.add(datawave.webservice.query.QueryParameters.QUERY_BEGIN);
        params.add(datawave.webservice.query.QueryParameters.QUERY_END);
        params.add(QueryParameters.QUERY_SYNTAX);
        params.add(QueryParameters.PARAMETER_MODEL_NAME);
        params.add(QueryParameters.PARAMETER_MODEL_TABLE_NAME);
        params.add(QueryParameters.DATATYPE_FILTER_SET);
        params.add(QueryParameters.RETURN_FIELDS);
        params.add(QueryParameters.BLACKLISTED_FIELDS);
        params.add(QueryParameters.FILTER_MASKED_VALUES);
        params.add(QueryParameters.INCLUDE_DATATYPE_AS_FIELD);
        params.add(QueryParameters.INCLUDE_GROUPING_CONTEXT);
        params.add(QueryParameters.RAW_DATA_ONLY);
        params.add(QueryParameters.TRANFORM_CONTENT_TO_UID);
        params.add(QueryOptions.REDUCED_RESPONSE);
        params.add(QueryOptions.POSTPROCESSING_CLASSES);
        params.add(QueryOptions.COMPRESS_SERVER_SIDE_RESULTS);
        params.add(QueryOptions.HIT_LIST);
        params.add(QueryOptions.TYPE_METADATA_IN_HDFS);
        params.add(QueryOptions.DATE_INDEX_TIME_TRAVEL);
        params.add(QueryParameters.LIMIT_FIELDS);
        params.add(QueryParameters.GROUP_FIELDS);
        params.add(QueryParameters.UNIQUE_FIELDS);
        params.add(QueryOptions.LOG_TIMING_DETAILS);
        return params;
    }
    
    @Override
    public Set<String> getRequiredQueryParameters() {
        return Collections.emptySet();
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
        return this.config.getRealmSuffixExclusionPatterns();
    }
    
    public void setRealmSuffixExclusionPatterns(List<String> realmSuffixExclusionPatterns) {
        this.config.setRealmSuffixExclusionPatterns(realmSuffixExclusionPatterns);
    }
    
    /**
     * @return
     */
    public String getAccumuloPassword() {
        return this.config.getAccumuloPassword();
    }
    
    public void setAccumuloPassword(String password) {
        this.config.setAccumuloPassword(password);
    }
    
    public boolean isExpansionLimitedToModelContents() {
        return this.config.isExpansionLimitedToModelContents();
    }
    
    public void setLimitTermExpansionToModel(boolean shouldLimitTermExpansionToModel) {
        this.config.setLimitTermExpansionToModel(shouldLimitTermExpansionToModel);
    }
    
    public boolean getSequentialScheduler() {
        return this.config.getSequentialScheduler();
    }
    
    public void setSequentialScheduler(boolean sequentialScheduler) {
        this.config.setSequentialScheduler(sequentialScheduler);
    }
    
    public boolean getCollapseUids() {
        return this.config.getCollapseUids();
    }
    
    public void setCollapseUids(boolean collapseUids) {
        this.config.setCollapseUids(collapseUids);
    }
    
    public long getMaxIndexScanTimeMillis() {
        return this.config.getMaxIndexScanTimeMillis();
    }
    
    public void setMaxIndexScanTimeMillis(long maxTime) {
        this.config.setMaxIndexScanTimeMillis(maxTime);
    }
    
    public Function getQueryMacroFunction() {
        return queryMacroFunction;
    }
    
    public void setQueryMacroFunction(Function queryMacroFunction) {
        this.queryMacroFunction = queryMacroFunction;
    }
    
    public boolean getLimitAnyFieldLookups() {
        return this.config.getLimitAnyFieldLookups();
    }
    
    public void setLimitAnyFieldLookups(boolean limitAnyFieldLookups) {
        this.config.setLimitAnyFieldLookups(limitAnyFieldLookups);
    }
    
    public boolean getSpeculativeScanning() {
        return this.config.getSpeculativeScanning();
    }
    
    public void setSpeculativeScanning(boolean speculativeScanning) {
        this.config.setSpeculativeScanning(speculativeScanning);
    }
    
    public boolean getAllowShortcutEvaluation() {
        return this.config.getAllowShortcutEvaluation();
    }
    
    public void setAllowShortcutEvaluation(boolean allowShortcutEvaluation) {
        this.config.setAllowShortcutEvaluation(allowShortcutEvaluation);
    }
    
    public boolean isAllowFieldIndexEvaluation() {
        return this.config.isAllowFieldIndexEvaluation();
    }
    
    public void setAllowFieldIndexEvaluation(boolean allowFieldIndexEvaluation) {
        this.config.setAllowFieldIndexEvaluation(allowFieldIndexEvaluation);
    }
    
    public boolean isAllowTermFrequencyLookup() {
        return this.config.isAllowTermFrequencyLookup();
    }
    
    public void setAllowTermFrequencyLookup(boolean allowTermFrequencyLookup) {
        this.config.setAllowTermFrequencyLookup(allowTermFrequencyLookup);
    }
    
    public boolean getAccrueStats() {
        return this.config.getAccrueStats();
    }
    
    public void setAccrueStats(final boolean accrueStats) {
        this.config.setAccrueStats(accrueStats);
    }
    
    public Boolean getCollectTimingDetails() {
        return this.config.getCollectTimingDetails();
    }
    
    public void setCollectTimingDetails(Boolean collectTimingDetails) {
        this.config.setCollectTimingDetails(collectTimingDetails);
    }
    
    public Boolean getLogTimingDetails() {
        return this.config.getLogTimingDetails();
    }
    
    public void setLogTimingDetails(Boolean logTimingDetails) {
        this.config.setLogTimingDetails(logTimingDetails);
    }
    
    public String getStatsdHost() {
        return this.config.getStatsdHost();
    }
    
    public void setStatsdHost(String statsdHost) {
        this.config.setStatsdHost(statsdHost);
    }
    
    public int getStatsdPort() {
        return this.config.getStatsdPort();
    }
    
    public void setStatsdPort(int statsdPort) {
        this.config.setStatsdPort(statsdPort);
    }
    
    public int getStatsdMaxQueueSize() {
        return this.config.getStatsdMaxQueueSize();
    }
    
    public void setStatsdMaxQueueSize(int statsdMaxQueueSize) {
        this.config.setStatsdMaxQueueSize(statsdMaxQueueSize);
    }
    
    public boolean getSendTimingToStatsd() {
        return this.config.getSendTimingToStatsd();
    }
    
    public void setSendTimingToStatsd(boolean sendTimingToStatsd) {
        this.config.setSendTimingToStatsd(sendTimingToStatsd);
    }
    
    public boolean getCacheModel() {
        return this.config.getCacheModel();
    }
    
    public void setCacheModel(boolean cacheModel) {
        this.config.setCacheModel(cacheModel);
    }
    
    public List<IndexHole> getIndexHoles() {
        return this.config.getIndexHoles();
    }
    
    public void setIndexHoles(List<IndexHole> indexHoles) {
        this.config.setIndexHoles(indexHoles);
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
        return this.config.getBackoffEnabled();
    }
    
    public void setBackoffEnabled(boolean backoffEnabled) {
        this.config.setBackoffEnabled(backoffEnabled);
    }
    
    public boolean getUnsortedUIDsEnabled() {
        return this.config.getUnsortedUIDsEnabled();
    }
    
    public void setUnsortedUIDsEnabled(boolean unsortedUIDsEnabled) {
        this.config.setUnsortedUIDsEnabled(unsortedUIDsEnabled);
    }
    
    public boolean isDebugMultithreadedSources() {
        return this.config.isDebugMultithreadedSources();
    }
    
    public void setDebugMultithreadedSources(boolean debugMultithreadedSources) {
        this.config.setDebugMultithreadedSources(debugMultithreadedSources);
    }
    
    public boolean isDataQueryExpressionFilterEnabled() {
        return this.config.isDataQueryExpressionFilterEnabled();
    }
    
    public void setDataQueryExpressionFilterEnabled(boolean dataQueryExpressionFilterEnabled) {
        this.config.setDataQueryExpressionFilterEnabled(dataQueryExpressionFilterEnabled);
    }
    
    public boolean isSortGeoWaveQueryRanges() {
        return this.config.isSortGeoWaveQueryRanges();
    }
    
    public void setSortGeoWaveQueryRanges(boolean sortGeoWaveQueryRanges) {
        this.config.setSortGeoWaveQueryRanges(sortGeoWaveQueryRanges);
    }
    
    public int getNumRangesToBuffer() {
        return this.config.getNumRangesToBuffer();
    }
    
    public void setNumRangesToBuffer(int numRangesToBuffer) {
        this.config.setNumRangesToBuffer(numRangesToBuffer);
    }
    
    public long getRangeBufferTimeoutMillis() {
        return this.config.getRangeBufferTimeoutMillis();
    }
    
    public void setRangeBufferTimeoutMillis(long rangeBufferTimeoutMillis) {
        this.config.setRangeBufferTimeoutMillis(rangeBufferTimeoutMillis);
    }
    
    public long getRangeBufferPollMillis() {
        return this.config.getRangeBufferPollMillis();
    }
    
    public void setRangeBufferPollMillis(long rangeBufferPollMillis) {
        this.config.setRangeBufferPollMillis(rangeBufferPollMillis);
    }
    
    public int getGeoWaveMaxExpansion() {
        return this.config.getGeoWaveMaxExpansion();
    }
    
    public void setGeoWaveMaxExpansion(int geoWaveMaxExpansion) {
        this.config.setGeoWaveMaxExpansion(geoWaveMaxExpansion);
    }
    
    public int getGeoWaveMaxEnvelopes() {
        return this.config.getGeoWaveMaxEnvelopes();
    }
    
    public void setGeoWaveMaxEnvelopes(int geoWaveMaxEnvelopes) {
        this.config.setGeoWaveMaxEnvelopes(geoWaveMaxEnvelopes);
    }
    
    public long getBeginDateCap() {
        return this.config.getBeginDateCap();
    }
    
    public void setBeginDateCap(long beginDateCap) {
        this.config.setBeginDateCap(beginDateCap);
    }
    
    public boolean isFailOutsideValidDateRange() {
        return this.config.isFailOutsideValidDateRange();
    }
    
    public void setFailOutsideValidDateRange(boolean failOutsideValidDateRange) {
        this.config.setFailOutsideValidDateRange(failOutsideValidDateRange);
    }
    
    public Map<String,List<String>> getPrimaryToSecondaryFieldMap() {
        return primaryToSecondaryFieldMap;
    }
    
    public void setPrimaryToSecondaryFieldMap(Map<String,List<String>> primaryToSecondaryFieldMap) {
        this.primaryToSecondaryFieldMap = primaryToSecondaryFieldMap;
    }
    
    public boolean isTrackSizes() {
        return this.config.isTrackSizes();
    }
    
    public void setTrackSizes(boolean trackSizes) {
        this.config.setTrackSizes(trackSizes);
    }
    
    public Profile getSelectedProfile() {
        return this.selectedProfile;
    }
    
    public void setSelectedProfile(Profile profile) {
        this.selectedProfile = profile;
    }
    
    public Query getSettings() {
        return this.config.getQuery();
    }
    
    public void setSettings(Query settings) {
        this.config.setQuery(settings);
    }
}
