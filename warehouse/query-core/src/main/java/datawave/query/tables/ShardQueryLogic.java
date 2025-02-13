package datawave.query.tables;

import static datawave.query.jexl.functions.QueryFunctions.GROUPBY_FUNCTION;
import static datawave.query.jexl.functions.QueryFunctions.UNIQUE_FUNCTION;

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

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.core.query.configuration.Result;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.CheckpointableQueryLogic;
import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.core.query.logic.WritesQueryMetrics;
import datawave.data.type.Type;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl.Parameter;
import datawave.query.CloseableIterable;
import datawave.query.Constants;
import datawave.query.DocumentSerialization;
import datawave.query.QueryParameters;
import datawave.query.attributes.ExcerptFields;
import datawave.query.attributes.SummaryOptions;
import datawave.query.attributes.UniqueFields;
import datawave.query.cardinality.CardinalityConfiguration;
import datawave.query.common.grouping.GroupFields;
import datawave.query.config.IndexValueHole;
import datawave.query.config.Profile;
import datawave.query.config.ScanHintRule;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.enrich.DataEnricher;
import datawave.query.enrich.EnrichingMaster;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.index.lookup.CreateUidsIterator;
import datawave.query.index.lookup.IndexInfo;
import datawave.query.index.lookup.UidIntersector;
import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.jexl.visitors.InvertNodeVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuilder;
import datawave.query.language.parser.ParseException;
import datawave.query.language.parser.QueryParser;
import datawave.query.language.parser.lucene.LuceneSyntaxQueryParser;
import datawave.query.language.tree.QueryNode;
import datawave.query.model.QueryModel;
import datawave.query.planner.DatePartitionedQueryPlanner;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.planner.MetadataHelperQueryModelProvider;
import datawave.query.planner.QueryModelProvider;
import datawave.query.planner.QueryOptionsSwitch;
import datawave.query.planner.QueryPlanner;
import datawave.query.rules.QueryRule;
import datawave.query.rules.QueryValidationResult;
import datawave.query.rules.ShardQueryValidationConfiguration;
import datawave.query.scheduler.PushdownScheduler;
import datawave.query.scheduler.Scheduler;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.query.transformer.DocumentTransform;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.transformer.EventQueryDataDecoratorTransformer;
import datawave.query.transformer.FieldRenameTransform;
import datawave.query.transformer.GroupingTransform;
import datawave.query.transformer.QueryValidationResultTransformer;
import datawave.query.transformer.UniqueTransform;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.query.util.QueryStopwatch;
import datawave.query.util.ShardQueryUtils;
import datawave.query.util.sortedset.FileSortedSet;
import datawave.util.time.TraceStopwatch;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.QueryValidationResponse;

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
 * <p>
 * We are not supporting all of the operators that JEXL supports at this time. We are supporting the following operators:
 *
 * <pre>
 *  ==, !=, &gt;, &ge;, &lt;, &le;, =~, !~, and the reserved word 'null'
 * </pre>
 * <p>
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
public class ShardQueryLogic extends BaseQueryLogic<Entry<Key,Value>> implements CheckpointableQueryLogic {

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
    protected Transformer<Object,QueryValidationResponse> validationResponseTransformer = null;
    // Map of syntax names to QueryParser classes
    private Map<String,QueryParser> querySyntaxParsers = new HashMap<>();
    private Set<String> mandatoryQuerySyntax = null;
    private QueryPlanner planner = null;
    private QueryParser parser = null;
    private QueryLogicTransformer transformerInstance = null;
    private CardinalityConfiguration cardinalityConfiguration = null;
    private List<QueryRule> validationRules = null;

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

        log.trace("copy CTOR setting metadataHelperFactory to " + other.getMetadataHelperFactory());
        this.setMetadataHelperFactory(other.getMetadataHelperFactory());
        this.setDateIndexHelperFactory(other.getDateIndexHelperFactory());
        this.setQueryMacroFunction(other.getQueryMacroFunction());
        this.setCardinalityConfiguration(other.getCardinalityConfiguration());
        this.setConfiguredProfiles(other.getConfiguredProfiles());
        this.setSelectedProfile(other.getSelectedProfile());
        this.setPrimaryToSecondaryFieldMap(other.getPrimaryToSecondaryFieldMap());

        if (other.eventQueryDataDecoratorTransformer != null) {
            this.setEventQueryDataDecoratorTransformer(new EventQueryDataDecoratorTransformer(other.getEventQueryDataDecoratorTransformer()));
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

        if (config.getTableConsistencyLevels().containsKey(config.getTableName())) {
            bs.setConsistencyLevel(config.getTableConsistencyLevels().get(config.getTableName()));
        }

        if (config.getTableHints().containsKey(config.getTableName())) {
            bs.setExecutionHints(config.getTableHints().get(config.getTableName()));
        }

        return bs;
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {
        // whenever we reinitialize, ensure we have a fresh transformer
        this.transformerInstance = null;

        this.config = ShardQueryConfiguration.create(this, settings);
        if (log.isTraceEnabled())
            log.trace("Initializing ShardQueryLogic: " + System.identityHashCode(this) + '('
                            + (this.getSettings() == null ? "empty" : this.getSettings().getId()) + ')');
        this.config.setExpandFields(true);
        this.config.setExpandValues(true);
        this.config.setGeneratePlanOnly(false);
        initialize(config, client, settings, auths);
        return config;
    }

    @Override
    public String getPlan(AccumuloClient client, Query settings, Set<Authorizations> auths, boolean expandFields, boolean expandValues) throws Exception {

        this.config = ShardQueryConfiguration.create(this, settings);
        if (log.isTraceEnabled())
            log.trace("Initializing ShardQueryLogic for plan: " + System.identityHashCode(this) + '('
                            + (this.getSettings() == null ? "empty" : this.getSettings().getId()) + ')');
        this.config.setExpandFields(expandFields);
        this.config.setExpandValues(expandValues);
        // if we are not generating the full plan, then set the flag such that we avoid checking for final executability/full table scan
        if (!expandFields || !expandValues) {
            this.config.setGeneratePlanOnly(true);
        }
        initialize(config, client, settings, auths);
        return this.config.getQueryString();
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
        if (originalQuery == null) {
            throw new IllegalArgumentException("Query cannot be null");
        }

        // Determine the valid query syntax to use (i.e. JEXL, LUCENE, etc.)
        String querySyntax = getValidQuerySyntax(settings);

        if (querySyntax.equals(Constants.JEXL)) {
            return originalQuery;
        } else {
            QueryParser queryParser = getQueryParser(querySyntax);
            QueryNode node = queryParser.parse(originalQuery);
            String jexlQuery = node.getOriginalQuery();
            if (log.isTraceEnabled()) {
                log.trace("luceneQueryString: " + originalQuery + " --> jexlQueryString: " + jexlQuery);
            }
            return jexlQuery;
        }
    }

    /**
     * Returns the {@link QueryParser} that should be used to parse a query of the given syntax to JEXL.
     *
     * @param syntax
     *            the syntax
     * @return the query parser
     */
    private QueryParser getQueryParser(String syntax) {
        if (this.querySyntaxParsers == null) {
            throw new IllegalStateException("Query syntax parsers not configured");
        }
        QueryParser queryParser = this.querySyntaxParsers.get(syntax);
        if (queryParser == null) {
            queryParser = getParser();
            if (queryParser == null) {
                throw new IllegalArgumentException("QueryParser not configured for syntax: " + syntax);
            }
        }
        return queryParser;
    }

    /**
     * Returns the query syntax that should be used when parsing the query for the given settings. If any mandatory query syntaxes are specified, the syntax
     * will be checked to verify if it is one of the allowed mandatory query syntaxes. After this check, if the query syntax is blank and no default parser has
     * been configured, the syntax {@value Constants#JEXL} will be returned, otherwise the original query syntax extracted from the query settings will be
     * returned.
     *
     * @param settings
     *            the query settings
     * @return the query syntax
     * @throws IllegalStateException
     *             if mandatory query syntaxes were configured and the query syntax does not match any of them
     */
    private String getValidQuerySyntax(Query settings) {
        String querySyntax = settings.findParameter(QueryParameters.QUERY_SYNTAX).getParameterValue();
        checkMandatoryQuerySyntaxes(querySyntax);
        return (StringUtils.isBlank(querySyntax) && getParser() == null) ? Constants.JEXL : querySyntax;
    }

    /**
     * Checks if the given query syntax is one of the allowed mandatory query syntaxes, if any mandatory query syntaxes were specified.
     *
     * @param querySyntax
     * @throws IllegalStateException
     *             if {@link ShardQueryLogic#mandatoryQuerySyntax} is not null and the given query syntax is either empty or not present within the set of
     *             mandatory query syntaxes
     */
    private void checkMandatoryQuerySyntaxes(String querySyntax) {
        if (null != this.mandatoryQuerySyntax) {
            if (StringUtils.isEmpty(querySyntax)) {
                throw new IllegalStateException("Must specify one of the following syntax options: " + this.mandatoryQuerySyntax);
            } else {
                if (!this.mandatoryQuerySyntax.contains(querySyntax)) {
                    throw new IllegalStateException(
                                    "Syntax not supported, must be one of the following: " + this.mandatoryQuerySyntax + ", submitted: " + querySyntax);
                }
            }
        }
    }

    private String getQuerySyntaxOrDefault(String querySyntax) {
        return StringUtils.isBlank(querySyntax) && getParser() == null ? Constants.JEXL : querySyntax;
    }

    public void initialize(ShardQueryConfiguration config, AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {
        // Set the connector and the authorizations into the config object
        config.setClient(client);
        config.setAuthorizations(auths);
        config.setMaxScannerBatchSize(getMaxScannerBatchSize());
        config.setMaxIndexBatchSize(getMaxIndexBatchSize());

        setScannerFactory(new ScannerFactory(config));

        // load params before parsing jexl string so these can be injected
        loadQueryParameters(config, settings);

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

        MetadataHelper metadataHelper = prepareMetadataHelper(client, this.getMetadataTableName(), auths, config.isRawTypes());

        DateIndexHelper dateIndexHelper = prepareDateIndexHelper(client, this.getDateIndexTableName(), auths);
        if (config.isDateIndexTimeTravel()) {
            dateIndexHelper.setTimeTravel(config.isDateIndexTimeTravel());
        }

        initializeQueryModel(config, metadataHelper, dateIndexHelper);

        if (this.queryModel == null) {
            loadQueryModel(metadataHelper, config);
        }

        getQueryPlanner().setCreateUidsIteratorClass(createUidsIteratorClass);
        getQueryPlanner().setUidIntersector(uidIntersector);

        validateConfiguration(config);

        if (getCardinalityConfiguration() != null && (!config.getDisallowlistedFields().isEmpty() || !config.getProjectFields().isEmpty())) {
            // Ensure that fields used for resultCardinalities are returned. They will be removed in the DocumentTransformer.
            // Modify the projectFields and disallowlistFields only for this stage, then return to the original values.
            // Not advisable to create a copy of the config object due to the embedded timers.
            Set<String> originalDisallowlistedFields = new HashSet<>(config.getDisallowlistedFields());
            Set<String> originalProjectFields = new HashSet<>(config.getProjectFields());

            // either projectFields or disallowlistedFields can be used, but not both
            // this will be caught when loadQueryParameters is called
            if (!config.getDisallowlistedFields().isEmpty()) {
                config.setDisallowlistedFields(getCardinalityConfiguration().getRevisedDisallowlistFields(queryModel, originalDisallowlistedFields));
            }
            if (!config.getProjectFields().isEmpty()) {
                config.setProjectFields(getCardinalityConfiguration().getRevisedProjectFields(queryModel, originalProjectFields));
            }

            setQueries(getQueryPlanner().process(config, jexlQueryString, settings, this.getScannerFactory()));

            config.setDisallowlistedFields(originalDisallowlistedFields);
            config.setProjectFields(originalProjectFields);
        } else {
            setQueries(getQueryPlanner().process(config, jexlQueryString, settings, this.getScannerFactory()));
        }

        TraceStopwatch stopwatch = config.getTimers().newStartedStopwatch("ShardQueryLogic - Get iterator of queries");

        if (this.queries != null) {
            config.setQueriesIter(this.queries.iterator());
        }

        config.setQueryString(getQueryPlanner().getPlannedScript());

        stopwatch.stop();
    }

    private QueryModel initializeQueryModel(ShardQueryConfiguration config, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper)
                    throws TableNotFoundException, ExecutionException, InstantiationException, IllegalAccessException {

        // If the current query planner is a DefaultQueryPlanner or a FederatedQueryPlanner, get the query model if possible.
        QueryPlanner queryPlanner = getQueryPlanner();
        DefaultQueryPlanner defaultQueryPlanner = null;
        if (queryPlanner instanceof DefaultQueryPlanner) {
            defaultQueryPlanner = (DefaultQueryPlanner) queryPlanner;
        } else if (queryPlanner instanceof DatePartitionedQueryPlanner) {
            defaultQueryPlanner = ((DatePartitionedQueryPlanner) queryPlanner).getQueryPlanner();
        }

        if (defaultQueryPlanner != null) {
            defaultQueryPlanner.setMetadataHelper(metadataHelper);
            defaultQueryPlanner.setDateIndexHelper(dateIndexHelper);

            QueryModelProvider queryModelProvider = defaultQueryPlanner.getQueryModelProviderFactory().createQueryModelProvider();
            if (queryModelProvider instanceof MetadataHelperQueryModelProvider) {
                ((MetadataHelperQueryModelProvider) queryModelProvider).setMetadataHelper(metadataHelper);
                ((MetadataHelperQueryModelProvider) queryModelProvider).setConfig(config);
            }

            if (null != queryModelProvider.getQueryModel()) {
                queryModel = queryModelProvider.getQueryModel();
            }
        }

        if (this.queryModel == null) {
            loadQueryModel(metadataHelper, config);
        }

        return this.queryModel;
    }

    private void setupQueryPlanner(ShardQueryConfiguration config)
                    throws TableNotFoundException, ExecutionException, InstantiationException, IllegalAccessException {
        MetadataHelper metadataHelper = prepareMetadataHelper(config.getClient(), this.getMetadataTableName(), config.getAuthorizations(), config.isRawTypes());

        DateIndexHelper dateIndexHelper = prepareDateIndexHelper(config.getClient(), this.getDateIndexTableName(), config.getAuthorizations());
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

    }

    /**
     * Validate that the configuration is in a consistent state
     *
     * @param config
     *            the config
     * @throws IllegalArgumentException
     *             when config constraints are violated
     */
    protected void validateConfiguration(ShardQueryConfiguration config) {
        // do not allow disabling track sizes unless page size is no more than 1
        if (!config.isTrackSizes() && this.getMaxPageSize() > 1) {
            throw new IllegalArgumentException("trackSizes cannot be disabled with a page size greater than 1");
        }
    }

    protected MetadataHelper prepareMetadataHelper(AccumuloClient client, String metadataTableName, Set<Authorizations> auths) {
        return prepareMetadataHelper(client, metadataTableName, auths, false);
    }

    protected MetadataHelper prepareMetadataHelper(AccumuloClient client, String metadataTableName, Set<Authorizations> auths, boolean rawTypes) {
        if (log.isTraceEnabled())
            log.trace("prepareMetadataHelper with " + client);
        MetadataHelper helper = metadataHelperFactory.createMetadataHelper(client, metadataTableName, auths, rawTypes);
        helper.setEvaluationOnlyFields(config.getEvaluationOnlyFields());
        return helper;
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

    private DateIndexHelper prepareDateIndexHelper(AccumuloClient client, String dateIndexTableName, Set<Authorizations> auths) {
        DateIndexHelper dateIndexHelper = this.dateIndexHelperFactory.createDateIndexHelper();
        return dateIndexHelper.initialize(client, dateIndexTableName, auths, getDateIndexThreads(), getCollapseDatePercentThreshold());
    }

    @Override
    public void setupQuery(GenericQueryConfiguration genericConfig) throws Exception {
        if (!ShardQueryConfiguration.class.isAssignableFrom(genericConfig.getClass())) {
            throw new QueryException("Did not receive a ShardQueryConfiguration instance!!");
        }

        config = (ShardQueryConfiguration) genericConfig;

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
        Iterator<Result> resultIterator = this.scheduler.iterator();

        if (!config.isSortedUIDs()) {
            DedupingIterator dedupIterator = new DedupingIterator(resultIterator, config.getBloom());
            config.setBloom(dedupIterator.getBloom());
            resultIterator = dedupIterator;
        }

        this.iterator = Result.keyValueIterator(resultIterator);

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
        if (this.transformerInstance != null) {
            try {
                addConfigBasedTransformers();
            } catch (QueryException e) {
                throw new DatawaveFatalQueryException("Unable to configure transformers", e);
            }
            return this.transformerInstance;
        }

        MarkingFunctions markingFunctions = this.getMarkingFunctions();
        ResponseObjectFactory responseObjectFactory = this.getResponseObjectFactory();

        boolean reducedInSettings = false;
        String reducedResponseStr = settings.findParameter(QueryOptions.REDUCED_RESPONSE).getParameterValue().trim();
        if (StringUtils.isNotBlank(reducedResponseStr)) {
            reducedInSettings = Boolean.parseBoolean(reducedResponseStr);
        }
        boolean reduced = (this.isReducedResponse() || reducedInSettings);
        DocumentTransformer transformer = createDocumentTransformer(this, settings, markingFunctions, responseObjectFactory, reduced);
        transformer.setEventQueryDataDecoratorTransformer(eventQueryDataDecoratorTransformer);
        transformer.setContentFieldNames(getConfig().getContentFieldNames());
        transformer.setLogTimingDetails(this.getLogTimingDetails());
        if (cardinalityConfiguration != null && cardinalityConfiguration.isEnabled()) {
            transformer.setCardinalityConfiguration(cardinalityConfiguration);
        }
        transformer.setPrimaryToSecondaryFieldMap(primaryToSecondaryFieldMap);
        transformer.setQm(queryModel);
        this.transformerInstance = transformer;
        try {
            addConfigBasedTransformers();
        } catch (QueryException e) {
            throw new DatawaveFatalQueryException("Unable to configure transformers", e);
        }

        return this.transformerInstance;
    }

    protected DocumentTransformer createDocumentTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory, Boolean reducedResponse) {
        return new DocumentTransformer(logic, settings, markingFunctions, responseObjectFactory, reducedResponse);
    }

    public boolean isLongRunningQuery() {
        return getConfig().getGroupFields().hasGroupByFields() || !getUniqueFields().isEmpty();
    }

    /**
     * If the configuration didn't exist, OR IT CHANGED, we need to create or update the transformers that have been added.
     */
    private void addConfigBasedTransformers() throws QueryException {
        if (getConfig() != null) {
            ((DocumentTransformer) this.transformerInstance).setProjectFields(getConfig().getProjectFields());
            ((DocumentTransformer) this.transformerInstance).setDisallowlistedFields(getConfig().getDisallowlistedFields());

            if (getConfig().getUniqueFields() != null && !getConfig().getUniqueFields().isEmpty()) {
                DocumentTransform alreadyExists = ((DocumentTransformer) this.transformerInstance).containsTransform(UniqueTransform.class);
                if (alreadyExists != null) {
                    ((UniqueTransform) alreadyExists).updateConfig(getConfig().getUniqueFields());
                } else {
                    try {
                        // @formatter:off
                        ((DocumentTransformer) this.transformerInstance).addTransform(new UniqueTransform.Builder()
                                .withUniqueFields(getConfig().getUniqueFields())
                                .withQueryExecutionForPageTimeout(this.getQueryExecutionForPageTimeout())
                                .withModel(getQueryModel())
                                .withBufferPersistThreshold(getUniqueCacheBufferSize())
                                .withIvaratorCacheDirConfigs(getIvaratorCacheDirConfigs())
                                .withHdfsSiteConfigURLs(getHdfsSiteConfigURLs())
                                .withSubDirectory(getConfig().getQuery().getId().toString())
                                .withMaxOpenFiles(getIvaratorMaxOpenFiles())
                                .withNumRetries(getIvaratorNumRetries())
                                .withPersistOptions(new FileSortedSet.PersistOptions(true, false, 0))
                                .build());
                        // @formatter:on
                    } catch (IOException ioe) {
                        throw new QueryException("Unable to create a unique transform", ioe);
                    }
                }
            }

            GroupFields groupFields = getGroupByFields();
            if (groupFields != null && groupFields.hasGroupByFields()) {
                DocumentTransform alreadyExists = ((DocumentTransformer) this.transformerInstance).containsTransform(GroupingTransform.class);
                if (alreadyExists != null) {
                    ((GroupingTransform) alreadyExists).updateConfig(groupFields);
                } else {
                    ((DocumentTransformer) this.transformerInstance)
                                    .addTransform(new GroupingTransform(groupFields, this.markingFunctions, this.getQueryExecutionForPageTimeout()));
                }
            }

            if (getConfig().getRenameFields() != null && !getConfig().getRenameFields().isEmpty()) {
                DocumentTransform alreadyExists = ((DocumentTransformer) this.transformerInstance).containsTransform(FieldRenameTransform.class);
                if (alreadyExists != null) {
                    ((FieldRenameTransform) alreadyExists).updateConfig(getConfig().getRenameFields());
                } else {
                    ((DocumentTransformer) this.transformerInstance)
                                    .addTransform(new FieldRenameTransform(getConfig().getRenameFields(), getIncludeGroupingContext(), isReducedResponse()));
                }
            }

        }
        if (getQueryModel() != null) {
            ((DocumentTransformer) this.transformerInstance).setQm(getQueryModel());
        }
    }

    public void setPageProcessingStartTime(long pageProcessingStartTime) {
        // we only care about setting the start time if we have an instance already
        if (this.transformerInstance != null) {
            transformerInstance.setQueryExecutionForPageStartTime(pageProcessingStartTime);
        }
    }

    protected void loadQueryParameters(ShardQueryConfiguration config, Query settings) throws QueryException {
        TraceStopwatch stopwatch = config.getTimers().newStartedStopwatch("ShardQueryLogic - Parse query parameters");
        boolean rawDataOnly = false;
        String rawDataOnlyStr = settings.findParameter(QueryParameters.RAW_DATA_ONLY).getParameterValue().trim();
        if (StringUtils.isNotBlank(rawDataOnlyStr)) {
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

        if (StringUtils.isNotBlank(typeList)) {
            HashSet<String> typeFilter = new HashSet<>();
            HashSet<String> excludeSet = new HashSet<>();

            for (String dataType : Arrays.asList(StringUtils.split(typeList, Constants.PARAM_VALUE_SEP))) {
                if (dataType.charAt(0) == '!') {
                    excludeSet.add(StringUtils.substring(dataType, 1));
                } else {
                    typeFilter.add(dataType);
                }
            }

            if (!excludeSet.isEmpty()) {
                if (typeFilter.isEmpty()) {
                    MetadataHelper metadataHelper = prepareMetadataHelper(config.getClient(), this.getMetadataTableName(), config.getAuthorizations(),
                                    config.isRawTypes());

                    try {
                        typeFilter.addAll(metadataHelper.getDatatypes(null));
                    } catch (TableNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }

                typeFilter.removeAll(excludeSet);
            }

            if (log.isDebugEnabled()) {
                log.debug("Type Filter: " + typeFilter);
            }

            config.setDatatypeFilter(typeFilter);
        }

        // Get the list of field rename mappings. May be null.
        String renameFields = settings.findParameter(QueryParameters.RENAME_FIELDS).getParameterValue().trim();
        if (StringUtils.isNotBlank(renameFields)) {
            Set<String> renameFieldExpressions = new HashSet<>(Arrays.asList(StringUtils.split(renameFields, Constants.PARAM_VALUE_SEP)));
            config.setRenameFields(renameFieldExpressions);

            if (log.isDebugEnabled()) {
                final int maxLen = 100;
                // Trim down the projection if it's stupid long
                renameFields = maxLen < renameFields.length() ? renameFields.substring(0, maxLen) + "[TRUNCATED]" : renameFields;
                log.debug("Rename fields: " + renameFields);
            }
        }

        // Get the list of fields to project up the stack. May be null.
        String projectFields = settings.findParameter(QueryParameters.RETURN_FIELDS).getParameterValue().trim();
        if (StringUtils.isNotBlank(projectFields)) {
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

        // if the TRANSFORM_CONTENT_TO_UID is false, then unset the list of content field names preventing the DocumentTransformer from
        // transforming them.
        String transformContentStr = settings.findParameter(QueryParameters.TRANSFORM_CONTENT_TO_UID).getParameterValue().trim();
        if (StringUtils.isNotBlank(transformContentStr)) {
            if (!Boolean.valueOf(transformContentStr)) {
                setContentFieldNames(Collections.EMPTY_LIST);
            }
        }

        // Get the list of disallowlisted fields. May be null.
        String tDisallowlistedFields = settings.findParameter(QueryParameters.DISALLOWLISTED_FIELDS).getParameterValue().trim();
        if (StringUtils.isNotBlank(tDisallowlistedFields)) {
            List<String> disallowlistedFieldsList = Arrays.asList(StringUtils.split(tDisallowlistedFields, Constants.PARAM_VALUE_SEP));

            // Only set the disallowlisted fields if we were actually given some
            if (!disallowlistedFieldsList.isEmpty()) {
                if (!config.getProjectFields().isEmpty()) {
                    throw new QueryException("Allowlist and disallowlist projection options are mutually exclusive");
                }

                config.setDisallowlistedFields(new HashSet<>(disallowlistedFieldsList));

                if (log.isDebugEnabled()) {
                    log.debug("Disallowlisted fields: " + tDisallowlistedFields);
                }
            }
        }

        // Get the LIMIT_FIELDS parameter if given
        String limitFields = settings.findParameter(QueryParameters.LIMIT_FIELDS).getParameterValue().trim();
        if (StringUtils.isNotBlank(limitFields)) {
            List<String> limitFieldsList = Arrays.asList(StringUtils.split(limitFields, Constants.PARAM_VALUE_SEP));

            // Only set the limit fields if we were actually given some
            if (!limitFieldsList.isEmpty()) {
                config.setLimitFields(new HashSet<>(limitFieldsList));
            }
        }

        // Get the MATCHING_FIELD_SETS parameter if given
        String matchingFieldSets = settings.findParameter(QueryParameters.MATCHING_FIELD_SETS).getParameterValue().trim();
        if (StringUtils.isNotBlank(matchingFieldSets)) {
            List<String> matchingFieldSetsList = Arrays.asList(StringUtils.split(matchingFieldSets, Constants.PARAM_VALUE_SEP));

            // Only set the limit fields if we were actually given some
            if (!matchingFieldSetsList.isEmpty()) {
                config.setMatchingFieldSets(new HashSet<>(matchingFieldSetsList));
            }
        }

        String limitFieldsPreQueryEvaluation = settings.findParameter(QueryOptions.LIMIT_FIELDS_PRE_QUERY_EVALUATION).getParameterValue().trim();
        if (StringUtils.isNotBlank(limitFieldsPreQueryEvaluation)) {
            Boolean limitFieldsPreQueryEvaluationValue = Boolean.parseBoolean(limitFieldsPreQueryEvaluation);
            this.setLimitFieldsPreQueryEvaluation(limitFieldsPreQueryEvaluationValue);
            config.setLimitFieldsPreQueryEvaluation(limitFieldsPreQueryEvaluationValue);
        }

        String limitFieldsField = settings.findParameter(QueryOptions.LIMIT_FIELDS_FIELD).getParameterValue().trim();
        if (StringUtils.isNotBlank(limitFieldsField)) {
            this.setLimitFieldsField(limitFieldsField);
            config.setLimitFieldsField(limitFieldsField);
        }

        // Get the GROUP_FIELDS parameter if given
        String groupFieldsParam = settings.findParameter(QueryParameters.GROUP_FIELDS).getParameterValue().trim();
        if (StringUtils.isNotBlank(groupFieldsParam)) {
            String[] groupFields = StringUtils.split(groupFieldsParam, Constants.PARAM_VALUE_SEP);

            // Only set the group fields if we were actually given some.
            if (groupFields.length > 0) {
                GroupFields groupByFields = config.getGroupFields();
                groupByFields.setGroupByFields(Sets.newHashSet(groupFields));

                // Update the sum fields if given.
                String sumFieldsParam = settings.findParameter(QueryParameters.SUM_FIELDS).getParameterValue().trim();
                if (StringUtils.isNotBlank(sumFieldsParam)) {
                    groupByFields.setSumFields(Sets.newHashSet(StringUtils.split(sumFieldsParam, Constants.PARAM_VALUE_SEP)));
                }

                // Update the count fields if given.
                String countFieldsParam = settings.findParameter(QueryParameters.COUNT_FIELDS).getParameterValue().trim();
                if (StringUtils.isNotBlank(countFieldsParam)) {
                    groupByFields.setCountFields(Sets.newHashSet(StringUtils.split(countFieldsParam, Constants.PARAM_VALUE_SEP)));
                }

                // Update the average fields if given.
                String averageFieldsParam = settings.findParameter(QueryParameters.AVERAGE_FIELDS).getParameterValue().trim();
                if (StringUtils.isNotBlank(averageFieldsParam)) {
                    groupByFields.setAverageFields(Sets.newHashSet(StringUtils.split(averageFieldsParam, Constants.PARAM_VALUE_SEP)));
                }

                // Update the min fields if given.
                String minFieldsParam = settings.findParameter(QueryParameters.MIN_FIELDS).getParameterValue().trim();
                if (StringUtils.isNotBlank(averageFieldsParam)) {
                    groupByFields.setMinFields(Sets.newHashSet(StringUtils.split(minFieldsParam, Constants.PARAM_VALUE_SEP)));
                }

                // Update the max fields if given.
                String maxFieldsParam = settings.findParameter(QueryParameters.MAX_FIELDS).getParameterValue().trim();
                if (StringUtils.isNotBlank(averageFieldsParam)) {
                    groupByFields.setMaxFields(Sets.newHashSet(StringUtils.split(maxFieldsParam, Constants.PARAM_VALUE_SEP)));
                }

                // Update the config and the projection fields.
                this.setGroupByFields(groupByFields);
                config.setGroupFields(groupByFields);
                config.setProjectFields(groupByFields.getProjectionFields());
            }
        }

        String groupFieldsBatchSizeString = settings.findParameter(QueryParameters.GROUP_FIELDS_BATCH_SIZE).getParameterValue().trim();
        if (StringUtils.isNotBlank(groupFieldsBatchSizeString)) {
            int groupFieldsBatchSize = Integer.parseInt(groupFieldsBatchSizeString);
            this.setGroupFieldsBatchSize(groupFieldsBatchSize);
            config.setGroupFieldsBatchSize(groupFieldsBatchSize);
        }

        // Get the UNIQUE_FIELDS parameter if given
        String uniqueFieldsParam = settings.findParameter(QueryParameters.UNIQUE_FIELDS).getParameterValue().trim();
        if (StringUtils.isNotBlank(uniqueFieldsParam)) {
            UniqueFields uniqueFields = UniqueFields.from(uniqueFieldsParam);
            // Only set the unique fields if we were actually given some
            if (!uniqueFields.isEmpty()) {
                // preserve the most recent flag
                uniqueFields.setMostRecent(config.getUniqueFields().isMostRecent());
                config.setUniqueFields(uniqueFields);
            }
        }

        // Get the most recent flag
        String mostRecentUnique = settings.findParameter(QueryParameters.MOST_RECENT_UNIQUE).getParameterValue().trim();
        if (StringUtils.isNotBlank(mostRecentUnique)) {
            config.getUniqueFields().setMostRecent(Boolean.valueOf(mostRecentUnique));
        }

        // Get the EXCERPT_FIELDS parameter if given
        String excerptFieldsParam = settings.findParameter(QueryParameters.EXCERPT_FIELDS).getParameterValue().trim();
        if (StringUtils.isNotBlank(excerptFieldsParam)) {
            ExcerptFields excerptFields = ExcerptFields.from(excerptFieldsParam);
            // Only set the excerpt fields if we were actually given some
            if (!excerptFieldsParam.isEmpty()) {
                this.setExcerptFields(excerptFields);
                config.setExcerptFields(excerptFields);
            }
        }

        // Get the SUMMARY parameter if given
        String summaryParam = settings.findParameter(QueryParameters.SUMMARY_OPTIONS).getParameterValue().trim();
        if (StringUtils.isNotBlank(summaryParam)) {
            SummaryOptions summaryOptions = SummaryOptions.from(summaryParam);
            this.setSummaryOptions(summaryOptions);
            config.setSummaryOptions(summaryOptions);
        }

        // Get the HIT_LIST parameter if given
        String hitListString = settings.findParameter(QueryParameters.HIT_LIST).getParameterValue().trim();
        if (StringUtils.isNotBlank(hitListString)) {
            Boolean hitListBool = Boolean.parseBoolean(hitListString);
            config.setHitList(hitListBool);
        }

        // Get the BYPASS_ACCUMULO parameter if given
        String bypassAccumuloString = settings.findParameter(BYPASS_ACCUMULO).getParameterValue().trim();
        if (StringUtils.isNotBlank(bypassAccumuloString)) {
            Boolean bypassAccumuloBool = Boolean.parseBoolean(bypassAccumuloString);
            config.setBypassAccumulo(bypassAccumuloBool);
        }

        // Get the DATE_INDEX_TIME_TRAVEL parameter if given
        String dateIndexTimeTravelString = settings.findParameter(QueryOptions.DATE_INDEX_TIME_TRAVEL).getParameterValue().trim();
        if (StringUtils.isNotBlank(dateIndexTimeTravelString)) {
            Boolean dateIndexTimeTravel = Boolean.parseBoolean(dateIndexTimeTravelString);
            config.setDateIndexTimeTravel(dateIndexTimeTravel);
        }

        // get the RAW_TYPES parameter if given
        String rawTypesString = settings.findParameter(QueryParameters.RAW_TYPES).getParameterValue().trim();
        if (StringUtils.isNotBlank(rawTypesString)) {
            Boolean rawTypesBool = Boolean.parseBoolean(rawTypesString);
            config.setRawTypes(rawTypesBool);
        }

        // Get the FILTER_MASKED_VALUES spring setting
        String filterMaskedValuesStr = settings.findParameter(QueryParameters.FILTER_MASKED_VALUES).getParameterValue().trim();
        if (StringUtils.isNotBlank(filterMaskedValuesStr)) {
            Boolean filterMaskedValuesBool = Boolean.parseBoolean(filterMaskedValuesStr);
            this.setFilterMaskedValues(filterMaskedValuesBool);
            config.setFilterMaskedValues(filterMaskedValuesBool);
        }

        // Get the INCLUDE_DATATYPE_AS_FIELD spring setting
        String includeDatatypeAsFieldStr = settings.findParameter(QueryParameters.INCLUDE_DATATYPE_AS_FIELD).getParameterValue().trim();
        if (((StringUtils.isNotBlank(includeDatatypeAsFieldStr) && Boolean.valueOf(includeDatatypeAsFieldStr)))
                        || (this.getIncludeDataTypeAsField() && !rawDataOnly)) {
            config.setIncludeDataTypeAsField(true);
        }

        // Get the INCLUDE_RECORD_ID spring setting
        String includeRecordIdStr = settings.findParameter(QueryParameters.INCLUDE_RECORD_ID).getParameterValue().trim();
        if (StringUtils.isNotBlank(includeRecordIdStr)) {
            boolean includeRecordIdBool = Boolean.parseBoolean(includeRecordIdStr) && !rawDataOnly;
            this.setIncludeRecordId(includeRecordIdBool);
            config.setIncludeRecordId(includeRecordIdBool);
        }

        // Get the INCLUDE_HIERARCHY_FIELDS spring setting
        String includeHierarchyFieldsStr = settings.findParameter(QueryParameters.INCLUDE_HIERARCHY_FIELDS).getParameterValue().trim();
        if (((StringUtils.isNotBlank(includeHierarchyFieldsStr) && Boolean.valueOf(includeHierarchyFieldsStr)))
                        || (this.getIncludeHierarchyFields() && !rawDataOnly)) {
            config.setIncludeHierarchyFields(true);

            final Map<String,String> options = this.getHierarchyFieldOptions();
            config.setHierarchyFieldOptions(options);
        }

        // Get the query profile to allow us to select the tune profile of the query
        String queryProfile = settings.findParameter(QueryParameters.QUERY_PROFILE).getParameterValue().trim();
        if ((StringUtils.isNotBlank(queryProfile))) {

            selectedProfile = configuredProfiles.get(queryProfile);

            if (null == selectedProfile) {
                throw new QueryException(QueryParameters.QUERY_PROFILE + " has been specified but " + queryProfile + " is not a selectable profile");
            }

        }

        // Get the include.grouping.context = true/false spring setting
        String includeGroupingContextStr = settings.findParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT).getParameterValue().trim();
        if (((StringUtils.isNotBlank(includeGroupingContextStr) && Boolean.valueOf(includeGroupingContextStr)))
                        || (this.getIncludeGroupingContext() && !rawDataOnly)) {
            config.setIncludeGroupingContext(true);
        }

        // Check if the default modelName and modelTableNames have been overridden by custom parameters.
        String parameterModelName = settings.findParameter(QueryParameters.PARAMETER_MODEL_NAME).getParameterValue().trim();
        if (StringUtils.isNotBlank(parameterModelName)) {
            this.setModelName(parameterModelName);
        }

        String ignoreNonExist = settings.findParameter(QueryParameters.IGNORE_NONEXISTENT_FIELDS).getParameterValue().trim();
        if (StringUtils.isNotBlank(ignoreNonExist)) {
            config.setIgnoreNonExistentFields(Boolean.valueOf(ignoreNonExist));
        }

        config.setModelName(this.getModelName());

        String parameterModelTableName = settings.findParameter(QueryParameters.PARAMETER_MODEL_TABLE_NAME).getParameterValue().trim();
        if (StringUtils.isNotBlank(parameterModelTableName)) {
            this.setModelTableName(parameterModelTableName);
        }

        if (null != config.getModelName() && null == config.getModelTableName()) {
            throw new IllegalArgumentException(QueryParameters.PARAMETER_MODEL_NAME + " has been specified but " + QueryParameters.PARAMETER_MODEL_TABLE_NAME
                            + " is missing. Both are required to use a model");
        }

        String noExpansion = settings.findParameter(QueryParameters.NO_EXPANSION_FIELDS).getParameterValue().trim();
        if (org.apache.commons.lang3.StringUtils.isNotBlank(noExpansion)) {
            Set<String> noExpansionFields = new HashSet<>(Arrays.asList(org.apache.commons.lang3.StringUtils.split(noExpansion, ',')));
            config.setNoExpansionFields(noExpansionFields);
        }

        String lenientFields = settings.findParameter(QueryParameters.LENIENT_FIELDS).getParameterValue().trim();
        if (org.apache.commons.lang3.StringUtils.isNotBlank(lenientFields)) {
            Set<String> lenientFieldSet = new HashSet<>(Arrays.asList(org.apache.commons.lang3.StringUtils.split(lenientFields, ',')));
            config.setLenientFields(lenientFieldSet);
        }

        String strictFields = settings.findParameter(QueryParameters.STRICT_FIELDS).getParameterValue().trim();
        if (org.apache.commons.lang3.StringUtils.isNotBlank(strictFields)) {
            Set<String> strictFieldSet = new HashSet<>(Arrays.asList(org.apache.commons.lang3.StringUtils.split(strictFields, ',')));
            config.setStrictFields(strictFieldSet);
        }

        configureDocumentAggregation(settings);

        config.setLimitTermExpansionToModel(this.isExpansionLimitedToModelContents());

        String reducedResponseStr = settings.findParameter(QueryOptions.REDUCED_RESPONSE).getParameterValue().trim();
        if (StringUtils.isNotBlank(reducedResponseStr)) {
            Boolean reducedResponseValue = Boolean.parseBoolean(reducedResponseStr);
            this.setReducedResponse(reducedResponseValue);
            config.setReducedResponse(reducedResponseValue);
        }

        final String postProcessingClasses = settings.findParameter(QueryOptions.POSTPROCESSING_CLASSES).getParameterValue().trim();

        final String postProcessingOptions = settings.findParameter(QueryOptions.POSTPROCESSING_OPTIONS).getParameterValue().trim();

        // build the post p
        if (StringUtils.isNotBlank(postProcessingClasses)) {

            List<String> filterClasses = config.getFilterClassNames();
            if (null == filterClasses) {
                filterClasses = new ArrayList<>();
            }

            for (String fClassName : new datawave.util.StringUtils.SplitIterable(postProcessingClasses, ',', true)) {
                filterClasses.add(fClassName);
            }
            config.setFilterClassNames(filterClasses);

            final Map<String,String> options = this.getFilterOptions();
            if (null != options) {
                config.putFilterOptions(options);
            }

            if (StringUtils.isNotBlank(postProcessingOptions)) {
                for (String filterOptionStr : new datawave.util.StringUtils.SplitIterable(postProcessingOptions, ',', true)) {
                    if (StringUtils.isNotBlank(filterOptionStr)) {
                        final String filterValueString = settings.findParameter(filterOptionStr).getParameterValue().trim();
                        if (StringUtils.isNotBlank(filterValueString)) {
                            config.putFilterOptions(filterOptionStr, filterValueString);
                        }
                    }
                }
            }
        }

        String tCompressServerSideResults = settings.findParameter(QueryOptions.COMPRESS_SERVER_SIDE_RESULTS).getParameterValue().trim();
        if (StringUtils.isNotBlank(tCompressServerSideResults)) {
            boolean compress = Boolean.parseBoolean(tCompressServerSideResults);
            config.setCompressServerSideResults(compress);
        }

        // Configure index-only filter functions to be enabled if not already set to such a state
        config.setIndexOnlyFilterFunctionsEnabled(this.isIndexOnlyFilterFunctionsEnabled());

        // Set the ReturnType for Documents coming out of the iterator stack
        config.setReturnType(DocumentSerialization.getReturnType(settings));

        // this needs to be configured first in order for FieldMappingTransform to work properly for profiles
        if (null != selectedProfile) {
            selectedProfile.configure(this);
            selectedProfile.configure(config);
            selectedProfile.configure(planner);
        }

        QueryLogicTransformer transformer = getTransformer(settings);
        if (transformer instanceof WritesQueryMetrics) {
            String logTimingDetailsStr = settings.findParameter(QueryOptions.LOG_TIMING_DETAILS).getParameterValue().trim();
            if (StringUtils.isNotBlank(logTimingDetailsStr)) {
                setLogTimingDetails(Boolean.valueOf(logTimingDetailsStr));
            }
            if (getLogTimingDetails()) {
                // we have to collect the timing details on the iterator stack in order to log them
                setCollectTimingDetails(true);
            } else {

                String collectTimingDetailsStr = settings.findParameter(QueryOptions.COLLECT_TIMING_DETAILS).getParameterValue().trim();
                if (StringUtils.isNotBlank(collectTimingDetailsStr)) {
                    setCollectTimingDetails(Boolean.valueOf(collectTimingDetailsStr));
                }
            }
        } else {
            // if the transformer can not process the timing metrics, then turn them off
            setLogTimingDetails(false);
            setCollectTimingDetails(false);
        }

        stopwatch.stop();
    }

    void configureDocumentAggregation(Query settings) {
        Parameter disabledIndexOnlyDocument = settings.findParameter(QueryOptions.DISABLE_DOCUMENTS_WITHOUT_EVENTS);
        if (null != disabledIndexOnlyDocument) {
            final String disabledIndexOnlyDocumentStr = disabledIndexOnlyDocument.getParameterValue().trim();
            if (StringUtils.isNotBlank(disabledIndexOnlyDocumentStr)) {
                Boolean disabledIndexOnlyDocuments = Boolean.parseBoolean(disabledIndexOnlyDocumentStr);
                setDisableIndexOnlyDocuments(disabledIndexOnlyDocuments);
            }
        }
    }

    /**
     * Loads a query Model
     *
     * @param helper
     *            the metadata helper
     * @param config
     *            the config
     * @throws InstantiationException
     *             for problems with instantiation
     * @throws IllegalAccessException
     *             for illegal access exceptions
     * @throws TableNotFoundException
     *             if the table is not found
     * @throws ExecutionException
     *             for execution exceptions
     */
    protected void loadQueryModel(MetadataHelper helper, ShardQueryConfiguration config)
                    throws InstantiationException, IllegalAccessException, TableNotFoundException, ExecutionException {
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
        return new PushdownScheduler(config, scannerFactory, this.metadataHelperFactory);
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
                for (ScannerBase bs : scannerFactory.currentScanners()) {
                    scannerFactory.close(bs);
                    ++nClosed;
                }
                if (log.isDebugEnabled()) {
                    log.debug("Cleaned up " + nClosed + " batch scanners associated with this query logic.");
                }

                nClosed = 0;

                for (ScannerSession bs : scannerFactory.currentSessions()) {
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
    public Object validateQuery(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {
        if (log.isTraceEnabled()) {
            log.trace("Validating query for settings: ");
            Map<String,List<String>> map = settings.toMap();
            for (Entry<String,List<String>> entry : map.entrySet()) {
                log.trace(entry.getKey() + ": " + entry.getValue());
            }
            log.trace("");
        }

        this.config = ShardQueryConfiguration.create(this, settings);
        if (log.isTraceEnabled()) {
            log.trace("Initializing ShardQueryLogic for query validation: " + System.identityHashCode(this) + '('
                            + (this.getSettings() == null ? "empty" : this.getSettings().getId()) + ')');
        }

        // Delegate to the super class if no validation rules were configured.
        if (validationRules == null || validationRules.isEmpty()) {
            log.trace("No validation rules configured");
            return super.validateQuery(client, settings, auths);
        }

        // Set the connector and authorizations for the config object.
        config.setClient(client);
        config.setAuthorizations(auths);
        config.setMaxScannerBatchSize(getMaxScannerBatchSize());
        config.setMaxIndexBatchSize(getMaxIndexBatchSize());

        // Load the query parameters.
        loadQueryParameters(config, settings);

        // Initialize the metadata helper and query model.
        MetadataHelper metadataHelper = prepareMetadataHelper(config.getClient(), this.getMetadataTableName(), config.getAuthorizations(), config.isRawTypes());
        initializeQueryModel(config, metadataHelper, null);
        config.setQueryModel(this.queryModel);

        // Set up the initial validation configuration.
        ShardQueryValidationConfiguration validationConfig = new ShardQueryValidationConfiguration();
        validationConfig.setQuerySettings(settings);
        validationConfig.setQueryConfiguration(config);
        validationConfig.setMetadataHelper(metadataHelper);
        validationConfig.setTypeMetadata(metadataHelper.getTypeMetadata());

        // Fetch the query syntax considered to be correct for the query.
        String querySyntax = getValidQuerySyntax(settings);

        // Create a copy of the rules. Rules will be removed from this list as they are executed so that we do not execute the same rule twice.
        List<QueryRule> unexecutedRules = new ArrayList<>(getValidationRules());
        QueryValidationResult result = new QueryValidationResult();

        // If the query syntax is not JEXL, i.e. LUCENE or otherwise, fetch the query parser and see if it supplies a lucene syntax parser. If so, the query
        // needs to be validated against all LUCENE-specific rules that support the given syntax.
        if (!querySyntax.equals(Constants.JEXL)) {
            // Fetch the query parser for the query syntax.
            QueryParser queryParser = getQueryParser(querySyntax);
            // If the query parser is one that parses LUCENE, parse the query to LUCENE.
            if (queryParser instanceof LuceneSyntaxQueryParser) {
                if (log.isTraceEnabled()) {
                    log.trace("Using LUCENE parser " + queryParser.getClass() + " for syntax " + querySyntax);
                }
                org.apache.lucene.queryparser.flexible.core.nodes.QueryNode luceneQuery;
                try {
                    luceneQuery = ((LuceneSyntaxQueryParser) queryParser).parseToLuceneQueryNode(settings.getQuery());
                } catch (Exception e) {
                    if (log.isTraceEnabled()) {
                        log.trace("Failed to parse query " + settings.getQuery() + " to LUCENE for syntax " + querySyntax + ": " + System.identityHashCode(this)
                                        + '(' + (this.getSettings() == null ? "empty" : this.getSettings().getId()) + ')', e);
                    }
                    QueryException exception = new QueryException("Failed to parse query as " + querySyntax, e,
                                    DatawaveErrorCode.INVALID_SYNTAX_PARSE_ERROR.getErrorCode());
                    result.setException(exception);
                    return result;
                }

                if (log.isTraceEnabled()) {
                    log.trace("LUCENE Query after parsing:");
                    List<String> lines = datawave.query.lucene.visitors.PrintingVisitor.printToList(luceneQuery);
                    for (String line : lines) {
                        log.trace(line);
                    }
                    log.trace("");
                }

                // Update the validation configuration with the parsed lucene
                validationConfig.setParsedQuery(luceneQuery);
                validationConfig.setQueryString(settings.getQuery());

                // Validate the lucene query against all rules that support the syntax.
                Iterator<QueryRule> ruleIter = unexecutedRules.iterator();
                while (ruleIter.hasNext()) {
                    QueryRule rule = ruleIter.next();
                    try {
                        // Check if the rule supports validating a query of the query's syntax.
                        if (rule.canValidate(validationConfig)) {
                            if (log.isTraceEnabled()) {
                                log.trace("Rule '" + rule.getName() + "' supports validating the query " + validationConfig.getQueryString());
                            }

                            // Validate the query against the rule's criteria.
                            result.addRuleResult(rule.validate(validationConfig));
                            // Remove the rule from the underlying list so that it is not executed again later.
                            ruleIter.remove();
                        } else {
                            if (log.isTraceEnabled()) {
                                log.trace("Rule '" + rule.getName() + "' does not support validating the query " + validationConfig.getQueryString());
                            }
                        }
                    } catch (Exception e) {
                        log.error("Error occurred when validating against rule " + rule.getName(), e);
                        QueryException exception = new QueryException("Error occurred when validating against rule " + rule.getName(), e);
                        result.setException(exception);
                        return result;
                    }
                }
            }
        }

        // Some rules expect to validate a JEXL query. Even if the query was originally provided in a syntax other than JEXL, e.g. LUCENE, we should validate
        // the JEXL version of the query against the remaining rules.

        // Parse the query to JEXL.
        String jexlQuery;
        try {
            jexlQuery = getJexlQueryString(settings);
            config.setQueryTree(JexlASTHelper.parseAndFlattenJexlQuery(jexlQuery));
        } catch (Exception e) {
            if (log.isTraceEnabled()) {
                log.trace("Failed to parse query to JEXL: " + System.identityHashCode(this) + '('
                                + (this.getSettings() == null ? "empty" : this.getSettings().getId()) + ')', e);
            }
            QueryException exception = new QueryException("Failed to parse query as JEXL", e, DatawaveErrorCode.INVALID_SYNTAX_PARSE_ERROR.getErrorCode());
            result.setException(exception);
            return result;
        }

        logQuery(config.getQueryTree(), "Query after parsing to JEXL");

        // Normalize the JEXL query on a very basic level, and apply the query model to the query.
        // Extract any query options and add them to the configuration.
        Map<String,String> optionsMap = new HashMap<>();
        if (jexlQuery.contains(QueryFunctions.QUERY_FUNCTION_NAMESPACE + ':')) {
            // only do the extra tree visit if the function is present
            config.setQueryTree(QueryOptionsFromQueryVisitor.collect(config.getQueryTree(), optionsMap));
            if (!optionsMap.isEmpty()) {
                QueryOptionsSwitch.apply(optionsMap, config);
            }
        }

        logQuery(config.getQueryTree(), "Query after applying options");

        // Ensure any nodes with the literal on the left and the identifier on the right are re-ordered.
        config.setQueryTree(InvertNodeVisitor.invertSwappedNodes(config.getQueryTree()));

        logQuery(config.getQueryTree(), "Query after inverting swapped nodes");

        // Uppercase all identifiers.
        config.setQueryTree(ShardQueryUtils.upperCaseIdentifiers(metadataHelper, config, config.getQueryTree()));

        logQuery(config.getQueryTree(), "Query after capitalizing identifiers");

        // Flatten the tree.
        config.setQueryTree(TreeFlatteningRebuilder.flatten(config.getQueryTree()));

        logQuery(config.getQueryTree(), "Query after flattening");

        // Apply the query model.
        config.setQueryTree(ShardQueryUtils.applyQueryModel(config.getQueryTree(), config, metadataHelper.getAllFields(config.getDatatypeFilter()),
                        this.queryModel));

        logQuery(config.getQueryTree(), "Query after applying query model");

        // Update the configurations with the target syntax JEXL and the jexl query string. Execute any remaining rules that expect to run against a JEXL query.
        validationConfig.setParsedQuery(config.getQueryTree());
        validationConfig.setQueryString(JexlStringBuildingVisitor.buildQuery(config.getQueryTree()));

        // Validate the JEXL query against the remaining rules that support JEXL.
        for (QueryRule rule : unexecutedRules) {
            try {
                // Check if the rule supports validating a JEXL query.
                if (rule.canValidate(validationConfig)) {
                    if (log.isTraceEnabled()) {
                        log.trace("Rule '" + rule.getName() + "' supports validating the query " + validationConfig.getQueryString());
                    }

                    // Validate the query against the rule's criteria.
                    result.addRuleResult(rule.validate(validationConfig));
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Rule '" + rule.getName() + "' does not support validating the query " + validationConfig.getQueryString());
                    }
                }
            } catch (Exception e) {
                log.error("Error occurred when validating against rule " + rule.getName(), e);
                QueryException exception = new QueryException("Error occurred when validating against rule " + rule.getName(), e);
                result.setException(exception);
                return result;
            }
        }

        return result;
    }

    private static void logQuery(final ASTJexlScript queryTree, String message) {
        if (log.isTraceEnabled()) {
            List<String> lines = PrintingVisitor.formattedQueryStringList(queryTree, -1, -1);
            log.trace(message);
            for (String line : lines) {
                log.trace(line);
            }
            log.trace("");
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

    public boolean getEnforceUniqueConjunctionsWithinExpression() {
        return getConfig().getEnforceUniqueConjunctionsWithinExpression();
    }

    public void setEnforceUniqueConjunctionsWithinExpression(boolean enforceUniqueConjunctionsWithinExpression) {
        getConfig().setEnforceUniqueConjunctionsWithinExpression(enforceUniqueConjunctionsWithinExpression);
    }

    public boolean getEnforceUniqueDisjunctionsWithinExpression() {
        return getConfig().getEnforceUniqueDisjunctionsWithinExpression();
    }

    public void setEnforceUniqueDisjunctionsWithinExpression(boolean enforceUniqueConjunctionsWithinExpression) {
        getConfig().setEnforceUniqueDisjunctionsWithinExpression(enforceUniqueConjunctionsWithinExpression);
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

    public Set<String> getDisallowlistedFields() {
        return getConfig().getDisallowlistedFields();
    }

    public void setdisallowlistedFields(Set<String> disallowlistedFields) {
        getConfig().setDisallowlistedFields(disallowlistedFields);
    }

    public Set<String> getLimitFields() {
        return getConfig().getLimitFields();
    }

    public void setLimitFields(Set<String> limitFields) {
        getConfig().setLimitFields(limitFields);
    }

    public Set<String> getMatchingFieldSets() {
        return getConfig().getMatchingFieldSets();
    }

    public void setMatchingFieldSets(Set<String> matchingFieldSets) {
        getConfig().setMatchingFieldSets(matchingFieldSets);
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

    public GroupFields getGroupByFields() {
        return getConfig().getGroupFields();
    }

    public void setGroupByFields(GroupFields groupFields) {
        getConfig().setGroupFields(groupFields);
    }

    public void setGroupFieldsBatchSize(int groupFieldsBatchSize) {
        getConfig().setGroupFieldsBatchSize(groupFieldsBatchSize);
    }

    public int getGroupFieldsBatchSize() {
        return getConfig().getGroupFieldsBatchSize();
    }

    public UniqueFields getUniqueFields() {
        return getConfig().getUniqueFields();
    }

    public void setUniqueFields(UniqueFields uniqueFields) {
        getConfig().setUniqueFields(uniqueFields);
    }

    public Set<String> getNoExpansionFields() {
        return getConfig().getNoExpansionFields();
    }

    public void setNoExpansionFields(Set<String> noExpansionFields) {
        getConfig().setNoExpansionFields(noExpansionFields);
    }

    public Set<String> getLenientFields() {
        return getConfig().getLenientFields();
    }

    public void setLenientFields(Set<String> lenientFields) {
        getConfig().setLenientFields(lenientFields);
    }

    public Set<String> getStrictFields() {
        return getConfig().getStrictFields();
    }

    public void setStrictFields(Set<String> strictFields) {
        getConfig().setStrictFields(strictFields);
    }

    public ExcerptFields getExcerptFields() {
        return getConfig().getExcerptFields();
    }

    public void setExcerptFields(ExcerptFields excerptFields) {
        getConfig().setExcerptFields(excerptFields);
    }

    public String getExcerptIterator() {
        return getConfig().getExcerptIterator().getName();
    }

    public void setExcerptIterator(String iteratorClass) {
        try {
            getConfig().setExcerptIterator((Class<? extends SortedKeyValueIterator<Key,Value>>) Class.forName(iteratorClass));
        } catch (Exception e) {
            throw new DatawaveFatalQueryException("Illegal term frequency excerpt iterator class", e);
        }
    }

    public SummaryOptions getSummaryOptions() {
        return getConfig().getSummaryOptions();
    }

    public void setSummaryOptions(SummaryOptions summaryOptions) {
        getConfig().setSummaryOptions(summaryOptions);
    }

    public String getSummaryIterator() {
        return getConfig().getSummaryIterator().getName();
    }

    public void setSummaryIterator(String iteratorClass) {
        try {
            getConfig().setSummaryIterator((Class<? extends SortedKeyValueIterator<Key,Value>>) Class.forName(iteratorClass));
        } catch (Exception e) {
            throw new DatawaveFatalQueryException("Illegal content summary iterator class", e);
        }
    }

    public int getFiFieldSeek() {
        return getConfig().getFiFieldSeek();
    }

    public void setFiFieldSeek(int fiFieldSeek) {
        getConfig().setFiFieldSeek(fiFieldSeek);
    }

    public int getFiNextSeek() {
        return getConfig().getFiNextSeek();
    }

    public void setFiNextSeek(int fiNextSeek) {
        getConfig().setFiNextSeek(fiNextSeek);
    }

    public int getEventFieldSeek() {
        return getConfig().getEventFieldSeek();
    }

    public void setEventFieldSeek(int eventFieldSeek) {
        getConfig().setEventFieldSeek(eventFieldSeek);
    }

    public int getEventNextSeek() {
        return getConfig().getEventNextSeek();
    }

    public void setEventNextSeek(int eventNextSeek) {
        getConfig().setEventNextSeek(eventNextSeek);
    }

    public int getTfFieldSeek() {
        return getConfig().getTfFieldSeek();
    }

    public void setTfFieldSeek(int tfFieldSeek) {
        getConfig().setTfFieldSeek(tfFieldSeek);
    }

    public int getTfNextSeek() {
        return getConfig().getTfNextSeek();
    }

    public void setTfNextSeek(int tfNextSeek) {
        getConfig().setTfNextSeek(tfNextSeek);
    }

    public boolean isSeekingEventAggregation() {
        return getConfig().isSeekingEventAggregation();
    }

    public void setSeekingEventAggregation(boolean seekingEventAggregation) {
        getConfig().setSeekingEventAggregation(seekingEventAggregation);
    }

    public String getdisallowlistedFieldsString() {
        return getConfig().getDisallowlistedFieldsAsString();
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

    @Deprecated(since = "7.1.0", forRemoval = true)
    public int getEventPerDayThreshold() {
        return getConfig().getEventPerDayThreshold();
    }

    @Deprecated(since = "7.1.0", forRemoval = true)
    public void setEventPerDayThreshold(int eventPerDayThreshold) {
        getConfig().setEventPerDayThreshold(eventPerDayThreshold);
    }

    @Deprecated(since = "7.1.0", forRemoval = true)
    public int getShardsPerDayThreshold() {
        return getConfig().getShardsPerDayThreshold();
    }

    @Deprecated(since = "7.1.0", forRemoval = true)
    public void setShardsPerDayThreshold(int shardsPerDayThreshold) {
        getConfig().setShardsPerDayThreshold(shardsPerDayThreshold);
    }

    public int getInitialMaxTermThreshold() {
        return getConfig().getInitialMaxTermThreshold();
    }

    public void setInitialMaxTermThreshold(int initialMaxTermThreshold) {
        getConfig().setInitialMaxTermThreshold(initialMaxTermThreshold);
    }

    public int getIntermediateMaxTermThreshold() {
        return getConfig().getIntermediateMaxTermThreshold();
    }

    public void setIntermediateMaxTermThreshold(int intermediateMaxTermThreshold) {
        getConfig().setIntermediateMaxTermThreshold(intermediateMaxTermThreshold);
    }

    public int getIndexedMaxTermThreshold() {
        return getConfig().getIndexedMaxTermThreshold();
    }

    public void setIndexedMaxTermThreshold(int indexedMaxTermThreshold) {
        getConfig().setIndexedMaxTermThreshold(indexedMaxTermThreshold);
    }

    public int getFinalMaxTermThreshold() {
        return getConfig().getFinalMaxTermThreshold();
    }

    public void setFinalMaxTermThreshold(int finalMaxTermThreshold) {
        getConfig().setFinalMaxTermThreshold(finalMaxTermThreshold);
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
        getConfig().setMaxOrRangeThreshold(maxOrRangeThreshold);
    }

    public int getMaxOrRangeThreshold() {
        return getConfig().getMaxOrRangeThreshold();
    }

    public void setMaxOrExpansionFstThreshold(int maxOrExpansionFstThreshold) {
        getConfig().setMaxOrExpansionFstThreshold(maxOrExpansionFstThreshold);
    }

    public int getMaxRangesPerRangeIvarator() {
        return getConfig().getMaxRangesPerRangeIvarator();
    }

    public void setMaxRangesPerRangeIvarator(int maxRangesPerRangeIvarator) {
        getConfig().setMaxRangesPerRangeIvarator(maxRangesPerRangeIvarator);
    }

    public int getMaxOrRangeIvarators() {
        return getConfig().getMaxOrRangeIvarators();
    }

    public void setMaxOrRangeIvarators(int maxOrRangeIvarators) {
        getConfig().setMaxOrRangeIvarators(maxOrRangeIvarators);
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

    public int getUniqueCacheBufferSize() {
        return getConfig().getUniqueCacheBufferSize();
    }

    public void setUniqueCacheBufferSize(int uniqueCacheBufferSize) {
        getConfig().setUniqueCacheBufferSize(uniqueCacheBufferSize);
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

    public boolean isIvaratorPersistVerify() {
        return getConfig().isIvaratorPersistVerify();
    }

    public void setIvaratorPersistVerify(boolean ivaratorPersistVerify) {
        getConfig().setIvaratorPersistVerify(ivaratorPersistVerify);
    }

    public int getIvaratorPersistVerifyCount() {
        return getConfig().getIvaratorPersistVerifyCount();
    }

    public void setIvaratorPersistVerifyCount(int ivaratorPersistVerifyCount) {
        getConfig().setIvaratorPersistVerifyCount(ivaratorPersistVerifyCount);
    }

    public int getMaxIvaratorSources() {
        return getConfig().getMaxIvaratorSources();
    }

    public void setMaxIvaratorSources(int maxIvaratorSources) {
        getConfig().setMaxIvaratorSources(maxIvaratorSources);
    }

    public long getMaxIvaratorSourceWait() {
        return getConfig().getMaxIvaratorSourceWait();
    }

    public void setMaxIvaratorSourceWait(long maxIvaratorSourceWait) {
        getConfig().setMaxIvaratorSourceWait(maxIvaratorSourceWait);
    }

    public long getMaxIvaratorResults() {
        return getConfig().getMaxIvaratorResults();
    }

    public void setMaxIvaratorResults(long maxIvaratorResults) {
        getConfig().setMaxIvaratorResults(maxIvaratorResults);
    }

    public int getMaxIvaratorTerms() {
        return getConfig().getMaxIvaratorTerms();
    }

    public void setMaxIvaratorTerms(int maxIvaratorTerms) {
        getConfig().setMaxIvaratorTerms(maxIvaratorTerms);
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

    public void setQueryExecutionForPageTimeout(long queryExecutionForPageTimeout) {
        getConfig().setQueryExecutionForPageTimeout(queryExecutionForPageTimeout);
    }

    public long getQueryExecutionForPageTimeout() {
        return getConfig().getQueryExecutionForPageTimeout();
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
            planner = new DatePartitionedQueryPlanner();
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
        optionalParams.add(QueryParameters.RENAME_FIELDS);
        optionalParams.add(QueryParameters.RETURN_FIELDS);
        optionalParams.add(QueryParameters.DISALLOWLISTED_FIELDS);
        optionalParams.add(QueryParameters.FILTER_MASKED_VALUES);
        optionalParams.add(QueryParameters.INCLUDE_DATATYPE_AS_FIELD);
        optionalParams.add(QueryParameters.INCLUDE_GROUPING_CONTEXT);
        optionalParams.add(QueryParameters.RAW_DATA_ONLY);
        optionalParams.add(QueryParameters.TRANSFORM_CONTENT_TO_UID);
        optionalParams.add(QueryOptions.REDUCED_RESPONSE);
        optionalParams.add(QueryOptions.POSTPROCESSING_CLASSES);
        optionalParams.add(QueryOptions.COMPRESS_SERVER_SIDE_RESULTS);
        optionalParams.add(QueryOptions.HIT_LIST);
        optionalParams.add(QueryOptions.DATE_INDEX_TIME_TRAVEL);
        optionalParams.add(QueryParameters.LIMIT_FIELDS);
        optionalParams.add(QueryParameters.MATCHING_FIELD_SETS);
        optionalParams.add(QueryParameters.GROUP_FIELDS);
        optionalParams.add(QueryParameters.UNIQUE_FIELDS);
        optionalParams.add(QueryOptions.LOG_TIMING_DETAILS);
        optionalParams.add(datawave.microservice.query.QueryParameters.QUERY_PAGESIZE);
        optionalParams.add(datawave.microservice.query.QueryParameters.QUERY_PAGETIMEOUT);
        optionalParams.add(datawave.microservice.query.QueryParameters.QUERY_EXPIRATION);
        optionalParams.add(datawave.microservice.query.QueryParameters.QUERY_MAX_RESULTS_OVERRIDE);
        optionalParams.add(QueryParameters.SUM_FIELDS);
        optionalParams.add(QueryParameters.MAX_FIELDS);
        optionalParams.add(QueryParameters.MIN_FIELDS);
        optionalParams.add(QueryParameters.COUNT_FIELDS);
        optionalParams.add(QueryParameters.AVERAGE_FIELDS);
        return optionalParams;
    }

    @Override
    public Set<String> getRequiredQueryParameters() {
        Set<String> requiredParams = new TreeSet<>();
        requiredParams.add(datawave.microservice.query.QueryParameters.QUERY_STRING);
        requiredParams.add(datawave.microservice.query.QueryParameters.QUERY_NAME);
        requiredParams.add(datawave.microservice.query.QueryParameters.QUERY_AUTHORIZATIONS);
        requiredParams.add(datawave.microservice.query.QueryParameters.QUERY_LOGIC_NAME);
        requiredParams.add(datawave.microservice.query.QueryParameters.QUERY_BEGIN);
        requiredParams.add(datawave.microservice.query.QueryParameters.QUERY_END);
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

    public String getAccumuloPassword() {
        return getConfig().getAccumuloPassword();
    }

    public void setAccumuloPassword(String password) {
        getConfig().setAccumuloPassword(password);
    }

    public boolean isExpansionLimitedToModelContents() {
        return getConfig().isLimitTermExpansionToModel();
    }

    public void setLimitTermExpansionToModel(boolean shouldLimitTermExpansionToModel) {
        getConfig().setLimitTermExpansionToModel(shouldLimitTermExpansionToModel);
    }

    public boolean getParseTldUids() {
        return getConfig().getParseTldUids();
    }

    public void setParseTldUids(boolean parseRootUids) {
        getConfig().setParseTldUids(parseRootUids);
    }

    public boolean getCollapseUids() {
        return getConfig().getCollapseUids();
    }

    public void setCollapseUids(boolean collapseUids) {
        getConfig().setCollapseUids(collapseUids);
    }

    public int getCollapseUidsThreshold() {
        return getConfig().getCollapseUidsThreshold();
    }

    public void setCollapseUidsThreshold(int collapseUidsThreshold) {
        getConfig().setCollapseUidsThreshold(collapseUidsThreshold);
    }

    public boolean getEnforceUniqueTermsWithinExpressions() {
        return this.config.getEnforceUniqueTermsWithinExpressions();
    }

    public void setEnforceUniqueTermsWithinExpressions(boolean enforceUniqueTermsWithinExpressions) {
        this.getConfig().setEnforceUniqueTermsWithinExpressions(enforceUniqueTermsWithinExpressions);
    }

    public boolean getReduceIngestTypes() {
        return getConfig().getReduceIngestTypes();
    }

    public void setReduceIngestTypes(boolean reduceIngestTypes) {
        getConfig().setReduceIngestTypes(reduceIngestTypes);
    }

    public boolean getReduceIngestTypesPerShard() {
        return getConfig().getReduceIngestTypesPerShard();
    }

    public void setReduceIngestTypesPerShard(boolean reduceIngestTypesPerShard) {
        getConfig().setReduceIngestTypesPerShard(reduceIngestTypesPerShard);
    }

    public boolean getPruneQueryByIngestTypes() {
        return getConfig().getPruneQueryByIngestTypes();
    }

    public void setPruneQueryByIngestTypes(boolean pruneQueryByIngestTypes) {
        getConfig().setPruneQueryByIngestTypes(pruneQueryByIngestTypes);
    }

    public boolean getReduceQueryFields() {
        return this.getConfig().getReduceQueryFields();
    }

    public void setReduceQueryFields(boolean reduceQueryFields) {
        this.getConfig().setReduceQueryFields(reduceQueryFields);
    }

    public boolean getReduceQueryFieldsPerShard() {
        return this.getConfig().getReduceQueryFieldsPerShard();
    }

    public void setReduceQueryFieldsPerShard(boolean reduceQueryFieldsPerShard) {
        this.getConfig().setReduceQueryFieldsPerShard(reduceQueryFieldsPerShard);
    }

    public boolean getReduceTypeMetadata() {
        return getConfig().getReduceTypeMetadata();
    }

    public void setReduceTypeMetadata(boolean reduceTypeMetadata) {
        getConfig().setReduceTypeMetadata(reduceTypeMetadata);
    }

    public boolean getReduceTypeMetadataPerShard() {
        return getConfig().getReduceTypeMetadataPerShard();
    }

    public void setReduceTypeMetadataPerShard(boolean reduceTypeMetadataPerShard) {
        getConfig().setReduceTypeMetadataPerShard(reduceTypeMetadataPerShard);
    }

    public long getMaxIndexScanTimeMillis() {
        return getConfig().getMaxIndexScanTimeMillis();
    }

    public void setMaxIndexScanTimeMillis(long maxTime) {
        getConfig().setMaxIndexScanTimeMillis(maxTime);
    }

    public long getMaxAnyFieldScanTimeMillis() {
        return getConfig().getMaxAnyFieldScanTimeMillis();
    }

    public void setMaxAnyFieldScanTimeMillis(long maxAnyFieldScanTimeMillis) {
        getConfig().setMaxAnyFieldScanTimeMillis(maxAnyFieldScanTimeMillis);
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

    public boolean isExpandUnfieldedNegations() {
        return getConfig().isExpandUnfieldedNegations();
    }

    public void setExpandUnfieldedNegations(boolean expandUnfieldedNegations) {
        getConfig().setExpandUnfieldedNegations(expandUnfieldedNegations);
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

    public List<IndexValueHole> getIndexValueHoles() {
        return getConfig().getIndexValueHoles();
    }

    public void setIndexValueHoles(List<IndexValueHole> indexValueHoles) {
        getConfig().setIndexValueHoles(indexValueHoles);
    }

    @Deprecated
    public void setIndexHoles(List<IndexValueHole> indexHoles) {
        setIndexValueHoles(indexHoles);
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

    public int getGeoMaxExpansion() {
        return getConfig().getGeoMaxExpansion();
    }

    public void setGeoMaxExpansion(int geoMaxExpansion) {
        getConfig().setGeoMaxExpansion(geoMaxExpansion);
    }

    public int getGeoWaveRangeSplitThreshold() {
        return getConfig().getGeoWaveRangeSplitThreshold();
    }

    public void setGeoWaveRangeSplitThreshold(int geoWaveRangeSplitThreshold) {
        getConfig().setGeoWaveRangeSplitThreshold(geoWaveRangeSplitThreshold);
    }

    public double getGeoWaveMaxRangeOverlap() {
        return getConfig().getGeoWaveMaxRangeOverlap();
    }

    public void setGeoWaveMaxRangeOverlap(double geoWaveMaxRangeOverlap) {
        getConfig().setGeoWaveMaxRangeOverlap(geoWaveMaxRangeOverlap);
    }

    public boolean isOptimizeGeoWaveRanges() {
        return getConfig().isOptimizeGeoWaveRanges();
    }

    public void setOptimizeGeoWaveRanges(boolean optimizeGeoWaveRanges) {
        getConfig().setOptimizeGeoWaveRanges(optimizeGeoWaveRanges);
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

    public void setEvaluationOnlyFields(String evaluationOnlyFields) {
        getConfig().setEvaluationOnlyFields(Sets.newHashSet(evaluationOnlyFields.split(",")));
    }

    public Set<String> getEvaluationOnlyFields() {
        return getConfig().getEvaluationOnlyFields();
    }

    /**
     * Implementations use the configuration to setup execution of a portion of their query. getTransformIterator should be used to get the partial results if
     * any.
     *
     * @param client
     *            The accumulo client
     * @param baseConfig
     *            The shard query configuration
     * @param checkpoint
     */
    @Override
    public void setupQuery(AccumuloClient client, GenericQueryConfiguration baseConfig, QueryCheckpoint checkpoint) throws Exception {
        ShardQueryConfiguration config = (ShardQueryConfiguration) baseConfig;
        config.setQueries(checkpoint.getQueries());
        config.setClient(client);
        setScannerFactory(new ScannerFactory(config));

        setupQuery(config);
    }

    @Override
    public boolean isCheckpointable() {
        boolean checkpointable = getConfig().isCheckpointable();

        // NOTE: For now, don't allow unique or groupby queries to be checkpointable
        if (checkpointable && getSettings() != null && getSettings().getQuery() != null) {
            String query = getSettings().getQuery().toLowerCase();
            if (query.contains(GROUPBY_FUNCTION) || query.contains(UNIQUE_FUNCTION)) {
                checkpointable = false;
                log.warn("Disabling checkpointing for groupby/unique query: " + getSettings().getId().toString());
            }
        }

        return checkpointable;
    }

    @Override
    public void setCheckpointable(boolean checkpointable) {
        getConfig().setCheckpointable(checkpointable);
    }

    /**
     * This can be called at any point to get a checkpoint such that this query logic instance can be torn down to be rebuilt later. At a minimum this should be
     * called after the getTransformIterator is depleted of results.
     *
     * @param queryKey
     *            The query key to include in the checkpoint
     * @return The query checkpoint
     */
    @Override
    public List<QueryCheckpoint> checkpoint(QueryKey queryKey) {
        if (!isCheckpointable()) {
            throw new UnsupportedOperationException("Cannot checkpoint a query that is not checkpointable.  Try calling setCheckpointable(true) first.");
        }

        // if we have started returning results, then capture the state of the query scheduler
        if (this.scheduler != null) {
            return this.scheduler.checkpoint(queryKey);
        }
        // otherwise we create a checkpoint per query data
        else {
            Iterator<QueryData> queries = getConfig().getQueriesIter();
            List<QueryCheckpoint> checkpoints = new ArrayList<>();
            while (queries.hasNext()) {
                checkpoints.add(new QueryCheckpoint(queryKey, Collections.singletonList(queries.next())));
            }
            return checkpoints;
        }
    }

    @Override
    public QueryCheckpoint updateCheckpoint(QueryCheckpoint checkpoint) {
        // for the shard query logic, the query data objects automatically get update with
        // the last result returned, so the checkpoint should already be updated!
        return checkpoint;
    }

    public Set<String> getDisallowedRegexPatterns() {
        return getConfig().getDisallowedRegexPatterns();
    }

    public void setDisallowedRegexPatterns(Set<String> disallowedRegexPatterns) {
        getConfig().setDisallowedRegexPatterns(disallowedRegexPatterns);
    }

    public boolean isDisableWhindexFieldMappings() {
        return getConfig().isDisableWhindexFieldMappings();
    }

    public void setDisableWhindexFieldMappings(boolean disableWhindexFieldMappings) {
        getConfig().setDisableWhindexFieldMappings(disableWhindexFieldMappings);
    }

    public Set<String> getWhindexMappingFields() {
        return getConfig().getWhindexMappingFields();
    }

    public void setWhindexMappingFields(Set<String> whindexMappingFields) {
        getConfig().setWhindexMappingFields(whindexMappingFields);
    }

    public Map<String,Map<String,String>> getWhindexFieldMappings() {
        return getConfig().getWhindexFieldMappings();
    }

    public void setWhindexFieldMappings(Map<String,Map<String,String>> whindexFieldMappings) {
        getConfig().setWhindexFieldMappings(whindexFieldMappings);
    }

    public boolean isLazySetMechanismEnabled() {
        return getConfig().isLazySetMechanismEnabled();
    }

    public void setLazySetMechanismEnabled(boolean lazySetMechanismEnabled) {
        getConfig().setLazySetMechanismEnabled(lazySetMechanismEnabled);
    }

    public long getVisitorFunctionMaxWeight() {
        return getConfig().getVisitorFunctionMaxWeight();
    }

    public void setVisitorFunctionMaxWeight(long visitorFunctionMaxWeight) {
        getConfig().setVisitorFunctionMaxWeight(visitorFunctionMaxWeight);
    }

    public int getDocAggregationThresholdMs() {
        return getConfig().getDocAggregationThresholdMs();
    }

    public void setDocAggregationThresholdMs(int docAggregationThresholdMs) {
        getConfig().setDocAggregationThresholdMs(docAggregationThresholdMs);
    }

    public int getTfAggregationThresholdMs() {
        return getConfig().getTfAggregationThresholdMs();
    }

    public void setTfAggregationThresholdMs(int tfAggregationThresholdMs) {
        getConfig().setTfAggregationThresholdMs(tfAggregationThresholdMs);
    }

    public boolean getPruneQueryOptions() {
        return getConfig().getPruneQueryOptions();
    }

    public void setPruneQueryOptions(boolean pruneQueryOptions) {
        getConfig().setPruneQueryOptions(pruneQueryOptions);
    }

    public boolean isRebuildDatatypeFilter() {
        return getConfig().isRebuildDatatypeFilter();
    }

    public void setRebuildDatatypeFilter(boolean rebuildDatatypeFilter) {
        getConfig().setRebuildDatatypeFilter(rebuildDatatypeFilter);
    }

    public boolean isRebuildDatatypeFilterPerShard() {
        return getConfig().isRebuildDatatypeFilterPerShard();
    }

    public void setRebuildDatatypeFilterPerShard(boolean rebuildDatatypeFilterPerShard) {
        getConfig().setRebuildDatatypeFilterPerShard(rebuildDatatypeFilterPerShard);
    }

    public boolean isSortQueryPreIndexWithImpliedCounts() {
        return getConfig().isSortQueryPreIndexWithImpliedCounts();
    }

    public void setSortQueryPreIndexWithImpliedCounts(boolean sortQueryPreIndexWithImpliedCounts) {
        getConfig().setSortQueryPreIndexWithImpliedCounts(sortQueryPreIndexWithImpliedCounts);
    }

    public boolean isSortQueryPreIndexWithFieldCounts() {
        return getConfig().isSortQueryPreIndexWithFieldCounts();
    }

    public void setSortQueryPreIndexWithFieldCounts(boolean sortQueryPreIndexWithFieldCounts) {
        getConfig().setSortQueryPreIndexWithImpliedCounts(sortQueryPreIndexWithFieldCounts);
    }

    public boolean isSortQueryPostIndexWithFieldCounts() {
        return getConfig().isSortQueryPostIndexWithFieldCounts();
    }

    public void setSortQueryPostIndexWithFieldCounts(boolean sortQueryPostIndexWithFieldCounts) {
        getConfig().setSortQueryPostIndexWithFieldCounts(sortQueryPostIndexWithFieldCounts);
    }

    public boolean isSortQueryPostIndexWithTermCounts() {
        return getConfig().isSortQueryPostIndexWithTermCounts();
    }

    public void setSortQueryPostIndexWithTermCounts(boolean sortQueryPostIndexWithTermCounts) {
        getConfig().setSortQueryPostIndexWithTermCounts(sortQueryPostIndexWithTermCounts);
    }

    public int getCardinalityThreshold() {
        return getConfig().getCardinalityThreshold();
    }

    public void setCardinalityThreshold(int cardinalityThreshold) {
        getConfig().setCardinalityThreshold(cardinalityThreshold);
    }

    public boolean isUseQueryTreeScanHintRules() {
        return getConfig().isUseQueryTreeScanHintRules();
    }

    public void setUseQueryTreeScanHintRules(boolean useQueryTreeScanHintRules) {
        getConfig().setUseQueryTreeScanHintRules(useQueryTreeScanHintRules);
    }

    public List<ScanHintRule<JexlNode>> getQueryTreeScanHintRules() {
        return getConfig().getQueryTreeScanHintRules();
    }

    public void setQueryTreeScanHintRules(List<ScanHintRule<JexlNode>> queryTreeScanHintRules) {
        getConfig().setQueryTreeScanHintRules(queryTreeScanHintRules);
    }

    public void setIndexFieldHoleMinThreshold(double fieldIndexHoleMinThreshold) {
        getConfig().setIndexFieldHoleMinThreshold(fieldIndexHoleMinThreshold);
    }

    public double getIndexFieldHoleMinThreshold() {
        return getConfig().getIndexFieldHoleMinThreshold();
    }

    public List<QueryRule> getValidationRules() {
        return validationRules;
    }

    public void setValidationRules(List<QueryRule> validationRules) {
        if (validationRules == null) {
            this.validationRules = List.of();
        } else {
            this.validationRules = new ArrayList<>();
            for (QueryRule rule : validationRules) {
                QueryRule copy = rule.copy();
                // If a rule name was not specified, use the name of the class for easier identification later.
                if (StringUtils.isBlank(copy.getName())) {
                    copy.setName(copy.getClass().getSimpleName());
                }
                this.validationRules.add(copy);
            }
        }
    }

    @Override
    public Transformer<Object,QueryValidationResponse> getQueryValidationResponseTransformer() {
        if (this.validationResponseTransformer == null) {
            this.validationResponseTransformer = new QueryValidationResultTransformer();
        }
        return validationResponseTransformer;
    }

    public void setValidationResponseTransformer(Transformer<Object,QueryValidationResponse> queryValidationResponseTransformer) {
        this.validationResponseTransformer = queryValidationResponseTransformer;
    }

    public Set<String> getNoExpansionIfCurrentDateTypes() {
        return getConfig().getNoExpansionIfCurrentDateTypes();
    }

    public void setNoExpansionIfCurrentDateTypes(Set<String> noExpansionIfCurrentDateTypes) {
        getConfig().setNoExpansionIfCurrentDateTypes(noExpansionIfCurrentDateTypes);
    }
}
