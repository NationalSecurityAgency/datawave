package datawave.query.planner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.microservice.query.Query;
import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.index.lookup.UidIntersector;
import datawave.query.jexl.visitors.QueryFieldsVisitor;
import datawave.query.model.IndexFieldHole;
import datawave.query.planner.pushdown.rules.PushDownRule;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.DatePartitioner;
import datawave.query.util.MetadataHelper;

/**
 * Executes a query over a time range while handling the case where a field may be both indexed and not indexed in the time range. A period of time in which a
 * field is not indexed will be referred to herein as a field index hole. Given a query that matches against fields with field index holes, the query will be
 * broken up into multiple sub-queries. Each sub-query will query over a span of time within the query's original time range where either no field index holes
 * are present for that span of time, or there is a field index hole present for each date in that span of time. The results for each sub-query will be
 * aggregated and returned.
 *
 * @see #process(GenericQueryConfiguration, String, Query, ScannerFactory)
 */
public class DatePartitionedQueryPlanner extends QueryPlanner implements Cloneable {

    private static final Logger log = ThreadConfigurableLogger.getLogger(DatePartitionedQueryPlanner.class);

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");

    private DefaultQueryPlanner queryPlanner;
    private String initialPlan;
    private String plannedScript;

    // The planner clones created by the latest call to process(): the one used for initial planning, and one per sub-plan. Each allocates its own thread
    // pool, so they must be shut down when this planner is closed. All of the state below is guarded by plannerCloneLock because process() and close() can
    // run on different threads: a query may be cancelled or expired while it is still being planned, or while its pages are still being fetched.
    private final Object plannerCloneLock = new Object();
    private boolean closed;
    private DefaultQueryPlanner initialPlanner;
    private boolean initialPlanning;
    private List<SubPlanCallable> subPlans = Collections.emptyList();

    // handles boilerplate operations that surround a visitor's execution (e.g., timers, logging, validating)
    private final TimedVisitorManager visitorManager = new TimedVisitorManager();

    /**
     * Return a new {@link DatePartitionedQueryPlanner} instance with a new {@link DefaultQueryPlanner} inner query planner instance.
     */
    public DatePartitionedQueryPlanner() {
        this(new DefaultQueryPlanner());
    }

    /**
     * Return a new {@link DatePartitionedQueryPlanner} instance with the given inner query planner.
     *
     * @param queryPlanner
     *            the inner query planner
     */
    public DatePartitionedQueryPlanner(DefaultQueryPlanner queryPlanner) {
        this.queryPlanner = queryPlanner;
    }

    /**
     * Return a copy of the given {@link DatePartitionedQueryPlanner} instance.
     *
     * @param other
     *            the instance to copy
     */
    public DatePartitionedQueryPlanner(DatePartitionedQueryPlanner other) {
        this.queryPlanner = other.queryPlanner != null ? other.queryPlanner.clone() : null;
        this.initialPlan = other.initialPlan;
        this.plannedScript = other.plannedScript;
    }

    /**
     * Return the inner query planner.
     *
     * @return the inner query planner.
     */
    public DefaultQueryPlanner getQueryPlanner() {
        return queryPlanner;
    }

    /**
     * Set the inner query planner.
     *
     * @param queryPlanner
     *            the query planner
     */
    public void setQueryPlanner(DefaultQueryPlanner queryPlanner) {
        this.queryPlanner = queryPlanner;
    }

    /**
     * Return the planned script resulting from the latest call to
     * {@link DatePartitionedQueryPlanner#process(GenericQueryConfiguration, String, Query, ScannerFactory)}.
     *
     * @return the planned script
     */
    @Override
    public String getPlannedScript() {
        return plannedScript;
    }

    /**
     * Return the initial plan
     *
     * @return the initial plan
     */
    @Override
    public String getInitialPlan() {
        return initialPlan;
    }

    /**
     * Returns a copy of this planner.
     *
     * @return the copy
     */
    @Override
    public DatePartitionedQueryPlanner clone() {
        return new DatePartitionedQueryPlanner(this);
    }

    /**
     * Calls {@link DefaultQueryPlanner#close(GenericQueryConfiguration, Query)} on the inner query planner instance with the given config and settings, and
     * releases the resources held by every planner clone created by the latest call to
     * {@link #process(GenericQueryConfiguration, String, Query, ScannerFactory)}. A clone that is still planning when this is called is released as soon as it
     * finishes, rather than having its thread pool shut down out from under it. Once closed, a concurrent call to
     * {@link #process(GenericQueryConfiguration, String, Query, ScannerFactory)} aborts instead of creating further clones.
     *
     * @param config
     *            the config
     * @param settings
     *            the settings
     */
    @Override
    public void close(GenericQueryConfiguration config, Query settings) {
        releaseClonedPlanners(true);
        this.queryPlanner.close(config, settings);
    }

    /**
     * Shut down the thread pool of each planner clone created during {@link #process(GenericQueryConfiguration, String, Query, ScannerFactory)}. The clones
     * share their query with the inner planner, so only their thread pools are released here; the query-level cleanup is left to the inner planner's
     * {@link DefaultQueryPlanner#close(GenericQueryConfiguration, Query)}.
     *
     * @param markClosed
     *            whether to mark this planner closed, so that a concurrent {@link #process(GenericQueryConfiguration, String, Query, ScannerFactory)} refuses
     *            to register any further clone rather than leaving one behind with nothing left to release it. False when starting a new round of planning,
     *            which reopens this planner.
     */
    private void releaseClonedPlanners(boolean markClosed) {
        DefaultQueryPlanner planner = null;
        List<SubPlanCallable> plans;

        // Claim the clones under the lock so that concurrent callers cannot both release the same set, then shut them down outside of it: releasing a
        // sub-plan acquires that sub-plan's own monitor, and this lock is never held while doing so.
        synchronized (plannerCloneLock) {
            this.closed = markClosed;

            // An initial planner that is still planning keeps submitting work to its own thread pool, so shutting that pool down here would reject the work
            // it has yet to submit. It stays registered, and initialPlanningComplete() releases it as soon as planning ends.
            if (!this.initialPlanning) {
                planner = this.initialPlanner;
                this.initialPlanner = null;
            }

            plans = this.subPlans;
            this.subPlans = Collections.emptyList();
        }

        if (planner != null) {
            planner.shutdownExecutor();
        }
        for (SubPlanCallable subPlan : plans) {
            subPlan.releasePlanner();
        }
    }

    /**
     * Take ownership of the planner clone that is about to perform the initial planning pass.
     *
     * @param planner
     *            the planner clone that will perform the initial planning pass
     * @return true if the clone was registered, or false if this planner was closed first, in which case planning must not start: it would allocate a thread
     *         pool that nothing is left to shut down. A clone that has not planned yet holds no thread pool, so a refused one can simply be discarded.
     */
    private boolean registerInitialPlanner(DefaultQueryPlanner planner) {
        synchronized (plannerCloneLock) {
            if (closed) {
                return false;
            }
            this.initialPlanner = planner;
            this.initialPlanning = true;
            return true;
        }
    }

    /**
     * Mark the initial planning pass complete, releasing its planner clone if {@link #close(GenericQueryConfiguration, Query)} arrived while that pass was
     * still in progress.
     */
    private void initialPlanningComplete() {
        DefaultQueryPlanner planner = null;

        synchronized (plannerCloneLock) {
            this.initialPlanning = false;
            if (closed) {
                planner = this.initialPlanner;
                this.initialPlanner = null;
            }
        }

        if (planner != null) {
            planner.shutdownExecutor();
        }
    }

    /**
     * Take ownership of the sub-plans created for this round of planning.
     *
     * @param plans
     *            the sub-plans created for this round of planning
     * @return true if the sub-plans were registered, or false if this planner was closed first, in which case they are released instead so that none of them
     *         can start planning
     */
    private boolean registerSubPlans(List<SubPlanCallable> plans) {
        synchronized (plannerCloneLock) {
            if (!closed) {
                this.subPlans = plans;
                return true;
            }
        }

        // Released outside the lock: releasing a sub-plan acquires that sub-plan's own monitor.
        for (SubPlanCallable subPlan : plans) {
            subPlan.releasePlanner();
        }
        return false;
    }

    /**
     * Return the max ranges per query piece for the inner query planner instance.
     *
     * @return the max ranges per query piece
     */
    @Override
    public long maxRangesPerQueryPiece() {
        return this.queryPlanner.maxRangesPerQueryPiece();
    }

    /**
     * Set the query iterator class for the inner query planner instance.
     *
     * @param clazz
     *            the class to set
     */
    @Override
    public void setQueryIteratorClass(Class<? extends SortedKeyValueIterator<Key,Value>> clazz) {
        this.queryPlanner.setQueryIteratorClass(clazz);
    }

    /**
     * Return the query iterator class for the inner query planner instance.
     *
     * @return the class
     */
    @Override
    public Class<? extends SortedKeyValueIterator<Key,Value>> getQueryIteratorClass() {
        return this.queryPlanner.getQueryIteratorClass();
    }

    /**
     * Set the rules for the inner query planner instance.
     *
     * @param rules
     *            the rules to set
     */
    @Override
    public void setRules(Collection<PushDownRule> rules) {
        this.queryPlanner.setRules(rules);
    }

    /**
     * Return the rules for the inner query planner instance.
     *
     * @return the rules
     */
    @Override
    public Collection<PushDownRule> getRules() {
        return this.queryPlanner.getRules();
    }

    /**
     * Set the uids iterator class for the inner query planner instance
     *
     * @param clazz
     *            the class to set
     */
    @Override
    public void setCreateUidsIteratorClass(Class<? extends SortedKeyValueIterator<Key,Value>> clazz) {
        this.queryPlanner.setCreateUidsIteratorClass(clazz);
    }

    /**
     * Return the uids iterator class for the inner query planner instance.
     *
     * @return the class
     */
    @Override
    public Class<? extends SortedKeyValueIterator<Key,Value>> getCreateUidsIteratorClass() {
        return this.queryPlanner.getCreateUidsIteratorClass();
    }

    /**
     * Set the uid intersector for the inner query planner instance.
     *
     * @param uidIntersector
     *            the intersector
     */
    @Override
    public void setUidIntersector(UidIntersector uidIntersector) {
        this.queryPlanner.setUidIntersector(uidIntersector);
    }

    /**
     * Return the uid intersector for the inner query planner instance.
     *
     * @return the intersector
     */
    @Override
    public UidIntersector getUidIntersector() {
        return this.queryPlanner.getUidIntersector();
    }

    /**
     * Not supported for {@link DatePartitionedQueryPlanner} and will result in an {@link UnsupportedOperationException}.
     *
     * @throws UnsupportedOperationException
     *             always
     */
    @Override
    public ASTJexlScript applyRules(ASTJexlScript queryTree, ScannerFactory scannerFactory, MetadataHelper metadataHelper, ShardQueryConfiguration config) {
        throw new UnsupportedOperationException("applyRules() is not a supported operation for " + getClass().getName());
    }

    /**
     * Processes the {@code query} with the given config, settings, and scanner factory. If the query contains any field index holes within its time range, the
     * query will be broken up into multiple sub-queries where each sub-query will either scan over no field index holes for any dates in its time range, or
     * will have a field index hole for each date in its time range. The sub-queries will collectively scan over the entire original time range. The query data
     * returned will return the query data from each sub-query, in chronological order. The configuration will be updated to reflect the resulting configuration
     * from the first executed sub-query.
     *
     * @param genericConfig
     *            the query configuration config
     * @param query
     *            the query string
     * @param settings
     *            the query settings
     * @param scannerFactory
     *            the scanner factory
     * @return the query data
     * @throws DatawaveQueryException
     *             if an exception occurs, or if this planner is closed while the query is being planned
     */
    @Override
    public CloseableIterable<QueryData> process(GenericQueryConfiguration genericConfig, String query, Query settings, ScannerFactory scannerFactory)
                    throws DatawaveQueryException {
        visitorManager.setDebugEnabled(log.isDebugEnabled());

        // Validate the config type.
        if (!ShardQueryConfiguration.class.isAssignableFrom(genericConfig.getClass())) {
            throw new ClassCastException("Config must be an instance of " + ShardQueryConfiguration.class.getSimpleName());
        }

        // Reset the initial and planned script.
        this.initialPlan = null;
        this.plannedScript = null;

        // Release any planner clones left over from a previous call before creating a new set, reopening this planner so that the clones created below are
        // tracked again.
        releaseClonedPlanners(false);

        if (log.isDebugEnabled()) {
            log.debug("Federated query: " + query);
        }

        ShardQueryConfiguration shardQueryConfig = (ShardQueryConfiguration) genericConfig;
        if (log.isDebugEnabled()) {
            log.debug("Query's original date range " + dateFormat.format(shardQueryConfig.getBeginDate()) + "-"
                            + dateFormat.format(shardQueryConfig.getEndDate()));
        }

        // Let's do the planning with the delegate planner first to ensure we have a final date range
        // and appropriately expanded unfielded terms etc.
        boolean generatePlanOnly = shardQueryConfig.isGeneratePlanOnly();
        shardQueryConfig.setGeneratePlanOnly(true);
        boolean expandValues = shardQueryConfig.isExpandValues();
        // we do NOT want to expand any values yet as they may not be dependable
        // note we are expanding unfielded values (different flag)
        shardQueryConfig.setExpandValues(false);
        boolean deferPushdownPullup = shardQueryConfig.isDeferPushdownPullup();
        shardQueryConfig.setDeferPushdownPullup(true);

        try {
            // now let's do the initial planning, tracking the clone before it is used so that a concurrent close() cannot miss it
            DefaultQueryPlanner planner = this.queryPlanner.clone();
            if (!registerInitialPlanner(planner)) {
                throw new DatawaveQueryException("Query planner was closed before the initial plan could be created");
            }

            try {
                planner.process(shardQueryConfig, query, settings, scannerFactory);

                // Our initial plan and planned script will both be the initial planned script
                this.initialPlan = this.plannedScript = planner.getPlannedScript();
            } finally {
                initialPlanningComplete();
            }
        } finally {
            // and reset the expansion flags to what we had previously, whether or not the initial planning succeeded: the caller's config outlives a failed
            // plan, and must not be left in the temporary initial-planning mode set above
            shardQueryConfig.setGeneratePlanOnly(generatePlanOnly);
            shardQueryConfig.setExpandValues(expandValues);
            shardQueryConfig.setDeferPushdownPullup(deferPushdownPullup);
        }

        // Get the relevant date ranges and the sets of fields that have gaps in those ranges
        SortedMap<Pair<Date,Date>,Set<String>> dateRanges = getSubQueryDateRanges(shardQueryConfig);

        // create a clone of the config for the sub plan callables as the planningConfig may be updated dynamically
        ShardQueryConfiguration planningConfig = new ShardQueryConfiguration(shardQueryConfig);

        // Create a callable for each sub plan
        List<SubPlanCallable> futures = new ArrayList<>();
        for (Map.Entry<Pair<Date,Date>,Set<String>> dateRange : dateRanges.entrySet()) {
            SubPlanCallable subPlan = new SubPlanCallable(this.queryPlanner, planningConfig, dateRange, scannerFactory);
            futures.add(subPlan);
        }
        if (!registerSubPlans(futures)) {
            throw new DatawaveQueryException("Query planner was closed before its sub-plans could be created");
        }

        // create a listener for plan updates and update the configuration
        PlanListener listener = plan -> {
            plannedScript = plan;
            genericConfig.setQueryString(plan);
        };

        // Return an iterable out of the callables
        DatePartitionedQueryIterable iterable = new DatePartitionedQueryIterable(futures, shardQueryConfig, listener);

        return iterable;
    }

    /**
     * Return the set of date ranges that sub-queries should be created for. Each date range will have a consistent index state, meaning that within each date
     * range all query fields are either indexed or not-indexed across the entire range. It is expected that the date ranges will completely cover the original
     * query date range without gaps or overlaps.
     */
    protected SortedMap<Pair<Date,Date>,Set<String>> getSubQueryDateRanges(ShardQueryConfiguration config) throws DatawaveQueryException {
        // Fetch the field index holes for the specified fields and datatypes, using the configured minimum threshold.
        Map<String,Map<String,IndexFieldHole>> fieldIndexHolesByDatatype = getFieldIndexHoles(config);
        return DatePartitioner.partition(fieldIndexHolesByDatatype, config.getBeginDate(), config.getEndDate());
    }

    /**
     * Get the field index holes for the fields in the query
     *
     * @param config
     * @return field to datatype to index field holes
     * @throws DatawaveQueryException
     */
    private Map<String,Map<String,IndexFieldHole>> getFieldIndexHoles(ShardQueryConfiguration config) throws DatawaveQueryException {
        MetadataHelper metadataHelper = queryPlanner.getMetadataHelper();
        Map<String,Map<String,IndexFieldHole>> fieldIndexHolesByDatatype;
        try {
            Set<String> fields = getFieldsForQuery(config.getQueryTree(), metadataHelper);
            if (log.isDebugEnabled()) {
                log.debug("Fetching field index holes for fields " + fields + " and datatypes " + config.getDatatypeFilter());
            }
            // if we found no fields in the query, then we have no index holes
            if (fields.isEmpty()) {
                fieldIndexHolesByDatatype = Collections.emptyMap();
            } else {
                fieldIndexHolesByDatatype = metadataHelper.getFieldIndexHoles(fields, config.getDatatypeFilter(), config.getIndexFieldHoleMinThreshold());
            }
        } catch (TableNotFoundException | IOException e) {
            throw new DatawaveQueryException("Error occurred when fetching field index holes from metadata table", e);
        }
        return fieldIndexHolesByDatatype;
    }

    /**
     * Return the set of fields in the query.
     */
    protected Set<String> getFieldsForQuery(ASTJexlScript queryTree, MetadataHelper metadataHelper) {
        // Extract and return the fields from the query.
        return QueryFieldsVisitor.parseQueryFields(queryTree, metadataHelper);
    }

}
