package datawave.query.planner;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.tuple.Pair;

import datawave.core.query.configuration.QueryData;
import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveAsyncOperationException;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.jexl.visitors.PushdownUnindexedFieldsVisitor;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.QueryStopwatch;
import datawave.util.time.TraceStopwatch;

/**
 * This callable will hold the state of one of the plans and the future used to generate that plan.
 */
public class SubPlanCallable implements Callable<CloseableIterable<QueryData>> {
    private final ShardQueryConfiguration planningConfig;
    private final Map.Entry<Pair<Date,Date>,Set<String>> dateRange;
    private final DefaultQueryPlanner basePlanner;
    private final ScannerFactory scannerFactory;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");

    // handles boilerplate operations that surround a visitor's execution (e.g., timers, logging, validating)
    private final TimedVisitorManager visitorManager = new TimedVisitorManager();

    private volatile ShardQueryConfiguration subPlanConfig;

    // The planner clone that executes this sub-plan, and the state tracking who is responsible for shutting its thread pool down. All three are guarded by
    // this callable's monitor so that registering a clone cannot interleave with releasing it: the owning planner may be closed from a different thread
    // than the one iterating the sub-plans.
    private DefaultQueryPlanner subPlanner;
    private boolean planning;
    private boolean released;

    public SubPlanCallable(DefaultQueryPlanner planner, ShardQueryConfiguration planningConfig, Map.Entry<Pair<Date,Date>,Set<String>> dateRange,
                    ScannerFactory scannerFactory) {
        this.basePlanner = planner;
        this.planningConfig = planningConfig;
        this.dateRange = dateRange;
        this.scannerFactory = scannerFactory;
    }

    @Override
    public CloseableIterable<QueryData> call() throws Exception {
        try {
            // Get an updated configuration with the new date range and query tree
            this.subPlanConfig = getUpdatedConfig(planningConfig, dateRange.getKey(), dateRange.getValue());

            // Create a copy of the original default query planner, and process the query with the new date range. It is registered with this callable so
            // that the resources it allocates (notably its thread pool) can be released when the owning planner is closed.
            DefaultQueryPlanner planner = basePlanner.clone();
            registerPlanner(planner);

            try {
                // Get the range stream for the new date range and query
                return planner.reprocess(subPlanConfig, subPlanConfig.getQuery(), scannerFactory);
            } finally {
                planningComplete();
            }
        } catch (Exception e) {
            // subPlanConfig may still be null if getUpdatedConfig() threw, so take the query id from the config we were handed instead.
            throw new DatawaveAsyncOperationException("Failed to generate partitioned for " + queryId() + " and date range " + dateRange.getKey(), e);
        }
    }

    public Map.Entry<Pair<Date,Date>,Set<String>> getDateRange() {
        return dateRange;
    }

    public ShardQueryConfiguration getSubPlanConfig() {
        return subPlanConfig;
    }

    /**
     * Take ownership of the planner clone that is about to plan this sub-plan.
     *
     * @param planner
     *            the planner clone that will execute this sub-plan
     * @throws IllegalStateException
     *             if this callable has already been released, in which case planning must not start: it would allocate a thread pool that nothing is left to
     *             shut down
     */
    private synchronized void registerPlanner(DefaultQueryPlanner planner) {
        if (released) {
            throw new IllegalStateException("Sub-plan was released before it could be planned");
        }
        this.subPlanner = planner;
        this.planning = true;
    }

    /**
     * Mark planning as complete, releasing the planner clone's thread pool if {@link #releasePlanner()} was called while planning was still in progress.
     */
    private synchronized void planningComplete() {
        this.planning = false;
        if (released) {
            shutdownPlanner();
        }
    }

    /**
     * Release the resources held by this sub-plan's planner clone, shutting down its thread pool. The clone shares its query with the planner it was cloned
     * from, so only the thread pool is released here. If the clone is still planning, the shutdown is deferred to {@link #planningComplete()} so that an
     * in-flight sub-plan is not left submitting work to a terminated pool. Once released, this callable refuses to plan at all, so a {@link #call()} that has
     * not yet reached {@link #registerPlanner(DefaultQueryPlanner)} cannot strand a newly allocated pool.
     */
    public synchronized void releasePlanner() {
        this.released = true;
        if (!planning) {
            shutdownPlanner();
        }
    }

    /**
     * Shut down the registered planner clone's thread pool and forget it. Must be called while holding this callable's monitor.
     */
    private void shutdownPlanner() {
        if (subPlanner != null) {
            subPlanner.shutdownExecutor();
            subPlanner = null;
        }
    }

    /**
     * Return the id of the query being planned, for use in messages. Tolerates a config with no query set.
     *
     * @return the query id, or null if unavailable
     */
    private Object queryId() {
        ShardQueryConfiguration config = subPlanConfig != null ? subPlanConfig : planningConfig;
        return (config != null && config.getQuery() != null) ? config.getQuery().getId() : null;
    }

    /**
     * Get a configuration object configured with an updated query date range, and a plan with pushed down unindexed fields.
     *
     * @param originalConfig
     * @param dateRange
     * @param unindexedFields
     * @return The new configuration
     * @throws DatawaveQueryException
     */
    private ShardQueryConfiguration getUpdatedConfig(ShardQueryConfiguration originalConfig, Pair<Date,Date> dateRange, Set<String> unindexedFields)
                    throws DatawaveQueryException {
        // Format the beginDate and endDate of the current sub-query to execute.
        String subBeginDate = dateFormat.format(dateRange.getLeft());
        String subEndDate = dateFormat.format(dateRange.getRight());

        // Start a new stopwatch.
        final QueryStopwatch timers = originalConfig.getTimers();
        TraceStopwatch stopwatch = timers.newStartedStopwatch("FederatedQueryPlanner - Executing sub-plan against date range (" + subBeginDate + "-"
                        + subEndDate + ") with unindexed fields " + unindexedFields);

        try {
            // Set the new date range in a copy of the config.
            ShardQueryConfiguration configCopy = new ShardQueryConfiguration(originalConfig);
            configCopy.setBeginDate(dateRange.getLeft());
            configCopy.setEndDate(dateRange.getRight());

            // we want to make sure the same query id for tracking purposes and execution
            configCopy.getQuery().setId(originalConfig.getQuery().getId());

            if (!unindexedFields.isEmpty()) {
                configCopy.setQueryTree(visitorManager.timedVisit(timers, "Push down indexed field holes",
                                () -> (PushdownUnindexedFieldsVisitor.pushdownPredicates(configCopy.getQueryTree(), unindexedFields))));
            }

            return configCopy;
        } finally {
            stopwatch.stop();
        }
    }

}
