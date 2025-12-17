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

    private ShardQueryConfiguration subPlanConfig;

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

            // Create a copy of the original default query planner, and process the query with the new date range.
            DefaultQueryPlanner subPlan = basePlanner.clone();

            // Get the range stream for the new date range and query
            return subPlan.reprocess(subPlanConfig, subPlanConfig.getQuery(), scannerFactory);
        } catch (Exception e) {
            throw new DatawaveAsyncOperationException(
                            "Failed to generate partitioned for " + subPlanConfig.getQuery().getId() + " and date range " + dateRange.getKey(), e);
        }
    }

    public Map.Entry<Pair<Date,Date>,Set<String>> getDateRange() {
        return dateRange;
    }

    public ShardQueryConfiguration getSubPlanConfig() {
        return subPlanConfig;
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
