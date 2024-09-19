package datawave.query.planner;

import java.io.IOException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

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
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.exceptions.EmptyUnfieldedTermExpansionException;
import datawave.query.exceptions.NoResultsException;
import datawave.query.index.lookup.UidIntersector;
import datawave.query.jexl.visitors.QueryFieldsVisitor;
import datawave.query.jexl.visitors.UnfieldedIndexExpansionVisitor;
import datawave.query.model.FieldIndexHole;
import datawave.query.model.QueryModel;
import datawave.query.planner.pushdown.rules.PushDownRule;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.query.util.QueryStopwatch;
import datawave.util.time.TraceStopwatch;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.QueryException;

/**
 * Executes a query over a time range while handling the case where a field may be both indexed and not indexed in the time range. A period of time in which a
 * field is not indexed will be referred to herein as a field index hole. Given a query that matches against fields with field index holes, the query will be
 * broken up into multiple sub-queries. Each sub-query will query over a span of time within the query's original time range where either no field index holes
 * are present for that span of time, or there is a field index hole present for each date in that span of time. The results for each sub-query will be
 * aggregated and returned.
 *
 * @see #process(GenericQueryConfiguration, String, Query, ScannerFactory)
 */
public class FederatedQueryPlanner extends QueryPlanner implements Cloneable {

    private static final Logger log = ThreadConfigurableLogger.getLogger(FederatedQueryPlanner.class);

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
    private final Calendar calendar = Calendar.getInstance();

    // we want a unique set of plans, but maintain insertion order (facilitates easier testing)
    private final Set<String> plans = new LinkedHashSet<>();
    private DefaultQueryPlanner queryPlanner;
    private String plannedScript;

    /**
     * Return a new {@link FederatedQueryPlanner} instance with a new {@link DefaultQueryPlanner} inner query planner instance.
     */
    public FederatedQueryPlanner() {
        this(new DefaultQueryPlanner());
    }

    /**
     * Return a new {@link FederatedQueryPlanner} instance with the given inner query planner.
     *
     * @param queryPlanner
     *            the inner query planner
     */
    public FederatedQueryPlanner(DefaultQueryPlanner queryPlanner) {
        this.queryPlanner = queryPlanner;
    }

    /**
     * Return a copy of the given {@link FederatedQueryPlanner} instance.
     *
     * @param other
     *            the instance to copy
     */
    public FederatedQueryPlanner(FederatedQueryPlanner other) {
        this.queryPlanner = other.queryPlanner != null ? other.queryPlanner.clone() : null;
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
     * {@link FederatedQueryPlanner#process(GenericQueryConfiguration, String, Query, ScannerFactory)}.
     *
     * @return the planned script
     */
    @Override
    public String getPlannedScript() {
        return this.plannedScript;
    }

    /**
     * Returns a copy of this planner.
     *
     * @return the copy
     */
    @Override
    public FederatedQueryPlanner clone() {
        return new FederatedQueryPlanner(this);
    }

    /**
     * Calls {@link DefaultQueryPlanner#close(GenericQueryConfiguration, Query)} on the inner query planner instance with the given config and settings.
     *
     * @param config
     *            the config
     * @param settings
     *            the settings
     */
    @Override
    public void close(GenericQueryConfiguration config, Query settings) {
        this.queryPlanner.close(config, settings);
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
     * Not supported for {@link FederatedQueryPlanner} and will result in an {@link UnsupportedOperationException}.
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
     *             if an exception occurs
     */
    @Override
    public CloseableIterable<QueryData> process(GenericQueryConfiguration genericConfig, String query, Query settings, ScannerFactory scannerFactory)
                    throws DatawaveQueryException {
        // Validate the config type.
        if (!ShardQueryConfiguration.class.isAssignableFrom(genericConfig.getClass())) {
            throw new ClassCastException("Config must be an instance of " + ShardQueryConfiguration.class.getSimpleName());
        }

        // Reset the planned script.
        this.plannedScript = null;
        this.plans.clear();

        if (log.isDebugEnabled()) {
            log.debug("Federated query: " + query);
        }

        ShardQueryConfiguration originalConfig = (ShardQueryConfiguration) genericConfig;
        if (log.isDebugEnabled()) {
            log.debug("Query's original date range " + dateFormat.format(originalConfig.getBeginDate()) + "-" + dateFormat.format(originalConfig.getEndDate()));
        }

        // Get the relevant date ranges.
        SortedSet<Pair<Date,Date>> dateRanges = getSubQueryDateRanges(originalConfig, query, scannerFactory);

        // If debug is enabled, log the date ranges to be queried over in formatted form.
        if (log.isDebugEnabled()) {
            if (dateRanges.size() == 1) {
                log.debug("One query will be executed over original date range " + dateFormat.format(originalConfig.getBeginDate()) + "-"
                                + dateFormat.format(originalConfig.getEndDate()));
            } else {
                StringBuilder sb = new StringBuilder();
                Iterator<Pair<Date,Date>> it = dateRanges.iterator();
                while (it.hasNext()) {
                    Pair<Date,Date> range = it.next();
                    sb.append(dateFormat.format(range.getLeft())).append("-").append(dateFormat.format(range.getRight()));
                    if (it.hasNext()) {
                        sb.append(", ");
                    }
                }
                log.debug(dateRanges.size() + " sub-queries will be executed over date ranges: " + sb);
            }
        }

        // Execute the same query for each date range and collect the results.
        FederatedQueryIterable results = new FederatedQueryIterable();
        int totalProcessed = 1;
        ShardQueryConfiguration firstConfigCopy = null;
        UUID queryId = originalConfig.getQuery().getId();
        for (Pair<Date,Date> dateRange : dateRanges) {
            // Format the start and end date of the current sub-query to execute.
            String subStartDate = dateFormat.format(dateRange.getLeft());
            String subEndDate = dateFormat.format(dateRange.getRight());

            // Start a new stopwatch.
            TraceStopwatch stopwatch = originalConfig.getTimers().newStartedStopwatch("FederatedQueryPlanner - Executing sub-plan [" + totalProcessed + " of "
                            + dateRanges.size() + "] against date range (" + subStartDate + "-" + subEndDate + ")");

            // Set the new date range in a copy of the config.
            ShardQueryConfiguration configCopy = new ShardQueryConfiguration(originalConfig);
            configCopy.setBeginDate(dateRange.getLeft());
            configCopy.setEndDate(dateRange.getRight());

            // we want to make sure the same query id for tracking purposes and execution
            configCopy.getQuery().setId(queryId);

            // Create a copy of the original default query planner, and process the query with the new date range.
            DefaultQueryPlanner subPlan = this.queryPlanner.clone();

            try {

                CloseableIterable<QueryData> queryData = subPlan.process(configCopy, query, settings, scannerFactory);
                results.addIterable(queryData);
            } catch (Exception e) {
                log.warn("Exception occured when processing sub-plan [" + totalProcessed + " of " + dateRanges.size() + "] against date range (" + subStartDate
                                + "-" + subEndDate + ")", e);
                // If an exception occurs, ensure that the planned script and the original config are updated before allowing the exception to bubble up.
                plans.add(subPlan.plannedScript);
                updatePlannedScript();

                // Copy over any changes in the sub-config to the original config. This will not affect the start date, end date, or timers of the original
                // config.
                copySubConfigPropertiesToOriginal(originalConfig, configCopy);
                throw e;
            } finally {
                // Append the timers from the config copy to the original config for logging later.
                originalConfig.appendTimers(configCopy.getTimers());
                if (log.isDebugEnabled()) {
                    log.debug("Query string for config of sub-plan " + totalProcessed + ": " + configCopy.getQueryString());
                }
            }

            // Ensure we're tracking the planned script from the sub-plan.
            plans.add(subPlan.getPlannedScript());

            // Track the first sub-config.
            if (firstConfigCopy == null) {
                firstConfigCopy = configCopy;
                if (log.isDebugEnabled()) {
                    log.debug("Federated first config query string: " + firstConfigCopy.getQueryString());
                }
            }

            stopwatch.stop();
            totalProcessed++;
        }

        // Update the planned script.
        updatePlannedScript();

        // Copy over any changes from the first sub-config to the original config. This will not affect the start date, end date, or timers of the original
        // config.
        copySubConfigPropertiesToOriginal(originalConfig, firstConfigCopy);

        // Return the collected results.
        return results;
    }

    /**
     * Update the planned script to represent a concatenation of the planned scripts from all sub-plans of the most recently executed call to
     * {@link #process(GenericQueryConfiguration, String, Query, ScannerFactory)}.
     */
    private void updatePlannedScript() {
        if (plans.isEmpty()) {
            this.plannedScript = "";
        } else if (this.plans.size() == 1) {
            this.plannedScript = this.plans.iterator().next();
        } else {
            int lastIndex = plans.size() - 1;
            StringBuilder sb = new StringBuilder();
            int i = 0;
            for (String plan : plans) {
                if (sb.length() > 0) {
                    sb.append(" || ");
                }
                sb.append("((plan = ").append(++i).append(") && (").append(plan).append("))");
            }
            this.plannedScript = sb.toString();
        }
    }

    /**
     * Copy over all changes from the sub config to the original config, while preserving the start date, end date, and timers of the original config.
     */
    private void copySubConfigPropertiesToOriginal(ShardQueryConfiguration original, ShardQueryConfiguration subConfig) {
        Date originalBeginDate = original.getBeginDate();
        Date originalEndDate = original.getEndDate();
        QueryStopwatch originalTimers = original.getTimers();
        // Copy over all properties from the sub-config.
        original.copyFrom(subConfig);
        // Ensure the original begin date, end date, and timers are restored after copying over all changes from the sub-config.
        original.setBeginDate(originalBeginDate);
        original.setEndDate(originalEndDate);
        original.setTimers(originalTimers);
    }

    /**
     * Return the set of date ranges that sub-queries should be created for. Each date range will have a consistent index state, meaning that within each date
     * range, we can expect to either encounter no field index holes, or to always encounter a field index hole.
     */
    private SortedSet<Pair<Date,Date>> getSubQueryDateRanges(ShardQueryConfiguration config, String query, ScannerFactory scannerFactory)
                    throws DatawaveQueryException {
        // Fetch the field index holes for the specified fields and datatypes, using the configured minimum threshold.
        MetadataHelper metadataHelper = queryPlanner.getMetadataHelper();
        Map<String,Map<String,FieldIndexHole>> fieldIndexHoles;
        try {
            Set<String> fields = getFieldsForQuery(config, query, scannerFactory);
            if (log.isDebugEnabled()) {
                log.debug("Fetching field index holes for fields " + fields + " and datatypes " + config.getDatatypeFilter());
            }
            // if we found no fields in the query, then we have no index holes
            if (fields.isEmpty()) {
                fieldIndexHoles = Collections.emptyMap();
            } else {
                fieldIndexHoles = metadataHelper.getFieldIndexHoles(fields, config.getDatatypeFilter(), config.getFieldIndexHoleMinThreshold());
            }
        } catch (TableNotFoundException | IOException e) {
            throw new DatawaveQueryException("Error occurred when fetching field index holes from metadata table", e);
        }

        // If no field index holes were found, we can return early with the original query date range.
        if (fieldIndexHoles.isEmpty()) {
            log.debug("No field index holes found");
            SortedSet<Pair<Date,Date>> ranges = new TreeSet<>();
            ranges.add(Pair.of(config.getBeginDate(), config.getEndDate()));
            return ranges;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Field index holes found for fields " + fieldIndexHoles.keySet());
            }
        }

        // Collect all field index holes that fall within the original query's target date range.
        SortedSet<Pair<Date,Date>> relevantHoles = new TreeSet<>();
        for (String field : fieldIndexHoles.keySet()) {
            Map<String,FieldIndexHole> holes = fieldIndexHoles.get(field);
            for (FieldIndexHole indexHole : holes.values()) {
                relevantHoles.addAll(getHolesWithinOriginalQueryDateRange(config.getBeginDate(), config.getEndDate(), indexHole));
            }
        }

        // Establish the date ranges we can query on.
        SortedSet<Pair<Date,Date>> subDateRanges = new TreeSet<>();
        if (relevantHoles.isEmpty()) {
            // If we found no index holes, we can default to the original target date range.
            subDateRanges.add(Pair.of(config.getBeginDate(), config.getEndDate()));
        } else {
            // Otherwise, get the valid date ranges. First, Merge any overlaps.
            SortedSet<Pair<Date,Date>> mergedHoles = mergeRanges(relevantHoles);
            Iterator<Pair<Date,Date>> it = mergedHoles.iterator();

            // If the start of the first hole occurs after the configured start date, add a range spanning from the start date to one day before the start
            // of the first hole.
            Pair<Date,Date> firstHole = it.next();
            if (firstHole.getLeft().getTime() > config.getBeginDate().getTime()) {
                subDateRanges.add(Pair.of(new Date(config.getBeginDate().getTime()), oneDayBefore(firstHole.getLeft())));
                // If the end of the first hole occurs before or on the configured end date, add the entire span for the first hole.
                if (firstHole.getRight().getTime() <= config.getEndDate().getTime()) {
                    subDateRanges.add(firstHole);
                } else {
                    // Otherwise, add a range from the start of the first hole to the configured end date.
                    subDateRanges.add(Pair.of(firstHole.getLeft(), new Date(config.getEndDate().getTime())));
                }
                // If the start of the first hole is equal to the configured start date, check if the entire hole falls within the query's date range.
            } else if (firstHole.getLeft().getTime() == config.getBeginDate().getTime()) {
                // If the end of the first hole occurs before or on the configured end date, add the entire span for the first hole.
                if (firstHole.getRight().getTime() <= config.getEndDate().getTime()) {
                    subDateRanges.add(firstHole);
                } else {
                    // Otherwise, the first hole spans over the query's date range.
                    subDateRanges.add(Pair.of(new Date(config.getBeginDate().getTime()), new Date(config.getEndDate().getTime())));
                }
                // If the start of the first hole occurs before the configured start date, check how much of the hole falls within the query's date range.
            } else if (firstHole.getLeft().getTime() < config.getBeginDate().getTime()) {
                // If the end of the first hole occurs before or on the configured end date, add a range spanning from the configured start date to the end of
                // the first hole.
                if (firstHole.getRight().getTime() <= config.getEndDate().getTime()) {
                    subDateRanges.add(Pair.of(new Date(config.getBeginDate().getTime()), firstHole.getRight()));
                } else {
                    // Otherwise, the first hole spans over the query's date range.
                    subDateRanges.add(Pair.of(new Date(config.getBeginDate().getTime()), new Date(config.getEndDate().getTime())));
                }
            }

            // Track the end of the previous hole.
            Date endOfPrevHole = firstHole.getRight();
            while (it.hasNext()) {
                // The start of the next hole is guaranteed to fall within the original query's target date range. Add a target date range from one day after
                // the end of the previous hole to one day before the start of the next hole.
                Pair<Date,Date> currentHole = it.next();
                subDateRanges.add(Pair.of(oneDayAfter(endOfPrevHole), oneDayBefore(currentHole.getLeft())));

                // If there is another hole, the current hole is guaranteed to fall within the original target's date range. Add it to the sub ranges.
                if (it.hasNext()) {
                    subDateRanges.add(currentHole);
                } else {
                    // If this is the last hole, it is possible that the end date falls outside the original query's target date range. If so, shorten it to end
                    // at the original target end date.
                    if (currentHole.getRight().getTime() > config.getEndDate().getTime()) {
                        subDateRanges.add(Pair.of(currentHole.getLeft(), config.getEndDate()));
                    } else {
                        // If it does not fall outside the target date range, include it as is.
                        subDateRanges.add(currentHole);
                    }
                }
                endOfPrevHole = currentHole.getRight();
            }

            // If the last hole we saw ended before the end of the original query's target date range, add a target date range from one day after the end of the
            // last hole to the original target end date.
            if (endOfPrevHole.getTime() < config.getEndDate().getTime()) {
                subDateRanges.add(Pair.of(oneDayAfter(endOfPrevHole), new Date(config.getEndDate().getTime())));
            }
        }

        return subDateRanges;
    }

    /**
     * Return the set of fields in the query.
     */
    private Set<String> getFieldsForQuery(ShardQueryConfiguration config, String query, ScannerFactory scannerFactory) throws NoResultsException {
        // Parse the query.
        ASTJexlScript queryTree = queryPlanner.parseQueryAndValidatePattern(query, null);

        // Apply the query model.
        MetadataHelper metadataHelper = queryPlanner.getMetadataHelper();
        QueryModel queryModel = queryPlanner.loadQueryModel(config);
        if (queryModel != null) {
            queryTree = queryPlanner.applyQueryModel(metadataHelper, config, queryTree, queryModel);
        } else {
            log.warn("Query model was null, will not apply to query tree.");
        }

        // Expand unfielded terms unless explicitly disabled
        if (!queryPlanner.disableAnyFieldLookup) {
            ShardQueryConfiguration configCopy = new ShardQueryConfiguration(config);
            try {
                configCopy.setIndexedFields(metadataHelper.getIndexedFields(config.getDatatypeFilter()));
                configCopy.setReverseIndexedFields(metadataHelper.getReverseIndexedFields(config.getDatatypeFilter()));
                queryTree = UnfieldedIndexExpansionVisitor.expandUnfielded(configCopy, scannerFactory, metadataHelper, queryTree);
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_ACCESS_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            } catch (IllegalAccessException | InstantiationException e) {
                throw new DatawaveFatalQueryException(e);
            } catch (EmptyUnfieldedTermExpansionException e) {
                // in this case the planner will simply return an empty iterator, so ignore and keep going
                log.warn("Empty query", e);
            }
        }

        // Extract and return the fields from the query.
        return QueryFieldsVisitor.parseQueryFields(queryTree, metadataHelper);
    }

    /**
     * Return the set of any field index hole date ranges that fall within the original query's target date range.
     */
    private SortedSet<Pair<Date,Date>> getHolesWithinOriginalQueryDateRange(Date beginDate, Date endDate, FieldIndexHole fieldIndexHole) {
        SortedSet<Pair<Date,Date>> holes = fieldIndexHole.getDateRanges();
        // If the earliest date range falls after the original query date range, or the latest date range falls before the original query range, then none of
        // the holes fall within the date range.
        if (isOutsideDateRange(beginDate, endDate, holes.first(), holes.last())) {
            return Collections.emptySortedSet();
        }

        // There is at least one index hole that falls within the original query date range. Collect and return them.
        return holes.stream().filter((range) -> isInDateRange(beginDate, endDate, range)).collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Return whether the given date ranges representing the earliest and latest date ranges respectively do not encompass any dates that could fall within the
     */
    private boolean isOutsideDateRange(Date beginDate, Date endDate, Pair<Date,Date> earliestRange, Pair<Date,Date> latestRange) {
        return earliestRange.getLeft().getTime() > endDate.getTime() || latestRange.getRight().getTime() < beginDate.getTime();
    }

    /**
     * Return whether any dates in the given date range fall within the original query's target date range, inclusively.
     */
    private boolean isInDateRange(Date beginDate, Date endDate, Pair<Date,Date> dateRange) {
        return isInDateRange(beginDate, endDate, dateRange.getLeft()) || isInDateRange(beginDate, endDate, dateRange.getRight());
    }

    /**
     * Return whether the given date falls within the start and end date of the original query's target date range, inclusively.
     */
    private boolean isInDateRange(Date beginDate, Date endDate, Date date) {
        return beginDate.getTime() <= date.getTime() && date.getTime() <= endDate.getTime();
    }

    /**
     * Merge all consecutive/overlapping date ranges in the given set and return them.
     */
    private SortedSet<Pair<Date,Date>> mergeRanges(SortedSet<Pair<Date,Date>> ranges) {
        // No merging needs to occur if there is only one date range.
        if (ranges.size() == 1) {
            return ranges;
        }

        SortedSet<Pair<Date,Date>> merged = new TreeSet<>();

        // Scan over each date range and merge overlapping ones.
        Iterator<Pair<Date,Date>> it = ranges.iterator();
        Pair<Date,Date> prev = it.next();
        while (it.hasNext()) {
            Pair<Date,Date> curr = it.next();
            if (curr.getLeft().getTime() <= prev.getRight().getTime() || curr.getLeft().getTime() == oneDayAfter(prev.getRight()).getTime()) {
                // If the current date range's start date is equal to or before the end date of the previous date range, or is directly consecutive to the
                // previous date range, replace the previous date range with a new date range that spans both date ranges.
                prev = Pair.of(prev.getLeft(), curr.getRight());
            } else {
                // The previous and current date ranges do not overlap. Add the previous date range as a fully-merged range, and replace it with the current
                // date range.
                merged.add(prev);
                prev = curr;
            }
        }
        // Add the last date range.
        merged.add(prev);

        return merged;
    }

    /**
     * Return one day after the given date.
     */
    private Date oneDayAfter(Date date) {
        return addDays(date, 1);
    }

    /**
     * Return one day before the given date.
     */
    private Date oneDayBefore(Date date) {
        return addDays(date, -1);
    }

    /**
     * Return the given date with the number of dates added to it.
     */
    private Date addDays(Date date, int daysToAdd) {
        calendar.setTime(new Date(date.getTime()));
        calendar.add(Calendar.DATE, daysToAdd);
        return calendar.getTime();
    }
}
