package datawave.query.planner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.visitors.QueryFieldsVisitor;
import datawave.query.jexl.visitors.UnfieldedIndexExpansionVisitor;
import datawave.query.model.QueryModel;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.model.FieldIndexHole;
import datawave.query.planner.pushdown.rules.PushDownRule;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.util.time.TraceStopwatch;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.configuration.QueryData;

public class FederatedQueryPlanner extends QueryPlanner {

    private static final Logger log = ThreadConfigurableLogger.getLogger(FederatedQueryPlanner.class);
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
    private final Calendar calendar = Calendar.getInstance();

    private final ShardQueryConfiguration originalConfig;
    private final DefaultQueryPlanner originalPlanner;
    private final Date originalBeginDate;
    private final Date originalEndDate;
    private String plannedScript;

    public FederatedQueryPlanner(ShardQueryConfiguration config, DefaultQueryPlanner planner) {
        this.originalConfig = config;
        this.originalPlanner = planner;
        this.originalBeginDate = config.getBeginDate();
        this.originalEndDate = config.getEndDate();
    }

    @Override
    public CloseableIterable<QueryData> process(GenericQueryConfiguration genericConfig, String query, Query settings, ScannerFactory scannerFactory)
                    throws DatawaveQueryException {
        // Validate the config type.
        if (!(genericConfig instanceof ShardQueryConfiguration)) {
            throw new ClassCastException("Config must be an instance of " + ShardQueryConfiguration.class.getSimpleName());
        }

        ShardQueryConfiguration config = (ShardQueryConfiguration) genericConfig;

        log.debug("Query originally set to execute against date range " + dateFormat.format(originalBeginDate) + "-" + dateFormat.format(originalEndDate));

        // Get the relevant date ranges.
        SortedSet<Pair<Date,Date>> dateRanges = getSubQueryDateRanges(query, scannerFactory);

        // Execute the same query for each date range and collect the results.
        FederatedQueryIterable results = new FederatedQueryIterable();
        int totalProcessed = 1;
        for (Pair<Date,Date> dateRange : dateRanges) {
            TraceStopwatch stopwatch = config.getTimers()
                            .newStartedStopwatch("FederatedQueryPlanner - Execute query against date range subset " + dateFormat.format(dateRange.getLeft())
                                            + "-" + dateFormat.format(dateRange.getRight()) + " [" + totalProcessed + " of " + dateRanges.size() + "]");
            log.debug("Executing query against date range " + dateFormat.format(dateRange.getLeft()) + "-" + dateFormat.format(dateRange.getRight()));

            // Set the new date range in a copy of the config.
            ShardQueryConfiguration configCopy = new ShardQueryConfiguration(config);
            configCopy.setBeginDate(dateRange.getLeft());
            configCopy.setEndDate(dateRange.getRight());

            // Create a copy of the original default query planner, and process the query with the new date range.
            DefaultQueryPlanner planner = new DefaultQueryPlanner(originalPlanner);
            results.addIterable(planner.process(config, query, settings, scannerFactory));

            // Update the planned script to reflect that of the first query.
            if (plannedScript == null) {
                plannedScript = planner.getPlannedScript();
            }
            stopwatch.stop();
            totalProcessed++;
        }
        
        // Return the collected results.
        return results;
    }
    
    /**
     * Return the set of date ranges that sub-queries should be created for. Each date range will have a consistent index state, meaning that within each date
     * range, we can expect to either encounter no field index holes, or to always encounter a field index hole.
     */
    private SortedSet<Pair<Date,Date>> getSubQueryDateRanges(String query, ScannerFactory scannerFactory) throws DatawaveQueryException {
        // Fetch the field index holes for the specified fields and datatypes, using the configured minimum threshold.
        MetadataHelper metadataHelper = originalPlanner.getMetadataHelper();
        Map<String,Map<String,FieldIndexHole>> fieldIndexHoles;
        try {
            Set<String> fields = getFieldsForQuery(query, scannerFactory);
            fieldIndexHoles = metadataHelper.getFieldIndexHoles(fields, originalConfig.getDatatypeFilter(), originalConfig.getFieldIndexHoleMinThreshold());
        } catch (TableNotFoundException | IOException e) {
            throw new DatawaveQueryException("Error occurred when fetching field index holes from metadata table", e);
        }

        // Collect all field index holes that fall within the original query's target date range.
        SortedSet<Pair<Date,Date>> relevantHoles = new TreeSet<>();
        for (String field : fieldIndexHoles.keySet()) {
            Map<String,FieldIndexHole> holes = fieldIndexHoles.get(field);
            for (FieldIndexHole indexHole : holes.values()) {
                relevantHoles.addAll(getHolesWithinOriginalQueryDateRange(indexHole));
            }
        }

        // Establish the date ranges we can query on.
        SortedSet<Pair<Date,Date>> subDateRanges = new TreeSet<>();
        if (relevantHoles.isEmpty()) {
            // If we found no index holes, we can default to the original target date range.
            subDateRanges.add(Pair.of(originalBeginDate, originalEndDate));
        } else {
            // Otherwise, get the valid date ranges.
            // Merge any overlaps.
            SortedSet<Pair<Date,Date>> mergedHoles = mergeOverlappingRanges(relevantHoles);

            // Determine if the first hole starts after the original target start date. If so, add a target date range from the original target start date to
            // one day before the start of the first hole.
            Iterator<Pair<Date,Date>> it = mergedHoles.iterator();
            Pair<Date,Date> firstHole = it.next();
            if (firstHole.getLeft().getTime() > originalBeginDate.getTime()) {
                subDateRanges.add(Pair.of(new Date(originalBeginDate.getTime()), oneDayBefore(firstHole.getLeft())));
            } else {
                subDateRanges.add(firstHole);
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
                    if (currentHole.getRight().getTime() > originalEndDate.getTime()) {
                        subDateRanges.add(Pair.of(currentHole.getLeft(), originalEndDate));
                    } else {
                        // If it does not fall outside the target date range, include it as is.
                        subDateRanges.add(currentHole);
                    }
                }
                endOfPrevHole = currentHole.getRight();
            }

            // If the last hole we saw ended before the end of the original query's target date range, add a target date range from one day after the end of the
            // last hole to the original target end date.
            if (endOfPrevHole.getTime() < originalEndDate.getTime()) {
                subDateRanges.add(Pair.of(oneDayAfter(endOfPrevHole), new Date(originalEndDate.getTime())));
            }
        }

        return subDateRanges;
    }
    
    /**
     * Return the set of fields in the query.
     */
    private Set<String> getFieldsForQuery(String query, ScannerFactory scannerFactory) {
        // Parse the query.
        ASTJexlScript queryTree = originalPlanner.parseQueryAndValidatePattern(query, null);
    
        // Apply the query model.
        MetadataHelper metadataHelper = originalPlanner.getMetadataHelper();
        QueryModel queryModel = originalPlanner.loadQueryModel(originalConfig);
        queryTree = originalPlanner.applyQueryModel(metadataHelper, originalConfig, queryTree, queryModel);
        
        //Expand unfielded terms.
        ShardQueryConfiguration configCopy = new ShardQueryConfiguration(originalConfig);
        try {
            configCopy.setIndexedFields(metadataHelper.getIndexedFields(originalConfig.getDatatypeFilter()));
            configCopy.setReverseIndexedFields(metadataHelper.getReverseIndexedFields(originalConfig.getDatatypeFilter()));
            queryTree = UnfieldedIndexExpansionVisitor.expandUnfielded(configCopy, scannerFactory, metadataHelper, queryTree);
        } catch (TableNotFoundException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.METADATA_ACCESS_ERROR, e);
            log.info(qe);
            throw new DatawaveFatalQueryException(qe);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new DatawaveFatalQueryException(e);
        }
    
        // Extract and return the fields from the query.
        return QueryFieldsVisitor.parseQueryFields(queryTree, metadataHelper);
    }
    
    /**
     * Return the set of any field index hole date ranges that fall within the original query's target date range.
     */
    private SortedSet<Pair<Date,Date>> getHolesWithinOriginalQueryDateRange(FieldIndexHole fieldIndexHole) {
        SortedSet<Pair<Date,Date>> holes = fieldIndexHole.getDateRanges();
        // If the earliest date range falls after the original query date range, or the latest date range falls before the original query range, then none of
        // the holes fall within the date range.
        if (isOutsideTargetDates(holes.first(), holes.last())) {
            return Collections.emptySortedSet();
        }

        // There is at least one index hole that falls within the original query date range. Collect and return them.
        return holes.stream().filter(this::isWithinTargetDates).collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Return whether the given date ranges representing the earliest and latest date ranges respectively do not encompass any dates that could fall within the
     */
    private boolean isOutsideTargetDates(Pair<Date,Date> earliestRange, Pair<Date,Date> latestRange) {
        return earliestRange.getLeft().getTime() > originalBeginDate.getTime() || latestRange.getRight().getTime() < originalEndDate.getTime();
    }

    /**
     * Return whether any dates in the given date range fall within the original query's target date range, inclusively.
     */
    private boolean isWithinTargetDates(Pair<Date,Date> dateRange) {
        return isWithinTargetDates(dateRange.getLeft()) || isWithinTargetDates(dateRange.getRight());
    }

    /**
     * Return whether the given date falls within the start and end date of the original query's target date range, inclusively.
     */
    private boolean isWithinTargetDates(Date date) {
        return date.getTime() >= originalBeginDate.getTime() && date.getTime() <= originalEndDate.getTime();
    }

    /**
     * Merge all overlapping date ranges in the given set and return them.
     */
    private SortedSet<Pair<Date,Date>> mergeOverlappingRanges(SortedSet<Pair<Date,Date>> ranges) {
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
            if (curr.getLeft().getTime() <= prev.getRight().getTime()) {
                // If the current date range's start date is equal to or before the end date of the previous date range, replace the previous date range
                // new date range that spans both date ranges.
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

    @Override
    public long maxRangesPerQueryPiece() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close(GenericQueryConfiguration config, Query settings) {
        // Nothing to do.
    }

    @Override
    public void setQueryIteratorClass(Class<? extends SortedKeyValueIterator<Key,Value>> clazz) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends SortedKeyValueIterator<Key,Value>> getQueryIteratorClass() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getPlannedScript() {
        return plannedScript;
    }

    @Override
    public FederatedQueryPlanner clone() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRules(Collection<PushDownRule> rules) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<PushDownRule> getRules() {
        return Collections.emptyList();
    }

    @Override
    public ASTJexlScript applyRules(ASTJexlScript queryTree, ScannerFactory scannerFactory, MetadataHelper metadataHelper, ShardQueryConfiguration config) {
        return null;
    }
}
