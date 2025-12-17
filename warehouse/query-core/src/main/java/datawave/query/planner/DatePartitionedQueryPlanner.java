package datawave.query.planner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
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
import datawave.query.index.lookup.UidIntersector;
import datawave.query.jexl.visitors.QueryFieldsVisitor;
import datawave.query.model.IndexFieldHole;
import datawave.query.planner.pushdown.rules.PushDownRule;
import datawave.query.tables.ScannerFactory;
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
     *             if an exception occurs
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

        // now let's do the initial planning
        DefaultQueryPlanner initialPlanner = this.queryPlanner.clone();
        initialPlanner.process(shardQueryConfig, query, settings, scannerFactory);

        // Our initial plan and planned script will both be the initial planned script
        this.initialPlan = this.plannedScript = initialPlanner.getPlannedScript();

        // and reset the expansion flags to what we had previously
        shardQueryConfig.setGeneratePlanOnly(generatePlanOnly);
        shardQueryConfig.setExpandValues(expandValues);
        shardQueryConfig.setDeferPushdownPullup(deferPushdownPullup);

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

        // If no field index holes were found, we can return early with the original query date range.
        if (fieldIndexHolesByDatatype.isEmpty()) {
            log.debug("No field index holes found for query fields");
            SortedMap<Pair<Date,Date>,Set<String>> fullTimeline = new TreeMap<>();
            Pair<Date,Date> range = Pair.of(config.getBeginDate(), config.getEndDate());
            fullTimeline.put(range, Collections.emptySet());
            return fullTimeline;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Field index holes found for query fields " + fieldIndexHolesByDatatype.keySet());
            }
        }

        // first lets merge the datatypes in this list. If one datatype has a hole for a field, then consider it a hole for all datatypes
        Map<String,IndexFieldHole> fieldIndexHoles = collapseDatatypes(fieldIndexHolesByDatatype);

        // Now create a timeline of index segments from begin date to end date
        SortedSet<IndexFieldHoleBoundary> timeline = createTimeline(fieldIndexHoles, config.getBeginDate(), config.getEndDate());

        // if we found no holes that overlapped our date range, then we are done
        if (timeline.isEmpty()) {
            log.debug("No field index holes overlapping query range found");
            return null;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Timeline contains " + timeline.size() + " boundaries to be examined");
            }
        }

        // now scan through the timeline building ranges and the set of fields that are unindexed for each one
        SortedMap<Pair<Date,Date>,Set<String>> reducedTimeline = new TreeMap<>();
        Set<String> unindexedFields = new HashSet<>();
        IndexFieldHoleBoundary last = null;
        for (IndexFieldHoleBoundary next : timeline) {
            if (last != null) {
                Date start = last.getBoundary();
                if (!last.isStart()) {
                    start = oneMsAfter(start);
                }
                Date end = next.getBoundary();
                if (next.isStart()) {
                    end = oneMsBefore(end);
                }
                // if we had one index hole that butted up against another index hole,
                // then we may find ourselves with a zero length range
                if (start.compareTo(end) <= 0) {
                    Pair<Date,Date> range = Pair.of(start, end);
                    reducedTimeline.put(range, new HashSet<>(unindexedFields));
                }
            }
            // update the set of unindexed fields depending on whether we are starting or ending a hole
            if (next.hasField()) {
                if (next.isStart()) {
                    unindexedFields.add(next.getField());
                } else {
                    unindexedFields.remove(next.getField());
                }
            }
            last = next;
        }

        // If debug is enabled, log the date ranges to be queried over in formatted form.
        if (log.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            Iterator<Map.Entry<Pair<Date,Date>,Set<String>>> it = reducedTimeline.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Pair<Date,Date>,Set<String>> range = it.next();
                Pair<Date,Date> dateRange = range.getKey();
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(dateFormat.format(dateRange.getLeft())).append("-").append(dateFormat.format(dateRange.getRight())).append(':')
                                .append(range.getValue());
            }
            log.debug(reducedTimeline.size() + " sub-queries will be executed over date ranges: " + sb);
        }

        ensureConsistency(reducedTimeline, config.getBeginDate(), config.getEndDate());

        return reducedTimeline;
    }

    /**
     * This method is intended to ensure some fault tolerance in out production of the timeline. The date range from beginDate to endDate should be completely
     * covered, there should be no gaps, no overlapping date ranges, no negative length date ranges, and every date range should have a different set of
     * unindexed fields.
     *
     * @param timeline
     *            The timeline to verify
     * @param beginDate
     *            The begin date
     * @param endDate
     *            The end date
     */
    private void ensureConsistency(SortedMap<Pair<Date,Date>,Set<String>> timeline, Date beginDate, Date endDate) throws DatawaveFatalQueryException {
        boolean beginDateValidated = timeline.firstKey().getLeft().equals(beginDate);
        boolean endDateValidated = timeline.lastKey().getRight().equals(endDate);

        boolean unsortedRangesFound = false;
        boolean gapsFound = false;
        boolean overlapsFound = false;
        boolean matchingFieldSetsFound = false;

        Map.Entry<Pair<Date,Date>,Set<String>> last = null;
        for (Map.Entry<Pair<Date,Date>,Set<String>> next : timeline.entrySet()) {
            Date begin = next.getKey().getLeft();
            Date end = next.getKey().getRight();
            if (begin.after(end)) {
                unsortedRangesFound = true;
            }
            if (last != null) {
                Date lastEnd = last.getKey().getRight();
                Date expectedBegin = oneMsAfter(lastEnd);
                if (begin.before(expectedBegin)) {
                    overlapsFound = true;
                } else if (begin.after(expectedBegin)) {
                    gapsFound = true;
                }
                if (last.getValue().equals(next.getValue())) {
                    matchingFieldSetsFound = true;
                }
            }
            last = next;
        }

        if (!beginDateValidated || !endDateValidated || unsortedRangesFound || gapsFound || overlapsFound || matchingFieldSetsFound) {
            StringBuilder msg = new StringBuilder();
            msg.append("Ranges inconsistent for date range ").append(beginDate).append(", ").append(endDate);
            msg.append("; begin:").append(beginDateValidated);
            msg.append("; end:").append(endDateValidated);
            msg.append("; unsorted:").append(unsortedRangesFound);
            msg.append("; gaps:").append(gapsFound);
            msg.append("; overlaps:").append(overlapsFound);
            msg.append("; matching:").append(matchingFieldSetsFound);
            msg.append("; ").append(timeline);
            log.error(msg);
            throw new DatawaveFatalQueryException(msg.toString());
        }

    }

    /**
     * Collapse the datatypes such that if one datatype is unindexed for a field, then consider them all unindexed
     *
     * @param fieldIndexHolesByDatatype
     * @return The map of fields to their index holes (datatype agnostic)
     */
    private Map<String,IndexFieldHole> collapseDatatypes(Map<String,Map<String,IndexFieldHole>> fieldIndexHolesByDatatype) {
        Map<String,IndexFieldHole> collapsedDatatypes = new HashMap<>();

        // to do this, create a timeline of boundaries and then collapse consecutive begins and consecutive ends for each field
        for (Map.Entry<String,Map<String,IndexFieldHole>> holes : fieldIndexHolesByDatatype.entrySet()) {
            String field = holes.getKey();
            SortedSet<IndexFieldHoleBoundary> boundaries = new TreeSet<>();
            for (IndexFieldHole hole : holes.getValue().values()) {
                for (Pair<Date,Date> range : hole.getDateRanges()) {
                    boundaries.add(new IndexFieldHoleBoundary(range.getLeft(), true, field));
                    boundaries.add(new IndexFieldHoleBoundary(range.getRight(), false, field));
                }
            }
            SortedSet<Pair<Date,Date>> collapsedRanges = new TreeSet<>();
            Date lastStart = null;
            Date lastEnd = null;
            for (IndexFieldHoleBoundary next : boundaries) {
                if (next.isStart()) {
                    if (lastEnd != null) {
                        collapsedRanges.add(Pair.of(lastStart, lastEnd));
                        lastStart = null;
                        lastEnd = null;
                    }
                    // retain only the first date in a series of starts
                    if (lastStart == null) {
                        lastStart = next.getBoundary();
                    }
                } else {
                    // retain the last date in a series of ends
                    lastEnd = next.getBoundary();
                }
            }
            if (lastEnd != null) {
                collapsedRanges.add(Pair.of(lastStart, lastEnd));
            }
            collapsedDatatypes.put(field, new IndexFieldHole(field, null, collapsedRanges));
        }

        return collapsedDatatypes;
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
     * Take a map of field to index field holes (datatype agnostic), and return a sorted timeline of boundaries which are the start and end of the index holes
     *
     * @param fieldIndexHoles
     * @param beginDate
     * @param endDate
     * @return a timeline of index field hole boundaries
     */
    private SortedSet<IndexFieldHoleBoundary> createTimeline(Map<String,IndexFieldHole> fieldIndexHoles, Date beginDate, Date endDate) {
        // We want to create a timeline of index hole begin and end dates
        // that overlap the query's target date range
        // and map to the fields for which holes are beginning and ending
        SortedSet<IndexFieldHoleBoundary> timeline = new TreeSet<>();
        for (Map.Entry<String,IndexFieldHole> hole : fieldIndexHoles.entrySet()) {
            String field = hole.getKey();
            IndexFieldHole indexHole = hole.getValue();
            for (Pair<Date,Date> range : getHolesOverlappingOriginalQueryDateRange(beginDate, endDate, indexHole)) {
                timeline.add(new IndexFieldHoleBoundary(range.getLeft(), true, field));
                timeline.add(new IndexFieldHoleBoundary(range.getRight(), false, field));
            }
        }
        if (timeline.isEmpty()) {
            timeline.add(new IndexFieldHoleBoundary(beginDate, true));
            timeline.add(new IndexFieldHoleBoundary(endDate, false));
        } else {
            if (timeline.first().getBoundary().after(beginDate)) {
                // start with a beginning boundary sans field at the beginDate
                timeline.add(new IndexFieldHoleBoundary(beginDate, true));
            }
            // add an artificial end boundary if the end date of the query is not covered
            if (timeline.last().getBoundary().before(endDate)) {
                timeline.add(new IndexFieldHoleBoundary(endDate, false));
            }
        }
        return timeline;
    }

    /**
     * Return the set of fields in the query.
     */
    protected Set<String> getFieldsForQuery(ASTJexlScript queryTree, MetadataHelper metadataHelper) {
        // Extract and return the fields from the query.
        return QueryFieldsVisitor.parseQueryFields(queryTree, metadataHelper);
    }

    /**
     * Return the set of any field index hole date ranges that fall within the original query's target date range.
     */
    private SortedSet<Pair<Date,Date>> getHolesOverlappingOriginalQueryDateRange(Date beginDate, Date endDate, IndexFieldHole fieldIndexHole) {
        SortedSet<Pair<Date,Date>> holes = fieldIndexHole.getDateRanges();
        // If the earliest date range falls after the original query date range, or the latest date range falls before the original query range, then none of
        // the holes fall within the date range.
        if (isOutsideDateRange(beginDate, endDate, holes.first(), holes.last())) {
            return Collections.emptySortedSet();
        }

        // There is at least one index hole that falls within the original query date range. Collect and return them.
        return holes.stream().filter((range) -> isOverlappingDateRange(beginDate, endDate, range))
                        .map(range -> Pair.of(max(beginDate, range.getLeft()), min(endDate, range.getRight()))).collect(Collectors.toCollection(TreeSet::new));
    }

    private Date max(Date d1, Date d2) {
        return (d1.compareTo(d2) >= 0 ? d1 : d2);
    }

    private Date min(Date d1, Date d2) {
        return d1.compareTo(d2) <= 0 ? d1 : d2;
    }

    /**
     * Return whether the given date ranges overlap
     */
    private boolean isOverlappingDateRange(Date beginDate, Date endDate, Pair<Date,Date> range) {
        return range.getLeft().getTime() <= endDate.getTime() && range.getRight().getTime() >= beginDate.getTime();
    }

    /**
     * Return whether the given date ranges representing the earliest and latest date ranges respectively do not encompass any dates that could fall within the
     */
    private boolean isOutsideDateRange(Date beginDate, Date endDate, Pair<Date,Date> earliestRange, Pair<Date,Date> latestRange) {
        return earliestRange.getLeft().getTime() > endDate.getTime() || latestRange.getRight().getTime() < beginDate.getTime();
    }

    /**
     * Return one millisecond after the given date.
     */
    private Date oneMsAfter(Date date) {
        return new Date(date.getTime() + 1);
    }

    /**
     * Return one millisecond before the given date.
     */
    private Date oneMsBefore(Date date) {
        return new Date(date.getTime() - 1);
    }

    /**
     * This class represents the start or end of a range where a field is unindexed. If the field is null, then it represents an artificial boundary at the
     * start or end of the query range.
     */
    public static class IndexFieldHoleBoundary implements Comparable<IndexFieldHoleBoundary> {
        private final Date date;
        private final boolean start;
        private final String field;

        public IndexFieldHoleBoundary(Date date, boolean start, String field) {
            this.date = date;
            this.start = start;
            this.field = field;
        }

        public IndexFieldHoleBoundary(Date date, boolean start) {
            this.date = date;
            this.start = start;
            this.field = null;
        }

        public Date getBoundary() {
            return date;
        }

        public boolean isStart() {
            return start;
        }

        public boolean hasField() {
            return field != null;
        }

        public String getField() {
            return field;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(date).append(start).append(field).toHashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof IndexFieldHoleBoundary) {
                IndexFieldHoleBoundary other = (IndexFieldHoleBoundary) o;
                return new EqualsBuilder().append(date, other.date).append(start, other.start).append(field, other.field).isEquals();
            }
            return false;
        }

        @Override
        public int compareTo(IndexFieldHoleBoundary other) {
            int comparison = date.compareTo(other.date);
            if (comparison == 0) {
                comparison = Boolean.compare(other.start, start);
            }
            if (comparison == 0) {
                comparison = String.valueOf(field).compareTo(String.valueOf(other.field));
            }
            return comparison;
        }
    }

}
