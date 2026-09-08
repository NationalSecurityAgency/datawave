package datawave.query.util;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.model.IndexFieldHole;

/**
 * Pure interval math for partitioning a query date range into sub-ranges based on field index holes. Given a map of field to datatype to
 * {@link IndexFieldHole}, and a query's begin and end dates, {@link #partition(Map, Date, Date)} returns a sorted map of date sub-ranges to the set of fields
 * that are unindexed for that sub-range. Each sub-range will have a consistent index state, meaning that within each sub-range all query fields are either
 * indexed or not-indexed across the entire range. The sub-ranges will completely cover the original query date range without gaps or overlaps.
 * <p>
 * This class has no dependency on {@link datawave.query.config.ShardQueryConfiguration} or {@link MetadataHelper}; it is extracted from
 * {@link datawave.query.planner.DatePartitionedQueryPlanner} purely for testability.
 */
public final class DatePartitioner {

    private static final Logger log = ThreadConfigurableLogger.getLogger(DatePartitioner.class);

    private DatePartitioner() {
        // enforce static access
    }

    /**
     * Return the set of date ranges that sub-queries should be created for. Each date range will have a consistent index state, meaning that within each date
     * range all query fields are either indexed or not-indexed across the entire range. It is expected that the date ranges will completely cover the original
     * query date range without gaps or overlaps.
     *
     * @param fieldIndexHolesByDatatype
     *            field to datatype to index field holes
     * @param beginDate
     *            the query's begin date
     * @param endDate
     *            the query's end date
     * @return a sorted map of date sub-ranges to the set of fields that are unindexed for that sub-range
     */
    public static SortedMap<Pair<Date,Date>,Set<String>> partition(Map<String,Map<String,IndexFieldHole>> fieldIndexHolesByDatatype, Date beginDate,
                    Date endDate) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");

        // If no field index holes were found, we can return early with the original query date range.
        if (fieldIndexHolesByDatatype.isEmpty()) {
            log.debug("No field index holes found for query fields");
            return fullRange(beginDate, endDate);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Field index holes found for query fields " + fieldIndexHolesByDatatype.keySet());
            }
        }

        // first lets merge the datatypes in this list. If one datatype has a hole for a field, then consider it a hole for all datatypes
        Map<String,IndexFieldHole> fieldIndexHoles = collapseDatatypes(fieldIndexHolesByDatatype);

        // Now create a timeline of index segments from begin date to end date
        SortedSet<IndexFieldHoleBoundary> timeline = createTimeline(fieldIndexHoles, beginDate, endDate);

        // if we found no holes that overlapped our date range, then we are done. createTimeline always synthesizes boundaries for the query range, so this
        // is defensive: callers rely on a non-null timeline that covers the whole query date range.
        if (timeline.isEmpty()) {
            log.debug("No field index holes overlapping query range found");
            return fullRange(beginDate, endDate);
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

        ensureConsistency(reducedTimeline, beginDate, endDate);

        return reducedTimeline;
    }

    /**
     * Return a timeline consisting of a single sub-range covering the entire query date range, with no unindexed fields.
     *
     * @param beginDate
     *            the query's begin date
     * @param endDate
     *            the query's end date
     * @return a timeline containing the one full-coverage sub-range
     */
    private static SortedMap<Pair<Date,Date>,Set<String>> fullRange(Date beginDate, Date endDate) {
        SortedMap<Pair<Date,Date>,Set<String>> fullTimeline = new TreeMap<>();
        fullTimeline.put(Pair.of(beginDate, endDate), Collections.emptySet());
        return fullTimeline;
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
    private static void ensureConsistency(SortedMap<Pair<Date,Date>,Set<String>> timeline, Date beginDate, Date endDate) throws DatawaveFatalQueryException {
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
     * Collapse the datatypes such that if one datatype is unindexed for a field, then consider them all unindexed. Each field's resulting hole set is the union
     * of its holes across every datatype: holes that overlap, nest inside one another, or are merely contiguous (separated by no more than the 1ms that
     * day-aligned ranges leave between them) are merged into a single range, and only a gap of at least a full day leaves two ranges separate.
     *
     * @param fieldIndexHolesByDatatype
     * @return The map of fields to their index holes (datatype agnostic)
     */
    private static Map<String,IndexFieldHole> collapseDatatypes(Map<String,Map<String,IndexFieldHole>> fieldIndexHolesByDatatype) {
        Map<String,IndexFieldHole> collapsedDatatypes = new HashMap<>();

        // to do this, merge each field's hole date ranges, across all of its datatypes, into their union
        for (Map.Entry<String,Map<String,IndexFieldHole>> holes : fieldIndexHolesByDatatype.entrySet()) {
            String field = holes.getKey();

            // gather the hole date ranges of every datatype for this field, ordered by start date
            SortedSet<Pair<Date,Date>> ranges = new TreeSet<>();
            for (IndexFieldHole hole : holes.getValue().values()) {
                ranges.addAll(hole.getDateRanges());
            }

            SortedSet<Pair<Date,Date>> collapsedRanges = new TreeSet<>();
            Date lastStart = null;
            Date lastEnd = null;
            for (Pair<Date,Date> range : ranges) {
                // Close out the pending range only if this hole starts strictly after the pending one ends, leaving a real gap between them. A hole that
                // starts within, or immediately (1ms) after, the pending range is contiguous with it and must be merged in: leaving two back-to-back
                // ranges for the same field would produce two sub-ranges with identical unindexed field sets, which ensureConsistency rejects as a fatal
                // error.
                if (lastEnd != null && range.getLeft().getTime() > oneMsAfter(lastEnd).getTime()) {
                    collapsedRanges.add(Pair.of(lastStart, lastEnd));
                    lastStart = null;
                    lastEnd = null;
                }
                // retain only the first date in a series of merged holes
                if (lastStart == null) {
                    lastStart = range.getLeft();
                }
                // Extend the pending range to the latest end seen so far rather than adopting this hole's end outright. A hole nested inside a longer one
                // ends first, and taking its end would drop the remainder of the enclosing hole from the merged range and wrongly report those days as
                // indexed for a datatype that has no index over them.
                lastEnd = lastEnd == null ? range.getRight() : max(lastEnd, range.getRight());
            }
            if (lastEnd != null) {
                collapsedRanges.add(Pair.of(lastStart, lastEnd));
            }
            collapsedDatatypes.put(field, new IndexFieldHole(field, null, collapsedRanges));
        }

        return collapsedDatatypes;
    }

    /**
     * Take a map of field to index field holes (datatype agnostic), and return a sorted timeline of boundaries which are the start and end of the index holes
     *
     * @param fieldIndexHoles
     * @param beginDate
     * @param endDate
     * @return a timeline of index field hole boundaries
     */
    private static SortedSet<IndexFieldHoleBoundary> createTimeline(Map<String,IndexFieldHole> fieldIndexHoles, Date beginDate, Date endDate) {
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
     * Return the set of any field index hole date ranges that fall within the original query's target date range.
     */
    private static SortedSet<Pair<Date,Date>> getHolesOverlappingOriginalQueryDateRange(Date beginDate, Date endDate, IndexFieldHole fieldIndexHole) {
        SortedSet<Pair<Date,Date>> holes = fieldIndexHole.getDateRanges();
        // A hole with no date ranges contributes no boundaries at all.
        if (holes.isEmpty()) {
            return Collections.emptySortedSet();
        }
        // If the earliest date range falls after the original query date range, or the latest date range falls before the original query range, then none
        // of the holes fall within the date range.
        if (isOutsideDateRange(beginDate, endDate, holes.first(), holes.last())) {
            return Collections.emptySortedSet();
        }

        // There is at least one index hole that falls within the original query date range. Collect and return them.
        return holes.stream().filter((range) -> isOverlappingDateRange(beginDate, endDate, range))
                        .map(range -> Pair.of(max(beginDate, range.getLeft()), min(endDate, range.getRight()))).collect(Collectors.toCollection(TreeSet::new));
    }

    private static Date max(Date d1, Date d2) {
        return (d1.compareTo(d2) >= 0 ? d1 : d2);
    }

    private static Date min(Date d1, Date d2) {
        return d1.compareTo(d2) <= 0 ? d1 : d2;
    }

    /**
     * Return whether the given date ranges overlap
     */
    private static boolean isOverlappingDateRange(Date beginDate, Date endDate, Pair<Date,Date> range) {
        return range.getLeft().getTime() <= endDate.getTime() && range.getRight().getTime() >= beginDate.getTime();
    }

    /**
     * Return whether the given date ranges representing the earliest and latest date ranges respectively do not encompass any dates that could fall within the
     */
    private static boolean isOutsideDateRange(Date beginDate, Date endDate, Pair<Date,Date> earliestRange, Pair<Date,Date> latestRange) {
        return earliestRange.getLeft().getTime() > endDate.getTime() || latestRange.getRight().getTime() < beginDate.getTime();
    }

    /**
     * Return one millisecond after the given date.
     */
    private static Date oneMsAfter(Date date) {
        return new Date(date.getTime() + 1);
    }

    /**
     * Return one millisecond before the given date.
     */
    private static Date oneMsBefore(Date date) {
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
