package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import datawave.query.model.IndexFieldHole;
import datawave.util.time.DateHelper;

/**
 * Unit tests for {@link DatePartitioner}. No Accumulo, no Spring - each test hand-builds a field/datatype to {@link IndexFieldHole} map and asserts the
 * resulting sub-range partition.
 */
class DatePartitionerIT {

    private static final long DAY_MILLIS = 24L * 60 * 60 * 1000;

    private static Date startOfDay(String yyyyMMdd) {
        return DateHelper.parse(yyyyMMdd);
    }

    private static Date endOfDay(String yyyyMMdd) {
        return new Date(startOfDay(yyyyMMdd).getTime() + DAY_MILLIS - 1);
    }

    /** Build an {@link IndexFieldHole} spanning the given inclusive day range(s), e.g. {@code hole("F", "dt", "20130102", "20130103")}. */
    private static IndexFieldHole hole(String field, String datatype, String... dayRangePairs) {
        SortedSet<Pair<Date,Date>> ranges = new TreeSet<>();
        for (int i = 0; i < dayRangePairs.length; i += 2) {
            ranges.add(Pair.of(startOfDay(dayRangePairs[i]), startOfDay(dayRangePairs[i + 1])));
        }
        return new IndexFieldHole(field, datatype, ranges);
    }

    /** Build a field-to-datatype-to-holes map with a single field having holes in a single datatype. */
    private static Map<String,Map<String,IndexFieldHole>> holesFor(String field, String datatype, String... dayRangePairs) {
        Map<String,IndexFieldHole> byDatatype = new HashMap<>();
        byDatatype.put(datatype, hole(field, datatype, dayRangePairs));
        Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
        holes.put(field, byDatatype);
        return holes;
    }

    private static Set<String> fields(String... fields) {
        return new HashSet<>(List.of(fields));
    }

    private static final class RangeExpectation {
        private final Date start;
        private final Date end;
        private final Set<String> fields;

        private RangeExpectation(Date start, Date end, Set<String> fields) {
            this.start = start;
            this.end = end;
            this.fields = fields;
        }
    }

    private static RangeExpectation range(Date start, Date end, String... unindexedFields) {
        return new RangeExpectation(start, end, fields(unindexedFields));
    }

    private static void assertPartition(SortedMap<Pair<Date,Date>,Set<String>> actual, RangeExpectation... expected) {
        assertEquals(expected.length, actual.size(), () -> "expected " + expected.length + " ranges but got: " + actual);
        List<Map.Entry<Pair<Date,Date>,Set<String>>> entries = new ArrayList<>(actual.entrySet());
        for (int i = 0; i < expected.length; i++) {
            Map.Entry<Pair<Date,Date>,Set<String>> actualEntry = entries.get(i);
            RangeExpectation exp = expected[i];
            assertEquals(exp.start, actualEntry.getKey().getLeft(), "start of range " + i);
            assertEquals(exp.end, actualEntry.getKey().getRight(), "end of range " + i);
            assertEquals(exp.fields, actualEntry.getValue(), "unindexed fields of range " + i);
        }
    }

    @Test
    void noHolesForQueryFields() {
        Date begin = startOfDay("20130101");
        Date end = endOfDay("20130105");

        SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(Collections.emptyMap(), begin, end);

        // @formatter:off
        assertPartition(result,
                        range(begin, end));
        // @formatter:on
    }

    @Test
    void singleHoleMidRange() {
        Date begin = startOfDay("20130101");
        Date end = endOfDay("20130105");
        Map<String,Map<String,IndexFieldHole>> holes = holesFor("F", "dt", "20130103", "20130103");

        SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

        // @formatter:off
        assertPartition(result,
                        range(begin, new Date(startOfDay("20130103").getTime() - 1)),
                        range(startOfDay("20130103"), endOfDay("20130103"), "F"),
                        range(new Date(endOfDay("20130103").getTime() + 1), end));
        // @formatter:on
    }

    @Test
    void holeAtRangeStart() {
        Date begin = startOfDay("20130101");
        Date end = endOfDay("20130105");
        Map<String,Map<String,IndexFieldHole>> holes = holesFor("F", "dt", "20130101", "20130102");

        SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

        // @formatter:off
        assertPartition(result,
                        range(begin, endOfDay("20130102"), "F"),
                        range(new Date(endOfDay("20130102").getTime() + 1), end));
        // @formatter:on
    }

    @Test
    void holeAtRangeEnd() {
        Date begin = startOfDay("20130101");
        Date end = endOfDay("20130105");
        Map<String,Map<String,IndexFieldHole>> holes = holesFor("F", "dt", "20130104", "20130105");

        SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

        // @formatter:off
        assertPartition(result,
                        range(begin, new Date(startOfDay("20130104").getTime() - 1)),
                        range(startOfDay("20130104"), end, "F"));
        // @formatter:on
    }

    @Test
    void holeClippedAtBothEnds() {
        Date begin = startOfDay("20130102");
        Date end = endOfDay("20130104");
        // hole is wider than the query range on both sides
        Map<String,Map<String,IndexFieldHole>> holes = holesFor("F", "dt", "20130101", "20130106");

        SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

        // @formatter:off
        assertPartition(result,
                        range(begin, end, "F"));
        // @formatter:on
    }

    @Test
    void holeCoversEntireQueryRange() {
        Date begin = startOfDay("20130101");
        Date end = endOfDay("20130105");
        Map<String,Map<String,IndexFieldHole>> holes = holesFor("F", "dt", "20130101", "20130105");

        SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

        // @formatter:off
        assertPartition(result,
                        range(begin, end, "F"));
        // @formatter:on
    }

    @Test
    void holesOutsideQueryRange() {
        Date begin = startOfDay("20130101");
        Date end = endOfDay("20130103");
        // the hole falls entirely after the query range
        Map<String,Map<String,IndexFieldHole>> holes = holesFor("F", "dt", "20130110", "20130111");

        SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

        // @formatter:off
        assertPartition(result,
                        range(begin, end));
        // @formatter:on
    }

    @Test
    void beginDateEqualsEndDate() {
        Date begin = startOfDay("20130101");
        Date end = endOfDay("20130101");

        SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(Collections.emptyMap(), begin, end);

        // @formatter:off
        assertPartition(result,
                        range(begin, end));
        // @formatter:on
    }

    @Test
    void nestedHoles() {
        Date begin = startOfDay("20130101");
        Date end = endOfDay("20130105");
        Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
        holes.putAll(holesFor("F", "dt", "20130101", "20130105"));
        Map<String,IndexFieldHole> gByDatatype = new HashMap<>();
        gByDatatype.put("dt", hole("G", "dt", "20130102", "20130104"));
        holes.put("G", gByDatatype);

        SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

        // @formatter:off
        assertPartition(result,
                        range(begin, new Date(startOfDay("20130102").getTime() - 1), "F"),
                        range(startOfDay("20130102"), endOfDay("20130104"), "F", "G"),
                        range(new Date(endOfDay("20130104").getTime() + 1), end, "F"));
        // @formatter:on
    }

    @Test
    void identicalHolesForTwoFields() {
        Date begin = startOfDay("20130101");
        Date end = endOfDay("20130105");
        Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
        holes.putAll(holesFor("F", "dt", "20130102", "20130103"));
        Map<String,IndexFieldHole> gByDatatype = new HashMap<>();
        gByDatatype.put("dt", hole("G", "dt", "20130102", "20130103"));
        holes.put("G", gByDatatype);

        SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

        // @formatter:off
        assertPartition(result,
                        range(begin, new Date(startOfDay("20130102").getTime() - 1)),
                        range(startOfDay("20130102"), endOfDay("20130103"), "F", "G"),
                        range(new Date(endOfDay("20130103").getTime() + 1), end));
        // @formatter:on
    }

    /**
     * Adjacent holes for <em>different</em> fields must stay separate - only same-field holes are merged. The resulting back-to-back ranges are legal because
     * their unindexed-field sets differ.
     */
    @Test
    void adjacentHolesForDifferentFields() {
        Date begin = startOfDay("20130101");
        Date end = endOfDay("20130105");
        Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
        holes.putAll(holesFor("F", "dt", "20130101", "20130102"));
        Map<String,IndexFieldHole> gByDatatype = new HashMap<>();
        gByDatatype.put("dt", hole("G", "dt", "20130103", "20130104"));
        holes.put("G", gByDatatype);

        SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

        // @formatter:off
        assertPartition(result,
                        range(begin, endOfDay("20130102"), "F"),
                        range(startOfDay("20130103"), endOfDay("20130104"), "G"),
                        range(new Date(endOfDay("20130104").getTime() + 1), end));
        // @formatter:on
    }

    /**
     * Two adjacent date ranges within a single field/datatype's own hole set are merged, just as they are across datatypes.
     */
    @Test
    void adjacentRangesWithinOneDatatype() {
        Date begin = startOfDay("20130101");
        Date end = endOfDay("20130106");
        Map<String,Map<String,IndexFieldHole>> holes = holesFor("F", "dt", "20130102", "20130103", "20130104", "20130105");

        SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

        // @formatter:off
        assertPartition(result,
                        range(begin, new Date(startOfDay("20130102").getTime() - 1)),
                        range(startOfDay("20130102"), endOfDay("20130105"), "F"),
                        range(new Date(endOfDay("20130105").getTime() + 1), end));
        // @formatter:on
    }

    @Test
    void threeFieldsInterleavedHoles() {
        Date begin = startOfDay("20130101");
        Date end = endOfDay("20130107");
        Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
        holes.putAll(holesFor("F", "dt", "20130101", "20130102"));
        Map<String,IndexFieldHole> gByDatatype = new HashMap<>();
        gByDatatype.put("dt", hole("G", "dt", "20130102", "20130104"));
        holes.put("G", gByDatatype);
        Map<String,IndexFieldHole> hByDatatype = new HashMap<>();
        hByDatatype.put("dt", hole("H", "dt", "20130104", "20130107"));
        holes.put("H", hByDatatype);

        SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

        // @formatter:off
        assertPartition(result,
                        range(begin, new Date(startOfDay("20130102").getTime() - 1), "F"),
                        range(startOfDay("20130102"), endOfDay("20130102"), "F", "G"),
                        range(new Date(startOfDay("20130102").getTime() + DAY_MILLIS), new Date(startOfDay("20130104").getTime() - 1), "G"),
                        range(startOfDay("20130104"), endOfDay("20130104"), "G", "H"),
                        range(new Date(startOfDay("20130104").getTime() + DAY_MILLIS), end, "H"));
        // @formatter:on
    }

    @Test
    void emptyDateRangesInIndexFieldHole() {
        // A hole entry with no date ranges contributes no boundaries, so the query range is returned whole with nothing unindexed.
        Date begin = startOfDay("20130101");
        Date end = endOfDay("20130105");
        Map<String,IndexFieldHole> byDatatype = new HashMap<>();
        byDatatype.put("dt", new IndexFieldHole("F", "dt", Collections.emptySortedSet()));
        Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
        holes.put("F", byDatatype);

        SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

        assertPartition(result, range(begin, end));
    }

    @Test
    void emptyDateRangesAlongsideRealHole() {
        // The empty hole entry is ignored, but a second field's real hole still partitions the range.
        Date begin = startOfDay("20130101");
        Date end = endOfDay("20130105");
        Map<String,IndexFieldHole> emptyByDatatype = new HashMap<>();
        emptyByDatatype.put("dt", new IndexFieldHole("F", "dt", Collections.emptySortedSet()));
        Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
        holes.put("F", emptyByDatatype);
        holes.putAll(holesFor("G", "dt", "20130102", "20130103"));

        SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

        // @formatter:off
        assertPartition(result,
                        range(begin, new Date(startOfDay("20130102").getTime() - 1)),
                        range(startOfDay("20130102"), endOfDay("20130103"), "G"),
                        range(new Date(endOfDay("20130103").getTime() + 1), end));
        // @formatter:on
    }

    @Test
    void everyRangeIsContiguous() {
        Date begin = startOfDay("20130101");
        Date end = endOfDay("20130107");
        Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
        holes.putAll(holesFor("F", "dt", "20130102", "20130103"));
        Map<String,IndexFieldHole> gByDatatype = new HashMap<>();
        gByDatatype.put("dt", hole("G", "dt", "20130105", "20130106"));
        holes.put("G", gByDatatype);

        SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

        assertEquals(begin, result.firstKey().getLeft());
        assertEquals(end, result.lastKey().getRight());
        Date previousEnd = null;
        for (Pair<Date,Date> r : result.keySet()) {
            assertTrue(r.getLeft().compareTo(r.getRight()) <= 0, () -> "range " + r + " is not start<=end");
            if (previousEnd != null) {
                assertEquals(previousEnd.getTime() + 1, r.getLeft().getTime(), "expected 1ms gap between ranges");
            }
            previousEnd = r.getRight();
        }
    }

    /**
     * Tests for {@code collapseDatatypes} - the least-tested part of the partitioner, since the previous fixture ({@code IndexFieldHoleDataIngest}) hardcoded a
     * single datatype. Every case here builds holes for the same field across two or more datatypes.
     */
    @Nested
    class CollapseDatatypesTests {

        private Map<String,Map<String,IndexFieldHole>> twoDatatypeHoles(String field, String datatypeA, String[] aRange, String datatypeB, String[] bRange) {
            Map<String,IndexFieldHole> byDatatype = new HashMap<>();
            byDatatype.put(datatypeA, hole(field, datatypeA, aRange));
            byDatatype.put(datatypeB, hole(field, datatypeB, bRange));
            Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
            holes.put(field, byDatatype);
            return holes;
        }

        @Test
        void holeInSingleDatatypeOnly() {
            Date begin = startOfDay("20130101");
            Date end = endOfDay("20130105");
            // datatype B has no hole entry at all for the field
            Map<String,IndexFieldHole> byDatatype = new HashMap<>();
            byDatatype.put("datatype-a", hole("F", "datatype-a", "20130102", "20130103"));
            Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
            holes.put("F", byDatatype);

            SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

            // the collapsed hole applies regardless of which datatype originated it
            // @formatter:off
            assertPartition(result,
                            range(begin, new Date(startOfDay("20130102").getTime() - 1)),
                            range(startOfDay("20130102"), endOfDay("20130103"), "F"),
                            range(new Date(endOfDay("20130103").getTime() + 1), end));
            // @formatter:on
        }

        @Test
        void holesOverlappingAcrossDatatypes() {
            Date begin = startOfDay("20130101");
            Date end = endOfDay("20130106");
            Map<String,Map<String,IndexFieldHole>> holes = twoDatatypeHoles("F", "datatype-a", new String[] {"20130101", "20130103"}, "datatype-b",
                            new String[] {"20130102", "20130105"});

            SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

            // @formatter:off
            assertPartition(result,
                            range(begin, endOfDay("20130105"), "F"),
                            range(new Date(endOfDay("20130105").getTime() + 1), end));
            // @formatter:on
        }

        @Test
        void holesIdenticalAcrossDatatypes() {
            Date begin = startOfDay("20130101");
            Date end = endOfDay("20130105");
            Map<String,Map<String,IndexFieldHole>> holes = twoDatatypeHoles("F", "datatype-a", new String[] {"20130102", "20130103"}, "datatype-b",
                            new String[] {"20130102", "20130103"});

            SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

            // @formatter:off
            assertPartition(result,
                            range(begin, new Date(startOfDay("20130102").getTime() - 1)),
                            range(startOfDay("20130102"), endOfDay("20130103"), "F"),
                            range(new Date(endOfDay("20130103").getTime() + 1), end));
            // @formatter:on
        }

        @Test
        void holesNestedAcrossDatatypes() {
            Date begin = startOfDay("20130101");
            Date end = endOfDay("20130105");
            Map<String,Map<String,IndexFieldHole>> holes = twoDatatypeHoles("F", "datatype-a", new String[] {"20130101", "20130105"}, "datatype-b",
                            new String[] {"20130102", "20130103"});

            SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

            // @formatter:off
            assertPartition(result,
                            range(begin, end, "F"));
            // @formatter:on
        }

        /**
         * An enclosing hole in one datatype spans two separated holes in others. The union is the enclosing hole, so the days between the inner holes must
         * remain unindexed: ending the merged range at an inner hole's end would report those days as indexed for a datatype that has no index covering them,
         * silently dropping its documents from the results.
         */
        @Test
        void holesNestedWithSeparatedInnerHoles() {
            Date begin = startOfDay("20130101");
            Date end = endOfDay("20130110");
            Map<String,IndexFieldHole> byDatatype = new HashMap<>();
            byDatatype.put("datatype-a", hole("F", "datatype-a", "20130101", "20130110"));
            byDatatype.put("datatype-b", hole("F", "datatype-b", "20130102", "20130103"));
            byDatatype.put("datatype-c", hole("F", "datatype-c", "20130105", "20130106"));
            Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
            holes.put("F", byDatatype);

            SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

            assertPartition(result, range(begin, end, "F"));
        }

        /**
         * The same nesting, but with the enclosing hole ending before the query range does. The merged range must still cover the enclosing hole in full, and
         * must not swallow the trailing indexed portion of the query range.
         */
        @Test
        void holesNestedWithSeparatedInnerHolesFollowedByIndexedRange() {
            Date begin = startOfDay("20130101");
            Date end = endOfDay("20130110");
            Map<String,IndexFieldHole> byDatatype = new HashMap<>();
            byDatatype.put("datatype-a", hole("F", "datatype-a", "20130101", "20130108"));
            byDatatype.put("datatype-b", hole("F", "datatype-b", "20130102", "20130103"));
            byDatatype.put("datatype-c", hole("F", "datatype-c", "20130105", "20130106"));
            Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
            holes.put("F", byDatatype);

            SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

            // @formatter:off
            assertPartition(result,
                            range(begin, endOfDay("20130108"), "F"),
                            range(startOfDay("20130109"), end));
            // @formatter:on
        }

        /**
         * Nesting must not merge holes that are genuinely disjoint from the enclosing one: a hole starting more than a day after the enclosing hole ends still
         * gets its own range.
         */
        @Test
        void holesNestedThenDisjointAcrossDatatypes() {
            Date begin = startOfDay("20130101");
            Date end = endOfDay("20130112");
            Map<String,IndexFieldHole> byDatatype = new HashMap<>();
            byDatatype.put("datatype-a", hole("F", "datatype-a", "20130101", "20130108"));
            byDatatype.put("datatype-b", hole("F", "datatype-b", "20130103", "20130104"));
            byDatatype.put("datatype-c", hole("F", "datatype-c", "20130111", "20130112"));
            Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
            holes.put("F", byDatatype);

            SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

            // @formatter:off
            assertPartition(result,
                            range(begin, endOfDay("20130108"), "F"),
                            range(startOfDay("20130109"), new Date(startOfDay("20130111").getTime() - 1)),
                            range(startOfDay("20130111"), end, "F"));
            // @formatter:on
        }

        @Test
        void holesDisjointAcrossDatatypes() {
            Date begin = startOfDay("20130101");
            Date end = endOfDay("20130105");
            Map<String,Map<String,IndexFieldHole>> holes = twoDatatypeHoles("F", "datatype-a", new String[] {"20130101", "20130102"}, "datatype-b",
                            new String[] {"20130104", "20130105"});

            SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

            // @formatter:off
            assertPartition(result,
                            range(begin, endOfDay("20130102"), "F"),
                            range(new Date(endOfDay("20130102").getTime() + 1), new Date(startOfDay("20130104").getTime() - 1)),
                            range(startOfDay("20130104"), end, "F"));
            // @formatter:on
        }

        /**
         * Adjacent cross-datatype holes for the same field merge into one range. They are contiguous - the second starts 1ms after the first ends - so leaving
         * them separate would produce two back-to-back sub-ranges with the identical unindexed-field set {F}, which ensureConsistency rejects as fatal.
         */
        @Test
        void holesAdjacentAcrossDatatypes() {
            Date begin = startOfDay("20130101");
            Date end = endOfDay("20130105");
            Map<String,Map<String,IndexFieldHole>> holes = twoDatatypeHoles("F", "datatype-a", new String[] {"20130101", "20130102"}, "datatype-b",
                            new String[] {"20130103", "20130105"});

            SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

            assertPartition(result, range(begin, end, "F"));
        }

        /**
         * The merge of adjacent holes must not swallow the trailing indexed portion of the query range.
         */
        @Test
        void holesAdjacentAcrossDatatypesFollowedByIndexedRange() {
            Date begin = startOfDay("20130101");
            Date end = endOfDay("20130107");
            Map<String,Map<String,IndexFieldHole>> holes = twoDatatypeHoles("F", "datatype-a", new String[] {"20130101", "20130102"}, "datatype-b",
                            new String[] {"20130103", "20130105"});

            SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

            // @formatter:off
            assertPartition(result,
                            range(begin, endOfDay("20130105"), "F"),
                            range(new Date(endOfDay("20130105").getTime() + 1), end));
            // @formatter:on
        }

        /**
         * Three chained adjacent holes across three datatypes collapse into a single range.
         */
        @Test
        void holesChainAdjacentAcrossThreeDatatypes() {
            Date begin = startOfDay("20130101");
            Date end = endOfDay("20130107");
            Map<String,IndexFieldHole> byDatatype = new HashMap<>();
            byDatatype.put("datatype-a", hole("F", "datatype-a", "20130102", "20130103"));
            byDatatype.put("datatype-b", hole("F", "datatype-b", "20130104", "20130104"));
            byDatatype.put("datatype-c", hole("F", "datatype-c", "20130105", "20130106"));
            Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
            holes.put("F", byDatatype);

            SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

            // @formatter:off
            assertPartition(result,
                            range(begin, new Date(startOfDay("20130102").getTime() - 1)),
                            range(startOfDay("20130102"), endOfDay("20130106"), "F"),
                            range(new Date(endOfDay("20130106").getTime() + 1), end));
            // @formatter:on
        }

        /**
         * Holes one full day apart are not adjacent, so they must stay separate with an indexed gap between them.
         */
        @Test
        void holesOneDayApartAcrossDatatypesStaySeparate() {
            Date begin = startOfDay("20130101");
            Date end = endOfDay("20130106");
            Map<String,Map<String,IndexFieldHole>> holes = twoDatatypeHoles("F", "datatype-a", new String[] {"20130101", "20130102"}, "datatype-b",
                            new String[] {"20130104", "20130105"});

            SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

            // @formatter:off
            assertPartition(result,
                            range(begin, endOfDay("20130102"), "F"),
                            range(startOfDay("20130103"), new Date(startOfDay("20130104").getTime() - 1)),
                            range(startOfDay("20130104"), endOfDay("20130105"), "F"),
                            range(new Date(endOfDay("20130105").getTime() + 1), end));
            // @formatter:on
        }

        @Test
        void holesAcrossThreeDatatypes() {
            Date begin = startOfDay("20130101");
            Date end = endOfDay("20130107");
            Map<String,IndexFieldHole> byDatatype = new HashMap<>();
            byDatatype.put("datatype-a", hole("F", "datatype-a", "20130101", "20130102"));
            byDatatype.put("datatype-b", hole("F", "datatype-b", "20130102", "20130103"));
            byDatatype.put("datatype-c", hole("F", "datatype-c", "20130106", "20130107"));
            Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
            holes.put("F", byDatatype);

            SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

            // A and B chain-overlap and merge; C stays separate with a clean gap between
            // @formatter:off
            assertPartition(result,
                            range(begin, endOfDay("20130103"), "F"),
                            range(new Date(endOfDay("20130103").getTime() + 1), new Date(startOfDay("20130106").getTime() - 1)),
                            range(startOfDay("20130106"), end, "F"));
            // @formatter:on
        }

        @Test
        void differentFieldsInDifferentDatatypes() {
            Date begin = startOfDay("20130101");
            Date end = endOfDay("20130105");
            Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
            Map<String,IndexFieldHole> fByDatatype = new HashMap<>();
            fByDatatype.put("datatype-a", hole("F", "datatype-a", "20130102", "20130103"));
            holes.put("F", fByDatatype);
            Map<String,IndexFieldHole> gByDatatype = new HashMap<>();
            gByDatatype.put("datatype-b", hole("G", "datatype-b", "20130104", "20130105"));
            holes.put("G", gByDatatype);

            SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

            // F's hole ends Jan3 23:59:59.999 and G's hole starts Jan4 00:00:00.000 - adjacent with no gap, so there is no clean range between them
            // @formatter:off
            assertPartition(result,
                            range(begin, new Date(startOfDay("20130102").getTime() - 1)),
                            range(startOfDay("20130102"), endOfDay("20130103"), "F"),
                            range(startOfDay("20130104"), end, "G"));
            // @formatter:on
        }

        @Test
        void holeInDatatypeAOverlapsHoleForOtherFieldInDatatypeB() {
            Date begin = startOfDay("20130101");
            Date end = endOfDay("20130105");
            Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
            Map<String,IndexFieldHole> fByDatatype = new HashMap<>();
            fByDatatype.put("datatype-a", hole("F", "datatype-a", "20130102", "20130104"));
            holes.put("F", fByDatatype);
            Map<String,IndexFieldHole> gByDatatype = new HashMap<>();
            gByDatatype.put("datatype-b", hole("G", "datatype-b", "20130103", "20130105"));
            holes.put("G", gByDatatype);

            SortedMap<Pair<Date,Date>,Set<String>> result = DatePartitioner.partition(holes, begin, end);

            // @formatter:off
            assertPartition(result,
                            range(begin, new Date(startOfDay("20130102").getTime() - 1)),
                            range(startOfDay("20130102"), new Date(startOfDay("20130103").getTime() - 1), "F"),
                            range(startOfDay("20130103"), endOfDay("20130104"), "F", "G"),
                            range(new Date(endOfDay("20130104").getTime() + 1), end, "G"));
            // @formatter:on
        }
    }
}
