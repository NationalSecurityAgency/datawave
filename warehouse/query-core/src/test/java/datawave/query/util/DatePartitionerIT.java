package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import datawave.query.exceptions.DatawaveFatalQueryException;
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
        // BUG: IndexFieldHole.getDateRanges() is empty here, and getHolesOverlappingOriginalQueryDateRange calls holes.first()/holes.last() without an
        // isEmpty() guard, so this throws a raw NoSuchElementException instead of a controlled query exception.
        Date begin = startOfDay("20130101");
        Date end = endOfDay("20130105");
        Map<String,IndexFieldHole> byDatatype = new HashMap<>();
        byDatatype.put("dt", new IndexFieldHole("F", "dt", Collections.emptySortedSet()));
        Map<String,Map<String,IndexFieldHole>> holes = new HashMap<>();
        holes.put("F", byDatatype);

        assertThrows(NoSuchElementException.class, () -> DatePartitioner.partition(holes, begin, end));
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
         * BUG: adjacent cross-datatype holes for the same field should merge into one range; collapseDatatypes only merges *overlapping* ranges (a start
         * boundary must fall on or before a preceding end boundary), so an adjacent-but-not-overlapping pair leaves two separate hole ranges. Because they are
         * adjacent, the gap between them is zero-length and gets skipped by the zero-length guard in the boundary reduction loop, so the two remaining ranges
         * end up back-to-back with an identical unindexed-field set {F}. ensureConsistency's matchingFieldSetsFound check then throws.
         */
        @Test
        void holesAdjacentAcrossDatatypes() {
            Date begin = startOfDay("20130101");
            Date end = endOfDay("20130105");
            Map<String,Map<String,IndexFieldHole>> holes = twoDatatypeHoles("F", "datatype-a", new String[] {"20130101", "20130102"}, "datatype-b",
                            new String[] {"20130103", "20130105"});

            assertThrows(DatawaveFatalQueryException.class, () -> DatePartitioner.partition(holes, begin, end));
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
