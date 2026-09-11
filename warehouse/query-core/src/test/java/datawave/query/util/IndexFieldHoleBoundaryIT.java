package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import datawave.query.util.DatePartitioner.IndexFieldHoleBoundary;
import datawave.util.time.DateHelper;

/**
 * Unit tests for {@link IndexFieldHoleBoundary#compareTo(IndexFieldHoleBoundary)}, {@code equals}, and {@code hashCode}.
 */
class IndexFieldHoleBoundaryIT {

    private static final Date DAY_1 = DateHelper.parse("20130101");
    private static final Date DAY_2 = DateHelper.parse("20130102");

    @Test
    void earlierDateSortsFirst() {
        IndexFieldHoleBoundary earlier = new IndexFieldHoleBoundary(DAY_1, true, "F");
        IndexFieldHoleBoundary later = new IndexFieldHoleBoundary(DAY_2, true, "F");

        assertTrue(earlier.compareTo(later) < 0);
        assertTrue(later.compareTo(earlier) > 0);
    }

    @Test
    void sameDateStartSortsBeforeEnd() {
        IndexFieldHoleBoundary start = new IndexFieldHoleBoundary(DAY_1, true, "F");
        IndexFieldHoleBoundary end = new IndexFieldHoleBoundary(DAY_1, false, "F");

        assertTrue(start.compareTo(end) < 0);
        assertTrue(end.compareTo(start) > 0);
    }

    @Test
    void sameDateAndStartFlagSortsByFieldName() {
        IndexFieldHoleBoundary aField = new IndexFieldHoleBoundary(DAY_1, true, "A");
        IndexFieldHoleBoundary bField = new IndexFieldHoleBoundary(DAY_1, true, "B");

        assertTrue(aField.compareTo(bField) < 0);
        assertTrue(bField.compareTo(aField) > 0);
    }

    @Test
    void nullFieldSortsAfterUppercaseFieldNames() {
        // String.valueOf(null) == "null", and uppercase letters sort before lowercase 'n', so any all-uppercase field name sorts before a null field.
        IndexFieldHoleBoundary namedField = new IndexFieldHoleBoundary(DAY_1, true, "UUID");
        IndexFieldHoleBoundary noField = new IndexFieldHoleBoundary(DAY_1, true);

        assertTrue(namedField.compareTo(noField) < 0, "expected \"UUID\" to sort before \"null\"");
        assertTrue(noField.compareTo(namedField) > 0);
    }

    @Test
    void sortingOrdersByDateThenEndBeforeStartThenField() {
        IndexFieldHoleBoundary day2Start = new IndexFieldHoleBoundary(DAY_2, true, "F");
        IndexFieldHoleBoundary day1End = new IndexFieldHoleBoundary(DAY_1, false, "F");
        IndexFieldHoleBoundary day1StartA = new IndexFieldHoleBoundary(DAY_1, true, "A");
        IndexFieldHoleBoundary day1StartB = new IndexFieldHoleBoundary(DAY_1, true, "B");

        TreeSet<IndexFieldHoleBoundary> sorted = new TreeSet<>();
        sorted.add(day2Start);
        sorted.add(day1End);
        sorted.add(day1StartA);
        sorted.add(day1StartB);

        List<IndexFieldHoleBoundary> ordered = new ArrayList<>(sorted);
        assertEquals(List.of(day1StartA, day1StartB, day1End, day2Start), ordered);
    }

    @Test
    void equalsAndHashCodeConsiderDateStartAndField() {
        IndexFieldHoleBoundary a = new IndexFieldHoleBoundary(DAY_1, true, "F");
        IndexFieldHoleBoundary sameFields = new IndexFieldHoleBoundary(DAY_1, true, "F");
        IndexFieldHoleBoundary differentDate = new IndexFieldHoleBoundary(DAY_2, true, "F");
        IndexFieldHoleBoundary differentStart = new IndexFieldHoleBoundary(DAY_1, false, "F");
        IndexFieldHoleBoundary differentField = new IndexFieldHoleBoundary(DAY_1, true, "G");

        assertEquals(a, sameFields);
        assertEquals(a.hashCode(), sameFields.hashCode());
        assertNotEquals(a, differentDate);
        assertNotEquals(a, differentStart);
        assertNotEquals(a, differentField);
        assertFalse(a.equals(null));
        assertFalse(a.equals("not a boundary"));
    }

    @Test
    void hasFieldReflectsWhetherFieldIsPresent() {
        IndexFieldHoleBoundary withField = new IndexFieldHoleBoundary(DAY_1, true, "F");
        IndexFieldHoleBoundary withoutField = new IndexFieldHoleBoundary(DAY_1, true);

        assertTrue(withField.hasField());
        assertFalse(withoutField.hasField());
    }
}
