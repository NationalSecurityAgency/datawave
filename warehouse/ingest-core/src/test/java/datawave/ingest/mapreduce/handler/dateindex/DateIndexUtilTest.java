package datawave.ingest.mapreduce.handler.dateindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.ParseException;
import java.time.format.DateTimeParseException;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for DateIndexUtil.
 */
public class DateIndexUtilTest {

    /**
     * Verify that {@link DateIndexUtil#getBits} returns a BitSet with only the specified index set to true.
     */
    @Test
    public void testGetBits() {
        BitSet bits = DateIndexUtil.getBits(20);
        for (int i = 0; i < 20; i++) {
            assertFalse(bits.get(i));
        }
        assertTrue(bits.get(20));
        for (int i = 21; i < bits.size(); i++) {
            assertFalse(bits.get(i));
        }
    }

    /**
     * Verify that {@link DateIndexUtil#merge} correctly combines the true bits of two given BitSets into a single BitSet.
     */
    @Test
    public void testMerge() {

        Set<Integer> bits = new HashSet<>();
        for (int i = 0; i < 40; i++) {
            bits.add(i);
            bits.add(i * 2);
            bits.add(i * 3);
        }

        BitSet bitSet1 = new BitSet(1);
        BitSet bitSet2 = new BitSet(1);
        int count = 0;
        for (Integer i : bits) {
            if (count % 2 == 0) {
                bitSet1 = DateIndexUtil.merge(bitSet1, DateIndexUtil.getBits(i));
            } else {
                bitSet2 = DateIndexUtil.merge(bitSet2, DateIndexUtil.getBits(i));
            }
        }
        BitSet bitSet = DateIndexUtil.merge(bitSet1, bitSet2);

        for (int i = 0; i < 40 * 4; i++) {
            if (bits.contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
    }

    private Date create(int year, int month, int day, int hour, int minute, int second, int millis) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getDefault());
        calendar.set(year, month, day, hour, minute, second);
        calendar.set(Calendar.MILLISECOND, millis);
        return calendar.getTime();
    }

    /**
     * Tests for {@link DateIndexUtil#format(Date)}.
     */
    @DisplayName("Method format(Date)")
    @Nested
    class FormatTests {

        @DisplayName("Throws an NPE given a null arguemtn")
        @Test
        void nullDate() {
            assertThrows(NullPointerException.class, () -> DateIndexUtil.format(null));
        }

        @DisplayName("Returns the yyyyMMdd form of a date")
        @Test
        void nonNullDate() {
            Date date = create(2026, Calendar.AUGUST, 13, 7, 7, 7, 0);
            assertEquals("20260813", DateIndexUtil.format(date));
        }
    }

    @DisplayName("Method getBeginDate(String)")
    @Nested
    class GetBeginDateTests {

        @DisplayName("Returns Date at the very beginning of the day (00:00:00.000)")
        @Test
        void validDate() throws ParseException {
            Date date = create(2026, Calendar.AUGUST, 13, 0, 0, 0, 0);
            assertEquals(date, DateIndexUtil.getBeginDate("20260813"));
        }

        @DisplayName("Throws an exception given a null argument")
        @Test
        void nullDate() {
            assertThrows(IllegalArgumentException.class, () -> DateIndexUtil.getBeginDate(null));
        }

        @DisplayName("Throws an exception given a empty string")
        @Test
        void emptyString() {
            assertThrows(IllegalArgumentException.class, () -> DateIndexUtil.getBeginDate(""));
        }

        @DisplayName("Throws an exception given an argument not in the form yyyyMMdd")
        @Test
        void nonParseableDate() {
            assertThrows(DateTimeParseException.class, () -> DateIndexUtil.getBeginDate("2026080"));
        }
    }

    @DisplayName("Method getEndDate(String)")
    @Nested
    class GetEndDateTests {

        @DisplayName("Returns Date at the very end of the day (23:59:59.999)")
        @Test
        void validDate() throws ParseException {
            Date date = create(2026, Calendar.AUGUST, 13, 23, 59, 59, 999);
            assertEquals(date, DateIndexUtil.getBeginDate("20260813"));
        }

        @DisplayName("Throws an exception given a null argument")
        @Test
        void nullDate() {
            assertThrows(IllegalArgumentException.class, () -> DateIndexUtil.getBeginDate(null));
        }

        @DisplayName("Throws an exception given a empty string")
        @Test
        void emptyString() {
            assertThrows(IllegalArgumentException.class, () -> DateIndexUtil.getBeginDate(""));
        }

        @DisplayName("Throws an exception given an argument not in the form yyyyMMdd")
        @Test
        void nonParseableDate() {
            assertThrows(DateTimeParseException.class, () -> DateIndexUtil.getBeginDate("2026080"));
        }
    }

}
