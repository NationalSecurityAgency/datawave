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
     * Verify that {@link DateIndexUtil#format} formats a valid Date object into a yyyyMMdd string representation.
     */
    @Test
    public void testFormat() {
        Date date = create(2026, Calendar.AUGUST, 13, 7, 7, 7, 0);
        String dateStr = "20260813";
        assertEquals(dateStr, DateIndexUtil.format(date));
    }

    /**
     * Verify that {@link DateIndexUtil#format} throws a NullPointerException when attempting to format a null Date.
     */
    @Test
    public void testFormatNullDate() {
        Date date = null;
        assertThrows(NullPointerException.class, () -> {
            DateIndexUtil.format(date);
        });
    }

    /**
     * Verify that {@link DateIndexUtil#getBeginDate} properly parses a yyyyMMdd string into a Date representing the very beginning of that day.
     */
    @Test
    public void testGetBeginDate() throws ParseException {
        String dateStr = "20260813";
        Date date = create(2026, Calendar.AUGUST, 13, 0, 0, 0, 0);
        assertEquals(date, DateIndexUtil.getBeginDate(dateStr));
    }

    /**
     * Verify that {@link DateIndexUtil#getBeginDate} throws a DateTimeParseException when the date string is too short.
     */
    @Test
    public void testGetBeginDateStringTooShort() {
        String dateStr = "202608";
        assertThrows(DateTimeParseException.class, () -> {
            DateIndexUtil.getBeginDate(dateStr);
        });
    }

    /**
     * Verify that {@link DateIndexUtil#getBeginDate} throws a DateTimeParseException when the date string contains invalid non-numeric characters.
     */
    @Test
    public void testGetBeginDateInvalidCharacter() {
        String dateStr = "2026!081";
        assertThrows(DateTimeParseException.class, () -> {
            DateIndexUtil.getBeginDate(dateStr);
        });
    }

    /**
     * Verify that {@link DateIndexUtil#getBeginDate} throws an IllegalArgumentException when the input date string is null.
     */
    @Test
    public void testGetBeginDateNullString() {
        String dateStr = null;
        assertThrows(IllegalArgumentException.class, () -> {
            DateIndexUtil.getBeginDate(dateStr);
        });
    }

    /**
     * Verify that {@link DateIndexUtil#getBeginDate} throws an IllegalArgumentException when the input date string is empty.
     */
    @Test
    public void testGetBeginDateEmptyString() {
        String dateStr = "";
        assertThrows(IllegalArgumentException.class, () -> {
            DateIndexUtil.getBeginDate(dateStr);
        });
    }

    /**
     * Verify that {@link DateIndexUtil#getEndDate} properly parses a yyyyMMdd string into a Date representing the very end of that day (23:59:59.999).
     */
    @Test
    public void testGetEndDate() throws ParseException {
        String dateStr = "20260813";
        Date date = create(2026, Calendar.AUGUST, 13, 23, 59, 59, 999);
        assertEquals(date, DateIndexUtil.getEndDate(dateStr));
    }

    /**
     * Verify that {@link DateIndexUtil#getEndDate} throws a DateTimeParseException when the date string is too short.
     */
    @Test
    public void testGetEndDateStringTooShort() {
        String dateStr = "202608";
        assertThrows(DateTimeParseException.class, () -> {
            DateIndexUtil.getEndDate(dateStr);
        });
    }

    /**
     * Verify that {@link DateIndexUtil#getEndDate} throws a DateTimeParseException when the date string contains invalid non-numeric characters.
     */
    @Test
    public void testGetEndDateInvalidCharacter() {
        String dateStr = "2026!081";
        assertThrows(DateTimeParseException.class, () -> {
            DateIndexUtil.getEndDate(dateStr);
        });
    }

    /**
     * Verify that {@link DateIndexUtil#getEndDate} throws an IllegalArgumentException when the input date string is null.
     */
    @Test
    public void testGetEndDateNullString() {
        String dateStr = null;
        assertThrows(IllegalArgumentException.class, () -> {
            DateIndexUtil.getEndDate(dateStr);
        });
    }

    /**
     * Verify that {@link DateIndexUtil#getEndDate} throws an IllegalArgumentException when the input date string is empty.
     */
    @Test
    public void testGetEndDateHasEmptyString() {
        String dateStr = "";
        assertThrows(IllegalArgumentException.class, () -> {
            DateIndexUtil.getEndDate(dateStr);
        });
    }

    /**
     * Verify that {@link DateIndexUtil#getEndDate} throws a DateTimeParseException when the input date string does not have the expected length.
     */
    @Test
    public void testGetEndDateInvalidStringLength() {
        String dateStr = "202608";
        assertThrows(DateTimeParseException.class, () -> {
            DateIndexUtil.getEndDate(dateStr);
        });
    }

}
