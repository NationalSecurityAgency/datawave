package datawave.query.tables;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DateIteratorTest {

    private final List<String> expected = new ArrayList<>();
    private final List<String> results = new ArrayList<>();

    private String start;
    private String end;

    @BeforeEach
    public void beforeEach() {
        expected.clear();
        results.clear();
    }

    @Test
    public void testSingleDay() {
        expect("20251010");
        withDateRange("20251010");
        iterate();
    }

    @Test
    public void testSingleDayThatDoesNotExist() {
        expect("20251099");
        withDateRange("20251099");
        assertThrows(DateTimeParseException.class, this::iterate);
    }

    @Test
    public void testMultiDay() {
        expect("20251010", "20251011", "20251012", "20251013");
        withDateRange("20251010", "20251013");
        iterate();
    }

    @Test
    public void testMonthBoundary() {
        expect("20251031", "20251101");
        withDateRange("20251031", "20251101");
        iterate();
    }

    @Test
    public void testYearBoundary() {
        expect("20251231", "20260101");
        withDateRange("20251231", "20260101");
        iterate();
    }

    private void expect(String... dates) {
        expected.addAll(Arrays.asList(dates));
    }

    private void withDateRange(String date) {
        withDateRange(date, date);
    }

    private void withDateRange(String start, String end) {
        this.start = start;
        this.end = end;
    }

    private void iterate() {
        DateIterator iter = new DateIterator(start, end);
        while (iter.hasNext()) {
            results.add(iter.next());
        }
        assertEquals(expected, results);
    }
}
