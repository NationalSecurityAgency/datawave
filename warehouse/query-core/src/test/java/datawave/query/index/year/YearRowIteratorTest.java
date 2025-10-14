package datawave.query.index.year;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.util.time.DateHelper;

public class YearRowIteratorTest {

    private String start;
    private String stop;
    private final SortedSet<String> expected = new TreeSet<>();

    @BeforeEach
    public void before() {
        start = null;
        stop = null;
        expected.clear();
    }

    @Test
    public void testSingleYearScan() {
        setStartStop("20201122", "20201122");
        setExpected("2020");
        driveIterator();
    }

    @Test
    public void testThreeYearScan() {
        setStartStop("20201012", "20221122");
        setExpected("2020", "2021", "2022");
        driveIterator();
    }

    @Test
    public void testScanCrossesCenturyBoundary() {
        setStartStop("20991122", "21011122");
        setExpected("2099", "2100", "2101");
        driveIterator();
    }

    @Test
    public void testScanCrossesCenturyBoundaryPreEpoch() {
        setStartStop("18991122", "19011122");
        setExpected("1899", "1900", "1901");
        driveIterator();
    }

    @Test
    public void testScanCrossesMillenniumBoundary() {
        setStartStop("19991122", "20010203");
        setExpected("1999", "2000", "2001");
        driveIterator();
    }

    private void driveIterator() {
        assertNotNull(start);
        assertNotNull(stop);
        assertNotNull(expected);

        Date startDate = timeToDate(start);
        Date stopDate = timeToDate(stop);

        SortedSet<String> years = new TreeSet<>();
        YearRowIterator iter = new YearRowIterator(startDate, stopDate);

        while (iter.hasNext()) {
            String year = iter.next();
            assertNotNull(year);
            years.add(year);
        }

        assertEquals(expected, years, "Expected " + expected + " but got " + years);
    }

    private void setStartStop(String start, String stop) {
        setStart(start);
        setStop(stop);
    }

    private void setStart(String start) {
        this.start = start;
    }

    private void setStop(String stop) {
        this.stop = stop;
    }

    private void setExpected(String... years) {
        expected.addAll(Arrays.asList(years));
    }

    private Date timeToDate(String time) {
        return DateHelper.parse(time);
    }
}
