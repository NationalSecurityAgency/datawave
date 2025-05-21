package datawave.query.index.lookup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.index.lookup.RangeStream.NumShardFinder;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.util.Tuple2;
import datawave.util.time.DateHelper;

public class ShardSpecificIndexIteratorTest {

    private String start;
    private String stop;

    private final List<String> expected = new ArrayList<>();
    private final List<String> results = new ArrayList<>();

    @BeforeEach
    public void setup() {
        expected.clear();
        results.clear();
    }

    @Test
    public void testSingleDayScan() {
        withDate("20250301", "20250301");
        withExpected("20250301_0", "20250301_1");
        drive();
    }

    @Test
    public void testTwoDayScan() {
        withDate("20250301", "20250302");
        withExpected("20250301_0", "20250301_1", "20250302_0", "20250302_1");
        drive();
    }

    @Test
    public void testFullYearScan() {
        withDate("20250101", "20251231");

        List<String> shards = new ArrayList<>();
        Calendar start = getCalendarStartOfDay(DateHelper.parse("20250101"));
        Calendar stop = getCalendarStartOfDay(DateHelper.parse("20251231"));

        while (start.compareTo(stop) <= 0) {
            shards.add(DateHelper.format(start.getTime()) + "_0");
            shards.add(DateHelper.format(start.getTime()) + "_1");
            start.add(Calendar.DAY_OF_YEAR, 1);
        }

        expected.addAll(shards);
        drive();
    }

    @Test
    public void testScanCrossesMonthBoundary() {
        withDate("20250228", "20250301");
        withExpected("20250228_0", "20250228_1", "20250301_0", "20250301_1");
        drive();
    }

    @Test
    public void testScanCrossesYearBoundary() {
        withDate("20241231", "20250101");
        withExpected("20241231_0", "20241231_1", "20250101_0", "20250101_1");
        drive();
    }

    private void withDate(String start, String stop) {
        this.start = start;
        this.stop = stop;
    }

    private void withExpected(String... shards) {
        this.expected.addAll(List.of(shards));
    }

    private void drive() {
        JexlNode node = JexlNodeFactory.buildEQNode("FIELD", "value");

        TreeMap<String,Integer> cacheData = new TreeMap<>();
        cacheData.put("20230101", 2);

        NumShardFinder numShards = new MockNumShardFinder(cacheData);

        ShardSpecificIndexIterator iter = new ShardSpecificIndexIterator(node, numShards, DateHelper.parse(start), DateHelper.parse(stop));

        while (iter.hasNext()) {
            Tuple2<String,IndexInfo> tuple = iter.next();
            results.add(tuple.first());
        }

        assertEquals(expected, results);
    }

    private static Calendar getCalendarStartOfDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar;
    }

    private static class MockNumShardFinder extends NumShardFinder {

        public MockNumShardFinder(TreeMap<String,Integer> data) {
            super(null);
            this.cache.putAll(data);
        }
    }

}
