package datawave.query.index.lookup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import datawave.query.index.lookup.RangeStream.NumShardFinder;
import datawave.query.util.Tuple2;

public class HintToShardIteratorTest {

    private String[] hints;
    private final List<String> expectedShards = new ArrayList<>();
    private final List<String> results = new ArrayList<>();

    private final TreeMap<String,Integer> cacheData = new TreeMap<>();

    @AfterEach
    public void after() throws Exception {
        hints = null;
        expectedShards.clear();
        results.clear();
        cacheData.clear();
    }

    @Test
    public void testOnlyShardHintsNoNumShardsCache() {
        setHints("20250202_0", "20250202_1", "20250202_3");
        setExpectedShards("20250202_0", "20250202_1", "20250202_3");
        drive();
    }

    @Test
    public void testOnlyDayHintsNoNumShardsCache() {
        setHints("20250202", "20250203", "20250204");
        setExpectedShards("20250202", "20250203", "20250204");
        drive();
    }

    @Test
    public void testMixOfShardsAndDaysHintsNoNumShardsCache() {
        setHints("20250202_0", "20250202_1", "20250203");
        setExpectedShards("20250202_0", "20250202_1", "20250203");
        drive();
    }

    @Test
    public void testOnlyShardHintsWithNumShardCache() {
        setHints("20250202_0", "20250202_1", "20250202_3");
        setExpectedShards("20250202_0", "20250202_1", "20250202_3");
        addCacheData("20250202", 17);
        drive();
    }

    @Test
    public void testSingleDayHintWithNumShardCache() {
        setHints("20250202");
        setExpectedShards("20250202_0", "20250202_1", "20250202_2");
        addCacheData("20250202", 3);
        drive();
    }

    @Test
    public void testSingleDayHintBeforeStartOfNumShardCache() {
        setHints("20250201");
        setExpectedShards("20250201");
        addCacheData("20250202", 3);
        drive();
    }

    @Test
    public void testSingleDayHintAfterStartOfNumShardCache() {
        setHints("20250203");
        setExpectedShards("20250203_0", "20250203_1", "20250203_2");
        addCacheData("20250202", 3);
        drive();
    }

    @Test
    public void testMixOfShardsAndDaysHintWithNumShardCache() {
        setHints("20250201", "20250202", "20250203_17", "20250204");
        setExpectedShards("20250201", "20250202_0", "20250202_1", "20250202_2", "20250203_17", "20250204_0", "20250204_1", "20250204_2");
        addCacheData("20250202", 3);
        drive();
    }

    @Test
    public void testMixOfShardsAndDaysHintsForSameDayWithNumShardCache() {
        // in practice this shouldn't happen, but document the case anyway
        setHints("20250202_1", "20250202");
        setExpectedShards("20250202_1", "20250202_0", "20250202_1", "20250202_2");
        addCacheData("20250202", 3);
        drive();
    }

    @Test
    public void testLexicographicalShardOrdering() {
        setHints("20250202");
        //  @formatter:off
        //  formatter is off to drive home the importance of properly sorted shards
        setExpectedShards(
                "20250202_0",
                "20250202_1",
                "20250202_10",
                "20250202_11",
                "20250202_2",
                "20250202_3",
                "20250202_4",
                "20250202_5",
                "20250202_6",
                "20250202_7",
                "20250202_8",
                "20250202_9");
        //  @formatter:on
        addCacheData("20250202", 12);
        drive();
    }

    @Test
    public void testDayHintsWithMultipleNumShardCacheEntries() {
        setHints("20250202", "20250203");
        setExpectedShards("20250202_0", "20250202_1", "20250203_0", "20250203_1", "20250203_2");
        addCacheData("20250202", 2);
        addCacheData("20250203", 3);
        drive();
    }

    private void drive() {
        NumShardFinder numShardFinder = new MockNumShardFinder(cacheData);
        HintToShardIterator iterator = new HintToShardIterator(hints, numShardFinder);

        while (iterator.hasNext()) {
            Tuple2<String,IndexInfo> entry = iterator.next();
            results.add(entry.first());
        }

        assertEquals(expectedShards.size(), results.size(), "Expected and actual shards do not match");
        assertEquals(expectedShards, results);
    }

    private void setHints(String... data) {
        this.hints = new String[data.length];
        System.arraycopy(data, 0, this.hints, 0, data.length);
    }

    private void setExpectedShards(String... hints) {
        expectedShards.addAll(List.of(hints));
    }

    private void addCacheData(String hint, Integer numShards) {
        cacheData.put(hint, numShards);
    }

    private static class MockNumShardFinder extends NumShardFinder {

        public MockNumShardFinder(TreeMap<String,Integer> data) {
            super(null);
            this.cache.putAll(data);
        }
    }
}
