package datawave.next.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class DocIterStatsTest {

    @Test
    public void testInitialState() {
        DocIterStats stats = new DocIterStats();
        assertEquals(0, stats.getNextCount());
        assertEquals(0, stats.getSeekCount());
        assertEquals(0, stats.getDatatypeFilterMiss());
        assertEquals(0, stats.getTimeFilterMiss());
        assertEquals(0, stats.getRegexMiss());
    }

    @Test
    public void testIncrement() {
        DocIterStats stats = new DocIterStats();

        stats.incrementNextCount();
        stats.incrementSeekCount();
        stats.incrementDatatypeFilterMiss();
        stats.incrementTimeFilterMiss();
        stats.incrementRegexMiss();

        assertEquals(1, stats.getNextCount());
        assertEquals(1, stats.getSeekCount());
        assertEquals(1, stats.getDatatypeFilterMiss());
        assertEquals(1, stats.getTimeFilterMiss());
        assertEquals(1, stats.getRegexMiss());
    }

    @Test
    public void testIncrementByCount() {
        DocIterStats stats = new DocIterStats();

        stats.incrementNextCount(1);
        stats.incrementSeekCount(2);
        stats.incrementDatatypeFilterMiss(3);
        stats.incrementTimeFilterMiss(4);
        stats.incrementRegexMiss(5);

        assertEquals(1, stats.getNextCount());
        assertEquals(2, stats.getSeekCount());
        assertEquals(3, stats.getDatatypeFilterMiss());
        assertEquals(4, stats.getTimeFilterMiss());
        assertEquals(5, stats.getRegexMiss());
    }

    @Test
    public void testMerge() {
        DocIterStats first = createFirstStats();
        assertEquals(12345, first.getNextCount());
        assertEquals(123, first.getSeekCount());

        DocIterStats second = createSecondStats();
        assertEquals(54321, second.getNextCount());
        assertEquals(321, second.getSeekCount());

        first.merge(second);
        assertEquals(66666, first.getNextCount());
        assertEquals(444, first.getSeekCount());
    }

    @Test
    public void testEquals() {
        DocIterStats first = createFirstStats();
        DocIterStats copy = createFirstStats();
        assertEquals(first, copy);

        DocIterStats second = createSecondStats();
        assertNotEquals(first, second);
    }

    @Test
    public void testHashCode() {
        Set<DocIterStats> set = new HashSet<>();
        set.add(createFirstStats());
        set.add(createFirstStats());
        set.add(createFirstStats());

        assertEquals(1, set.size());
    }

    private DocIterStats createFirstStats() {
        DocIterStats stats = new DocIterStats();
        stats.incrementNextCount(12345);
        stats.incrementSeekCount(123);
        return stats;
    }

    private DocIterStats createSecondStats() {
        DocIterStats stats = new DocIterStats();
        stats.incrementNextCount(54321);
        stats.incrementSeekCount(321);
        return stats;
    }
}
