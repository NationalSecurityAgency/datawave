package datawave.next.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Clock;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class DocIdQueryIterStatsTest {

    @Test
    public void testInitialState() {
        DocIdQueryIterStats stats = new DocIdQueryIterStats();
        // main stats
        assertEquals(0, stats.getTotalDocumentIds());
        assertEquals(0, stats.getTotalInitTime());
        assertEquals(0, stats.getTotalScanTime());
        assertEquals(0, stats.getTotalRetrievalTime());
        assertEquals(0, stats.getTotalElapsedTime());
    }

    @Test
    public void testIncrement() {
        DocIdQueryIterStats stats = new DocIdQueryIterStats();

        stats.incrementTotalDocumentIds(1L);

        DocIterStats iterStats = new DocIterStats();
        iterStats.incrementNextCount();
        iterStats.incrementSeekCount();
        iterStats.incrementDatatypeFilterMiss();
        iterStats.incrementTimeFilterMiss();
        iterStats.incrementRegexMiss();

        // main stats
        assertEquals(1, stats.getTotalDocumentIds());
    }

    @Test
    public void testTiming() {
        DocIdQueryIterStats stats = new DocIdQueryIterStats();

        long start = Clock.systemUTC().instant().getNano();
        stats.markScanInit(start);
        stats.markScanStart(start + 2L);
        stats.markScanStop(start + 7L);
        stats.markRetrievalStart(start + 8L);
        stats.markRetrievalStop(start + 9L);

        assertEquals(2, stats.getTotalInitTime());
        assertEquals(7, stats.getTotalScanTime());
        assertEquals(1, stats.getTotalRetrievalTime());
        assertEquals(9, stats.getTotalElapsedTime());
    }

    @Test
    public void testMergeQueryStats() {
        DocIdQueryIterStats first = createStats();
        DocIdQueryIterStats second = createStats();
        first.merge(second);

        assertEquals(6, first.getTotalDocumentIds());
        assertEquals(4, first.getTotalInitTime());
        assertEquals(14, first.getTotalScanTime());
        assertEquals(2, first.getTotalRetrievalTime());
        assertEquals(18, first.getTotalElapsedTime());
    }

    @Test
    public void testMergeIterStats() {
        DocIdQueryIterStats stats = createStats();

    }

    @Test
    public void testMultiMerge() {
        DocIdQueryIterStats first = createStats();
        assertEquals(3, first.getTotalDocumentIds());

        DocIdQueryIterStats second = createStats();
        assertEquals(3, second.getTotalDocumentIds());

        first.merge(second);
        assertEquals(6, first.getTotalDocumentIds());
    }

    @Test
    public void testEquals() {
        DocIdQueryIterStats first = createStats();
        DocIdQueryIterStats second = createStats();
        assertEquals(first, second);
    }

    @Test
    public void testHashCode() {
        Set<DocIdQueryIterStats> set = new HashSet<>();
        set.add(createStats());
        set.add(createStats());
        set.add(createStats());
        assertEquals(1, set.size());
    }

    private DocIdQueryIterStats createStats() {
        DocIdQueryIterStats stats = new DocIdQueryIterStats();
        stats.incrementTotalDocumentIds(3L);

        long start = 0L;
        stats.markScanInit(start);
        stats.markScanStart(start + 2L);
        stats.markScanStop(start + 7L);
        stats.markRetrievalStart(start + 8L);
        stats.markRetrievalStop(start + 9L);
        return stats;
    }

    private DocIterStats createIterStats() {
        DocIterStats stats = new DocIterStats();
        stats.incrementNextCount(22);
        stats.incrementSeekCount(1);
        return stats;
    }
}
