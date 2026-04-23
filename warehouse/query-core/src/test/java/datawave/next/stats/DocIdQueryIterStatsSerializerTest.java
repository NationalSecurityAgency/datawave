package datawave.next.stats;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DocIdQueryIterStatsSerializerTest {

    private final DocIdQueryIterStatsSerializer serializer = new DocIdQueryIterStatsSerializer();

    @Test
    public void testRoundTrip() {
        DocIdQueryIterStats stats = getStats();

        byte[] data = serializer.serialize(stats);
        assertEquals(11, data.length);

        DocIdQueryIterStats result = serializer.deserialize(data);
        assertEquals(stats.getTotalDocumentIds(), result.getTotalDocumentIds());
        assertEquals(stats.getTotalInitTime(), result.getTotalInitTime());
        assertEquals(stats.getTotalRetrievalTime(), result.getTotalRetrievalTime());
        assertEquals(stats.getTotalElapsedTime(), result.getTotalElapsedTime());

        assertEquals(stats, result);
    }

    private DocIdQueryIterStats getStats() {
        DocIdQueryIterStats stats = new DocIdQueryIterStats();
        stats.incrementTotalDocumentIds(2400);

        long ms = 1776953271422L;
        stats.markScanInit(ms);
        stats.markScanStart(ms + 1);
        stats.markScanStop(ms + 23_000);
        stats.markRetrievalStart(ms + 25_000);
        stats.markRetrievalStop(ms + 25_500);
        return stats;
    }
}
