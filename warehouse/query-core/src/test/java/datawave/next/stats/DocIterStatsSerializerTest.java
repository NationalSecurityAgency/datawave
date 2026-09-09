package datawave.next.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class DocIterStatsSerializerTest {

    private final DocIterStatsSerializer serializer = new DocIterStatsSerializer();

    @Test
    public void testRoundTrip() {
        DocIterStats stats = createStats();

        for (int i = 0; i < 1_000; i++) {

            byte[] data = serializer.serialize(stats);
            assertEquals(5, data.length);

            DocIterStats deserialized = serializer.deserialize(data);
            assertEquals(stats, deserialized);

            String s = stats.toString();
            assertEquals(10, s.length());

            DocIterStats other = DocIterStats.fromString(s);
            assertEquals(stats, other);
        }
    }

    private DocIterStats createStats() {
        DocIterStats stats = new DocIterStats();
        stats.incrementNextCount(15);
        stats.incrementSeekCount(2);
        stats.incrementDatatypeFilterMiss(7);
        // zero time filter misses
        // zero regex filter misses
        return stats;
    }
}
