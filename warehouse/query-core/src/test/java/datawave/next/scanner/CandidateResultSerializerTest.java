package datawave.next.scanner;

import datawave.next.stats.DocIdQueryIterStats;
import datawave.next.stats.DocIterStats;
import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CandidateResultSerializerTest {

    private final long timestamp = Clock.systemUTC().millis();

    private final CandidateResultSerializer serializer = new CandidateResultSerializer();

    private CandidateResult result;

    @BeforeEach
    public void beforeEach() {
        result = new CandidateResult();
    }

    @Test
    public void testCandidateRoundTrip() throws IOException {
        Set<Key> candidates = createCandidates();
        result.setCandidates(candidates);

        CandidateResult deserialized = roundTrip();
        assertEquals(candidates, deserialized.getCandidates());
    }

    @Test
    public void testQueryStatsRoundTrip() throws IOException {
        DocIdQueryIterStats queryStats = createQueryStats();
        result.setQueryStats(queryStats);

        CandidateResult deserialized = roundTrip();
        assertEquals(queryStats, deserialized.getQueryStats());
    }

    @Test
    public void testIterStatsRoundTrip() throws IOException {
        DocIterStats iterStats = createIterStats();
        result.setIterStats(iterStats);

        CandidateResult deserialized = roundTrip();
        assertEquals(iterStats, deserialized.getIterStats());
    }

    @Test
    public void testQueryAndIterStatsRoundTrip() throws IOException {
        DocIdQueryIterStats queryStats = createQueryStats();
        DocIterStats iterStats = createIterStats();
        result.setQueryStats(queryStats);
        result.setIterStats(iterStats);

        CandidateResult deserialized = roundTrip();
        assertEquals(queryStats, deserialized.getQueryStats());
        assertEquals(iterStats, deserialized.getIterStats());
    }

    @Test
    public void testFullStatsRoundTrip() throws IOException {
        Set<Key> candidates = createCandidates();
        DocIdQueryIterStats queryStats = createQueryStats();
        DocIterStats iterStats = createIterStats();
        result.setCandidates(candidates);
        result.setQueryStats(queryStats);
        result.setIterStats(iterStats);

        CandidateResult deserialized = roundTrip();
        assertEquals(candidates, deserialized.getCandidates());
        assertEquals(queryStats, deserialized.getQueryStats());
        assertEquals(iterStats, deserialized.getIterStats());
    }

    // i.e., no shared state between method calls
    @Test
    public void testRepeatedDeserializeDifferentResults() throws IOException {
        DocIterStats firstIterStats = new DocIterStats();
        firstIterStats.incrementNextCount(123L);

        DocIterStats secondIterStats = new DocIterStats();
        secondIterStats.incrementNextCount(456L);

        CandidateResult firstResult = new CandidateResult();
        CandidateResult secondResult = new CandidateResult();
        firstResult.setIterStats(firstIterStats);
        secondResult.setIterStats(secondIterStats);

        CandidateResult firstDeserialized = roundTrip(firstResult);
        CandidateResult secondDeserialized = roundTrip(secondResult);

        assertEquals(firstIterStats, firstDeserialized.getIterStats());
        assertEquals(secondIterStats, secondDeserialized.getIterStats());
    }

    private CandidateResult roundTrip() throws IOException {
        return roundTrip(result);
    }

    private CandidateResult roundTrip(CandidateResult result) throws IOException {
        byte[] data = serializer.serialize(result);
        return serializer.deserialize(data);
    }

    private Set<Key> createCandidates() {
        Set<Key> candidates = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            candidates.add(new Key("20260427_17", "datatype-a\0uid-" + i));
        }
        return candidates;
    }

    private DocIterStats createIterStats() {
        DocIterStats stats = new DocIterStats();
        stats.incrementNextCount(123L);
        stats.incrementNextCount(456L);
        stats.incrementDatatypeFilterMiss(512L);
        stats.incrementTimeFilterMiss(1024L);
        stats.incrementRegexMiss(0L);
        return stats;
    }

    private DocIdQueryIterStats createQueryStats() {
        DocIdQueryIterStats stats = new DocIdQueryIterStats();
        stats.incrementTotalDocumentIds(12345L);
        stats.markScanInit(timestamp);
        stats.markScanStart(timestamp + 1);
        stats.markScanStop(timestamp + 25);
        stats.markRetrievalStart(timestamp + 30);
        stats.markRetrievalStop(timestamp + 40);
        return stats;
    }

}
