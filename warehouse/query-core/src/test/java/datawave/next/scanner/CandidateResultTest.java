package datawave.next.scanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

import datawave.next.stats.DocIdQueryIterStats;
import datawave.next.stats.DocIterStats;

public class CandidateResultTest {

    @Test
    public void testEmptyCandidateResultSetOperations() {
        CandidateResult one = new CandidateResult();
        CandidateResult two = new CandidateResult();
        Set<CandidateResult> candidates = new HashSet<>();
        candidates.add(one);
        candidates.add(two);
        assertEquals(1, candidates.size());
    }

    @Test
    public void testDifferentObjectEquality() {
        CandidateResult one = createCandidateResult();
        CandidateResult two = createCandidateResult();
        assertNotSame(one, two);
        assertEquals(one, two);
    }

    @Test
    public void testSameIterStatsDifferentCandidates() {
        CandidateResult one = new CandidateResult();
        one.setCandidates(Set.of(new Key("row", "datatype\0uid-a")));

        CandidateResult two = new CandidateResult();
        two.setCandidates(Set.of(new Key("row", "datatype\0uid-b")));

        DocIterStats oneStats = new DocIterStats();
        oneStats.incrementNextCount(1L);
        one.setIterStats(oneStats);

        DocIterStats twoStats = new DocIterStats();
        twoStats.incrementNextCount(1L);
        two.setIterStats(twoStats);

        assertNotEquals(one, two);
    }

    private CandidateResult createCandidateResult() {
        CandidateResult result = new CandidateResult();
        result.setCandidates(Set.of(new Key("row", "datatype\0uid-a")));

        DocIterStats iterStats = new DocIterStats();
        iterStats.incrementNextCount(1L);
        iterStats.incrementSeekCount(1L);
        result.setIterStats(iterStats);

        DocIdQueryIterStats queryStats = new DocIdQueryIterStats();
        queryStats.incrementTotalDocumentIds(123L);
        result.setQueryStats(queryStats);

        return result;
    }
}
