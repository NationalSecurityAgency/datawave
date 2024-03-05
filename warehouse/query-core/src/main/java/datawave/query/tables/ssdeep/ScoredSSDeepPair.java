package datawave.query.tables.ssdeep;

import java.util.Objects;

import datawave.util.ssdeep.SSDeepHash;

/**
 * Captures a scored pair of query hash and matching hash.
 */
public class ScoredSSDeepPair implements Comparable<ScoredSSDeepPair> {

    public static final java.util.Comparator<ScoredSSDeepPair> NATURAL_ORDER = new Comparator();

    private final SSDeepHash queryHash;
    private final SSDeepHash matchingHash;

    int overlapScore;
    int weightedScore;

    public ScoredSSDeepPair(SSDeepHash queryHash, SSDeepHash matchingHash, int overlapScore, int weightedScore) {
        this.queryHash = queryHash;
        this.matchingHash = matchingHash;
        this.overlapScore = overlapScore;
        this.weightedScore = weightedScore;
    }

    public SSDeepHash getQueryHash() {
        return queryHash;
    }

    public SSDeepHash getMatchingHash() {
        return matchingHash;
    }

    public int getWeightedScore() {
        return weightedScore;
    }

    public int getOverlapScore() {
        return overlapScore;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ScoredSSDeepPair that = (ScoredSSDeepPair) o;

        if (overlapScore != that.overlapScore)
            return false;
        if (weightedScore != that.weightedScore)
            return false;
        if (!Objects.equals(queryHash, that.queryHash))
            return false;
        return Objects.equals(matchingHash, that.matchingHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryHash, matchingHash, weightedScore, overlapScore);
    }

    @Override
    public int compareTo(ScoredSSDeepPair o) {
        return NATURAL_ORDER.compare(this, o);
    }

    public static class Comparator implements java.util.Comparator<ScoredSSDeepPair> {
        @Override
        public int compare(ScoredSSDeepPair o1, ScoredSSDeepPair o2) {
            int cmp = o1.getQueryHash().compareTo(o2.getQueryHash());
            if (cmp == 0) {
                cmp = o1.getMatchingHash().compareTo(o2.getMatchingHash());
            }
            if (cmp == 0) {
                cmp = o1.getWeightedScore() - o2.getWeightedScore();
            }
            if (cmp == 0) {
                cmp = o1.getOverlapScore() - o2.getOverlapScore();
            }
            return cmp;
        }
    }
}
