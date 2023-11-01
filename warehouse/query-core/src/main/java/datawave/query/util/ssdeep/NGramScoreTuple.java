package datawave.query.util.ssdeep;

import java.io.Serializable;

public class NGramScoreTuple implements Serializable, Comparable<NGramScoreTuple> {
    final SSDeepHash ssDeepHash;
    final float baseScore;
    final int weightedScore;

    public NGramScoreTuple(SSDeepHash ssDeepHash, float baseScore, int weightedScore) {
        this.ssDeepHash = ssDeepHash;
        this.baseScore = baseScore;
        this.weightedScore = weightedScore;
    }

    public SSDeepHash getSsDeepHash() {
        return ssDeepHash;
    }

    public float getBaseScore() {
        return baseScore;
    }

    public float getWeightedScore() {
        return weightedScore;
    }

    @Override
    public String toString() {
        return "ScoreTuple{" + "hash=" + ssDeepHash + ", baseScore=" + baseScore + ", weightedScore=" + weightedScore + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        NGramScoreTuple that = (NGramScoreTuple) o;

        if (Float.compare(baseScore, that.baseScore) != 0)
            return false;
        if (Float.compare(weightedScore, that.weightedScore) != 0)
            return false;
        return ssDeepHash.equals(that.ssDeepHash);
    }

    @Override
    public int hashCode() {
        int result = ssDeepHash.hashCode();
        result = 31 * result + (baseScore != 0.0f ? Float.floatToIntBits(baseScore) : 0);
        result = 31 * result + (weightedScore != 0.0f ? Float.floatToIntBits(weightedScore) : 0);
        return result;
    }

    @Override
    public int compareTo(NGramScoreTuple o) {
        int cmp = Integer.compare(o.weightedScore, weightedScore);
        if (cmp == 0) {
            cmp = ssDeepHash.compareTo(o.ssDeepHash);
        }
        return cmp;
    }
}
