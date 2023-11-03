package datawave.query.util.ssdeep;

import java.io.Serializable;
import java.util.Objects;

public class NGramScoreTuple implements Serializable, Comparable<NGramScoreTuple> {
    final SSDeepHash ssDeepHash;
    final float score;

    public NGramScoreTuple(SSDeepHash ssDeepHash, float score) {
        this.ssDeepHash = ssDeepHash;
        this.score = score;
    }

    public SSDeepHash getSsDeepHash() {
        return ssDeepHash;
    }

    public float getScore() {
        return score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof NGramScoreTuple))
            return false;
        NGramScoreTuple that = (NGramScoreTuple) o;
        return ssDeepHash == that.ssDeepHash && Float.compare(that.score, score) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ssDeepHash, score);
    }

    @Override
    public String toString() {
        return "ScoreTuple{" + "hash=" + ssDeepHash + ", score=" + score + '}';
    }

    @Override
    public int compareTo(NGramScoreTuple o) {
        int cmp = Float.compare(o.score, score);
        if (cmp == 0) {
            cmp = ssDeepHash.compareTo(o.ssDeepHash);
        }
        return cmp;
    }
}
