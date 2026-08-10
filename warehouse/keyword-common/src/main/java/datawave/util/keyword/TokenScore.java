package datawave.util.keyword;

import com.google.common.base.Objects;

/** A simple two item tuple used for associating a score with a token. */
public final class TokenScore {
    private final String token;
    private final double score;

    public TokenScore(String a, double b) {
        this.token = a;
        this.score = b;
    }

    public String getToken() {
        return token;
    }

    public double getScore() {
        return score;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        TokenScore that = (TokenScore) o;
        return Double.compare(score, that.score) == 0 && Objects.equal(token, that.token);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(token, score);
    }

    @Override
    public String toString() {
        return "['" + token + "', score=" + score + "]";
    }
}
