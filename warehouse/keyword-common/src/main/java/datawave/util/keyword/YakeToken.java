package datawave.util.keyword;

import java.util.Map;

/**
 * Represents a token found in input text which could be a component of an extracted keyword. Includes a number of statistics used in keyword selection.
 */
public class YakeToken {

    /** The string token represented by this class */
    final String token;

    /** The number of times the term/token appeared across all input */
    final int termFrequency;

    /** The total number of sentences in the input */
    final int totalSentences;

    /** The mean term frequency across all sentences */
    final double meanTF;

    /** The standard deviation of the term frequency across all sentences */
    final double stdTF;

    /** The maximum term frequency across all sentences */
    final double maxTF;

    /** The left co-occurrences for this token (e.g., tokens that appear before this token) */
    final Map<String,Integer> leftCo;

    /** The right co-occurrences for this token (e.g., tokens that appear after this token) */
    final Map<String,Integer> rightCo;

    int nCount = 0;
    int aCount = 0;

    int medianSentenceOffset = 0;

    /** The number of sentences this token appeared in */
    int numberOfSentences = 0;

    public YakeToken(String token, int termFrequency, int totalSentences, double meanTF, double stdTF, double maxTF, Map<String,Integer> leftCo,
                    Map<String,Integer> rightCo) {
        this.token = token;
        this.termFrequency = termFrequency;
        this.totalSentences = totalSentences;
        this.meanTF = meanTF;
        this.stdTF = stdTF;
        this.maxTF = maxTF;
        this.leftCo = leftCo;
        this.rightCo = rightCo;
    }

    public double tCase() {
        return ((double) Math.max(nCount, aCount)) / (1 + Math.log(termFrequency));
    }

    public double tPosition() {
        return Math.log(3 + medianSentenceOffset);
    }

    public double tfNorm() {
        return ((double) termFrequency) / (meanTF + stdTF);
    }

    public double tSentence() {
        return ((double) numberOfSentences) / ((double) totalSentences);
    }

    public double tRel() {
        double leftCoValueSum = leftCo.values().stream().mapToInt(Integer::intValue).sum();
        double rightCoValueSum = rightCo.values().stream().mapToInt(Integer::intValue).sum();

        final double left = (leftCo.isEmpty() ? 0.0 : (((double) leftCo.size()) / leftCoValueSum));
        final double right = (rightCo.isEmpty() ? 0.0 : (((double) leftCo.size()) / rightCoValueSum));

        return 1.0 + ((left + right) * (((double) termFrequency) / maxTF));
    }

    public double tScore() {
        final double tRel = tRel();
        return tPosition() * tRel / (tCase() + (tfNorm() / tRel) + (tSentence() / tRel));
    }

    @Override
    public String toString() {
        return "YakeToken{" + "token='" + token + '\'' + ", termFrequency=" + termFrequency + ", totalSentences=" + totalSentences + ", meanTF=" + meanTF
                        + ", stdTF=" + stdTF + ", maxTF=" + maxTF + ", nCount=" + nCount + ", aCount=" + aCount + ", medianSentenceOffset="
                        + medianSentenceOffset + ", numberOfSentences=" + numberOfSentences + ", leftCo=" + leftCo + ", rightCo=" + rightCo + '}';
    }
}
