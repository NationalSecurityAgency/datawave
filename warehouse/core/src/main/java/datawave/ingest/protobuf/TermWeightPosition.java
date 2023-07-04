package datawave.ingest.protobuf;

import java.util.Comparator;

import org.apache.log4j.Logger;

/**
 * This is a utility class for processing a single term weight position. These get aggregated into a TermWeight,
 */
public class TermWeightPosition implements Comparable<TermWeightPosition> {
    private static final Logger log = Logger.getLogger(TermWeightPosition.class);
    public static final int DEFAULT_OFFSET = -1;
    public static final int DEFAULT_PREV_SKIPS = -1;
    public static final int DEFAULT_SCORE = -1;
    public static final boolean DEFAULT_ZERO_OFFSET_MATCH = true;

    private final int offset;
    private final int prevSkips;
    private final int score;
    private final boolean zeroOffsetMatch;

    public TermWeightPosition(Builder builder) {
        offset = builder.offset;
        prevSkips = builder.prevSkips;
        score = builder.score;
        zeroOffsetMatch = builder.zeroOffsetMatch;
    }

    public TermWeightPosition(TermWeightPosition other) {
        offset = other.offset;
        prevSkips = other.prevSkips;
        score = other.score;
        zeroOffsetMatch = other.zeroOffsetMatch;
    }

    public TermWeightPosition clone() {
        return new TermWeightPosition(this);
    }

    public static class MaxOffsetComparator implements Comparator<TermWeightPosition> {
        @Override
        public int compare(TermWeightPosition o1, TermWeightPosition o2) {
            return Integer.compare(o1.getOffset(), o2.getOffset());
        }
    }

    /**
     * Score is a negative log function that approaches zero for storage reasons we will convert to integer and flip from negative to positive
     *
     * @param positionScore
     *            negative value that approaches 0
     * @return absolute value of the score, to a precision of 10000000
     */
    public static int positionScoreToTermWeightScore(float positionScore) {
        //
        return Math.round(Math.abs(positionScore * 10000000));
    }

    /**
     * Score is a negative log function that approaches zero for storage reasons we will convert to integer and flip from negative to positive
     *
     * @param termWeightScore
     *            positive value of the score
     * @return original score to the precision available
     */
    public static float termWeightScoreToPositionScore(int termWeightScore) {
        // score is a negative log function that approaches zero
        // for storage reasons we will convert to integer and flip from negative to positive
        return (float) (termWeightScore * -.0000001);
    }

    @Override
    public int compareTo(TermWeightPosition o) {
        int result = Integer.compare(getLowOffset(), o.getLowOffset());
        if (result != 0) {
            return result;
        }

        return Integer.compare(getOffset(), o.getOffset());
    }

    @Override
    public String toString() {
        return "{zeroMatch=" + zeroOffsetMatch + ", offset=" + offset + ", prevSkips=" + prevSkips + ", score=" + score + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TermWeightPosition) {
            return this.equals((TermWeightPosition) o);
        }

        return false;
    }

    public boolean equals(TermWeightPosition o) {

        return (compareTo(o) == 0);
    }

    /**
     * @return Maximum possible offset, skips can not increase the value
     */
    public int getOffset() {
        return offset;
    }

    /**
     * @return Lowest possible offset with respect to skips
     */
    public int getLowOffset() {
        if (prevSkips >= 0) {
            return offset - prevSkips;
        }

        return offset;
    }

    public int getPrevSkips() {
        return prevSkips;
    }

    public int getScore() {
        return score;
    }

    public boolean getZeroOffsetMatch() {
        return zeroOffsetMatch;
    }

    public static class Builder {
        private int offset = DEFAULT_OFFSET;
        private int prevSkips = DEFAULT_PREV_SKIPS;
        private int score = DEFAULT_SCORE;
        private boolean zeroOffsetMatch = DEFAULT_ZERO_OFFSET_MATCH;

        public Builder setOffset(int offset) {
            this.offset = offset;
            return this;
        }

        public Builder setPrevSkips(int prevSkips) {
            this.prevSkips = prevSkips;
            return this;
        }

        public Builder setScore(int score) {
            this.score = score;
            return this;
        }

        public Builder setZeroOffsetMatch(boolean zeroOffsetMatch) {
            this.zeroOffsetMatch = zeroOffsetMatch;
            return this;
        }

        public Builder setTermWeightOffsetInfo(TermWeight.Info info, int i) {
            setOffset(info.getTermOffset(i));

            // Only pull the previous skips if the counts match the offsets
            // offsets, skips, and scores are linked by index so array lengths must match
            if (info.getTermOffsetCount() == info.getPrevSkipsCount()) {
                setPrevSkips(info.getPrevSkips(i));
            }

            // Only pull the scores if the counts match the offsets
            if (info.getTermOffsetCount() == info.getScoreCount()) {
                setScore(info.getScore(i));
            }

            setZeroOffsetMatch(info.getZeroOffsetMatch());

            return this;
        }

        public TermWeightPosition build() {
            return new TermWeightPosition(this);
        }

        public void reset() {
            offset = DEFAULT_OFFSET;
            prevSkips = DEFAULT_PREV_SKIPS;
            score = DEFAULT_SCORE;
            zeroOffsetMatch = DEFAULT_ZERO_OFFSET_MATCH;
        }

    }
}
