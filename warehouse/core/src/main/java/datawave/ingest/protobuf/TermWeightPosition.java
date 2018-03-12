package datawave.ingest.protobuf;

import org.apache.log4j.Logger;

import java.util.Comparator;

public class TermWeightPosition implements Comparable<TermWeightPosition> {
    private static final Logger log = Logger.getLogger(TermWeightPosition.class);
    private Integer offset;
    private Integer prevSkips;
    private Integer score;
    private Boolean zeroOffsetMatch;
    
    public TermWeightPosition(Builder builder) {
        offset = builder.offset;
        prevSkips = builder.prevSkips;
        score = builder.score;
        zeroOffsetMatch = builder.zeroOffsetMatch;
    }
    
    public static class MaxOffsetComparator implements Comparator<TermWeightPosition> {
        @Override
        public int compare(TermWeightPosition o1, TermWeightPosition o2) {
            return o1.getOffset().compareTo(o2.getOffset());
        }
    }
    
    public static Integer PositionScoreToTermWeightScore(Float positionScore) {
        // score is a negative log function that approaches zero
        // for storage reasons we will convert to integer and flip from negative to positive
        return Math.round(Math.abs(positionScore * 10000000));
    }
    
    public static Float TermWeightScoreToPositionScore(Integer termWeightScore) {
        // score is a negative log function that approaches zero
        // for storage reasons we will convert to integer and flip from negative to positive
        return (float) (termWeightScore * -.0000001);
    }
    
    @Override
    public int compareTo(TermWeightPosition o) {
        int result = getLowOffset().compareTo(o.getLowOffset());
        if (result != 0) {
            return result;
        }
        
        return getOffset().compareTo(o.getOffset());
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
    
    public Integer getOffset() {
        return offset;
    }
    
    public Integer getLowOffset() {
        if (null != prevSkips) {
            return offset - prevSkips;
        }
        
        return offset;
    }
    
    public Integer getPrevSkips() {
        return prevSkips;
    }
    
    public Integer getScore() {
        return score;
    }
    
    public Boolean getZeroOffsetMatch() {
        return zeroOffsetMatch;
    }
    
    public static class Builder {
        private Integer offset;
        private Integer prevSkips;
        private Integer score;
        private Boolean zeroOffsetMatch;
        
        public Builder setOffset(Integer offset) {
            this.offset = offset;
            return this;
        }
        
        public Builder setPrevSkips(Integer prevSkips) {
            this.prevSkips = prevSkips;
            return this;
        }
        
        public Builder setScore(Integer score) {
            this.score = score;
            return this;
        }
        
        public Builder setZeroOffsetMatch(Boolean zeroOffsetMatch) {
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
            
            if (info.hasZeroOffsetMatch()) {
                setZeroOffsetMatch(info.getZeroOffsetMatch());
            }
            
            return this;
        }
        
        public TermWeightPosition build() {
            return new TermWeightPosition(this);
        }
        
    }
}
