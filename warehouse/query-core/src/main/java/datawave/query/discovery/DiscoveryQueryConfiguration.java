package datawave.query.discovery;

import com.google.common.collect.Multimap;
import datawave.query.config.ShardIndexQueryConfiguration;
import datawave.query.jexl.LiteralRange;
import datawave.query.tables.ShardIndexQueryTable;
import datawave.webservice.query.Query;

import java.io.Serializable;
import java.util.Objects;

/**
 * Adds the ability to hold on to two multimaps. They map literals and patterns to the fields they were associated with in the query.
 */
public class DiscoveryQueryConfiguration extends ShardIndexQueryConfiguration implements Serializable {
    private Multimap<String,String> literals, patterns;
    private Multimap<String,LiteralRange<String>> ranges;
    private Boolean separateCountsByColVis = false;
    private Boolean showReferenceCount = false;
    
    public DiscoveryQueryConfiguration(ShardIndexQueryTable logic, Query query) {
        super(logic, query);
    }
    
    public Multimap<String,String> getLiterals() {
        return literals;
    }
    
    public void setLiterals(Multimap<String,String> literals) {
        this.literals = literals;
    }
    
    public Multimap<String,LiteralRange<String>> getRanges() {
        return ranges;
    }
    
    public void setRanges(Multimap<String,LiteralRange<String>> ranges) {
        this.ranges = ranges;
    }
    
    public Multimap<String,String> getPatterns() {
        return patterns;
    }
    
    public void setPatterns(Multimap<String,String> patterns) {
        this.patterns = patterns;
    }
    
    public Boolean getSeparateCountsByColVis() {
        return separateCountsByColVis;
    }
    
    public Boolean getShowReferenceCount() {
        return showReferenceCount;
    }
    
    public void setSeparateCountsByColVis(boolean separateCountsByColVis) {
        this.separateCountsByColVis = separateCountsByColVis;
    }
    
    public void setShowReferenceCount(Boolean showReferenceCount) {
        this.showReferenceCount = showReferenceCount;
        
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        DiscoveryQueryConfiguration that = (DiscoveryQueryConfiguration) o;
        return Objects.equals(literals, that.literals) && Objects.equals(patterns, that.patterns) && Objects.equals(ranges, that.ranges)
                        && Objects.equals(separateCountsByColVis, that.separateCountsByColVis) && Objects.equals(showReferenceCount, that.showReferenceCount);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), literals, patterns, ranges, separateCountsByColVis, showReferenceCount);
    }
}
