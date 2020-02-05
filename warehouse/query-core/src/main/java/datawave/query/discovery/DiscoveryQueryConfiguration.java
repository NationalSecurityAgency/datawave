package datawave.query.discovery;

import datawave.query.config.ShardIndexQueryConfiguration;
import datawave.query.jexl.LiteralRange;
import datawave.query.tables.ShardIndexQueryTable;
import datawave.webservice.query.Query;

import com.google.common.collect.Multimap;

/**
 * Adds the ability to hold on to two multimaps. They map literals and patterns to the fields they were associated with in the query.
 */
public class DiscoveryQueryConfiguration extends ShardIndexQueryConfiguration {
    private static final long serialVersionUID = 4758499556388177460L;
    
    private Multimap<String,String> literals, patterns;
    private Multimap<String,LiteralRange<String>> ranges;
    private Boolean separateCountsByColVis = false;
    private Boolean showReferenceCount = false;
    
    public DiscoveryQueryConfiguration() {
        super();
    }
    
    public DiscoveryQueryConfiguration(DiscoveryQueryConfiguration other) {
        super(other);
        setLiterals(other.getLiterals());
        setPatterns(other.getPatterns());
        setRanges(other.getRanges());
        setSeparateCountsByColVis(other.getSeparateCountsByColVis());
        setShowReferenceCount(other.getShowReferenceCount());
    }
    
    public DiscoveryQueryConfiguration(DiscoveryLogic logic, Query query) {
        this(logic.getConfig());
        setQuery(query);
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
}
