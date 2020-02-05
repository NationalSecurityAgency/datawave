package datawave.query.config;

import java.util.Map;
import java.util.Map.Entry;

import datawave.query.tables.ShardIndexQueryTable;
import datawave.webservice.query.Query;

import org.apache.accumulo.core.data.Range;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class ShardIndexQueryConfiguration extends ShardQueryConfiguration {
    private static final long serialVersionUID = 7616552164239289739L;
    
    private Multimap<String,String> normalizedTerms = HashMultimap.create();
    private Multimap<String,String> normalizedPatterns = HashMultimap.create();
    
    private Map<Entry<String,String>,Range> rangesForTerms = Maps.newHashMap();
    private Map<Entry<String,String>,Entry<Range,Boolean>> rangesForPatterns = Maps.newHashMap();
    
    private boolean allowLeadingWildcard = true;
    
    public ShardIndexQueryConfiguration() {
        super();
        // default model for this logic is DATAWAVE
        setModelName("DATAWAVE");
    }
    
    public ShardIndexQueryConfiguration(ShardIndexQueryConfiguration other) {
        super(other);
        this.setNormalizedTerms(other.getNormalizedTerms());
        this.setNormalizedPatterns(other.getNormalizedPatterns());
        this.setRangesForPatterns(other.getRangesForPatterns());
        this.setRangesForTerms(other.getRangesForTerms());
        this.setAllowLeadingWildcard(other.isAllowLeadingWildcard());
    }
    
    public ShardIndexQueryConfiguration(ShardIndexQueryTable logic, Query query) {
        this(logic.getConfig());
        setQuery(query);
    }
    
    public void setNormalizedTerms(Multimap<String,String> normalizedTerms) {
        this.normalizedTerms = normalizedTerms;
    }
    
    public Multimap<String,String> getNormalizedTerms() {
        return normalizedTerms;
    }
    
    public Multimap<String,String> getNormalizedPatterns() {
        return normalizedPatterns;
    }
    
    public void setNormalizedPatterns(Multimap<String,String> normalizedPatterns) {
        this.normalizedPatterns = normalizedPatterns;
    }
    
    public void setRangesForTerms(Map<Entry<String,String>,Range> rangesForTerms) {
        this.rangesForTerms = rangesForTerms;
    }
    
    public Map<Entry<String,String>,Range> getRangesForTerms() {
        return this.rangesForTerms;
    }
    
    public void setRangesForPatterns(Map<Entry<String,String>,Entry<Range,Boolean>> rangesForPatterns) {
        this.rangesForPatterns = rangesForPatterns;
    }
    
    public Map<Entry<String,String>,Entry<Range,Boolean>> getRangesForPatterns() {
        return this.rangesForPatterns;
    }
    
    public boolean isAllowLeadingWildcard() {
        return allowLeadingWildcard;
    }
    
    public void setAllowLeadingWildcard(boolean allowLeadingWildcard) {
        this.allowLeadingWildcard = allowLeadingWildcard;
    }
}
