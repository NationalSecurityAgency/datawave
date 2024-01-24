package datawave.query.config;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Range;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import datawave.query.tables.ShardIndexQueryTable;
import datawave.webservice.query.Query;

public class ShardIndexQueryConfiguration extends ShardQueryConfiguration {
    private static final long serialVersionUID = 7616552164239289739L;

    private Multimap<String,String> normalizedTerms = HashMultimap.create();
    private Multimap<String,String> normalizedPatterns = HashMultimap.create();

    private Map<Entry<String,String>,Range> rangesForTerms = Maps.newHashMap();
    private Map<Entry<String,String>,Entry<Range,Boolean>> rangesForPatterns = Maps.newHashMap();

    public ShardIndexQueryConfiguration(ShardIndexQueryTable logic, Query query) {
        this.setIndexTableName(logic.getIndexTableName());
        this.setFullTableScanEnabled(logic.isFullTableScanEnabled());
        this.setQuery(query);
        this.setMetadataTableName(logic.getModelTableName());
        this.setRealmSuffixExclusionPatterns(logic.getRealmSuffixExclusionPatterns());
        this.setModelName(logic.getModelName());
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
}
