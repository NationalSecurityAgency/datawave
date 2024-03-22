package datawave.query.discovery;

import java.util.StringJoiner;

import com.google.common.collect.Multimap;

import datawave.query.config.ShardIndexQueryConfiguration;
import datawave.query.jexl.LiteralRange;
import datawave.query.tables.ShardIndexQueryTable;
import datawave.webservice.query.Query;

/**
 * Adds the ability to hold on to two multimaps. They map literals and patterns to the fields they were associated with in the query.
 */
public class DiscoveryQueryConfiguration extends ShardIndexQueryConfiguration {
    private Multimap<String,String> literals, patterns;
    private Multimap<String,LiteralRange<String>> ranges;
    private boolean separateCountsByColVis = false;
    private boolean showReferenceCount = false;
    private boolean sumCounts = false;

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

    public boolean getSeparateCountsByColVis() {
        return separateCountsByColVis;
    }

    public void setSeparateCountsByColVis(boolean separateCountsByColVis) {
        this.separateCountsByColVis = separateCountsByColVis;
    }

    public boolean getShowReferenceCount() {
        return showReferenceCount;
    }

    public void setShowReferenceCount(boolean showReferenceCount) {
        this.showReferenceCount = showReferenceCount;
    }

    public boolean getSumCounts() {
        return sumCounts;
    }

    public void setSumCounts(boolean summarize) {
        this.sumCounts = summarize;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DiscoveryQueryConfiguration.class.getSimpleName() + "[", "]").add("literals=" + literals).add("patterns=" + patterns)
                        .add("ranges=" + ranges).add("separateCountsByColVis=" + separateCountsByColVis).add("showReferenceCount=" + showReferenceCount)
                        .add("sumCounts=" + sumCounts).toString();
    }
}
