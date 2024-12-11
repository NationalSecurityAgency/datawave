package datawave.query.discovery;

import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;
import java.util.StringJoiner;

import com.google.common.collect.Multimap;

import datawave.core.query.configuration.QueryData;
import datawave.microservice.query.Query;
import datawave.query.config.ShardIndexQueryConfiguration;
import datawave.query.jexl.LiteralRange;

/**
 * Adds the ability to hold on to two multimaps. They map literals and patterns to the fields they were associated with in the query.
 */
public class DiscoveryQueryConfiguration extends ShardIndexQueryConfiguration implements Serializable {
    private Multimap<String,String> literals, patterns;
    private Multimap<String,LiteralRange<String>> ranges;
    private boolean separateCountsByColVis = false;
    private boolean showReferenceCount = false;
    private boolean sumCounts = false;

    public DiscoveryQueryConfiguration() {}

    public DiscoveryQueryConfiguration(DiscoveryQueryConfiguration other) {
        super(other);
        setSeparateCountsByColVis(other.separateCountsByColVis);
        setShowReferenceCount(other.showReferenceCount);
        setLiterals(other.literals);
        setPatterns(other.patterns);
        setRanges(other.ranges);
    }

    public DiscoveryQueryConfiguration(DiscoveryLogic logic, Query query) {
        this(logic.getConfig());
        setQuery(query);
    }

    public DiscoveryQueryConfiguration(DiscoveryQueryConfiguration other, Collection<QueryData> queries) {
        super(other, queries);
        setSeparateCountsByColVis(other.separateCountsByColVis);
        setShowReferenceCount(other.showReferenceCount);
        setLiterals(other.literals);
        setPatterns(other.patterns);
        setRanges(other.ranges);
    }

    /**
     * Factory method that instantiates a fresh DiscoveryQueryConfiguration
     *
     * @return - a clean DiscoveryQueryConfiguration
     */
    public static DiscoveryQueryConfiguration create() {
        return new DiscoveryQueryConfiguration();
    }

    /**
     * Factory method that returns a deep copy of the provided DiscoveryQueryConfiguration
     *
     * @param other
     *            - another instance of a DiscoveryQueryConfiguration
     * @return - copy of provided DiscoveryQueryConfiguration
     */
    public static DiscoveryQueryConfiguration create(DiscoveryQueryConfiguration other) {
        return new DiscoveryQueryConfiguration(other);
    }

    /**
     * Factory method that creates a DiscoveryQueryConfiguration deep copy from a DiscoveryQueryLogic
     *
     * @param shardQueryLogic
     *            - a configured DiscoveryQueryLogic
     * @return - a DiscoveryQueryConfiguration
     */
    public static DiscoveryQueryConfiguration create(DiscoveryLogic shardQueryLogic) {
        DiscoveryQueryConfiguration config = create(shardQueryLogic.getConfig());
        return config;
    }

    /**
     * Factory method that creates a DiscoveryQueryConfiguration from a DiscoveryQueryLogic and a Query
     *
     * @param shardQueryLogic
     *            - a configured DiscoveryQueryLogic
     * @param query
     *            - a configured Query object
     * @return - a DiscoveryQueryConfiguration
     */
    public static DiscoveryQueryConfiguration create(DiscoveryLogic shardQueryLogic, Query query) {
        DiscoveryQueryConfiguration config = create(shardQueryLogic);
        config.setQuery(query);
        return config;
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

    public boolean getShowReferenceCount() {
        return showReferenceCount;
    }

    public boolean getSumCounts() {
        return sumCounts;
    }

    public void setSeparateCountsByColVis(boolean separateCountsByColVis) {
        this.separateCountsByColVis = separateCountsByColVis;
    }

    public void setShowReferenceCount(boolean showReferenceCount) {
        this.showReferenceCount = showReferenceCount;

    }

    public void setSumCounts(boolean sumCounts) {
        this.sumCounts = sumCounts;
    }

    @Override
    public DiscoveryQueryConfiguration checkpoint() {
        // Create a new config that only contains what is needed to execute the specified ranges
        return new DiscoveryQueryConfiguration(this, getQueries());
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

    @Override
    public String toString() {
        return new StringJoiner(", ", DiscoveryQueryConfiguration.class.getSimpleName() + "[", "]").add("literals=" + literals).add("patterns=" + patterns)
                        .add("ranges=" + ranges).add("separateCountsByColVis=" + separateCountsByColVis).add("showReferenceCount=" + showReferenceCount)
                        .add("sumCounts=" + sumCounts).toString();
    }
}
