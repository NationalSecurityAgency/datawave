package datawave.query.config;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.accumulo.core.data.Range;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import datawave.core.query.configuration.CheckpointableQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.query.tables.ShardIndexQueryTable;
import datawave.webservice.query.Query;

public class ShardIndexQueryConfiguration extends ShardQueryConfiguration implements Serializable, CheckpointableQueryConfiguration {
    private static final long serialVersionUID = 7616552164239289739L;

    private Multimap<String,String> normalizedTerms = HashMultimap.create();
    private Multimap<String,String> normalizedPatterns = HashMultimap.create();

    private Map<Entry<String,String>,Range> rangesForTerms = Maps.newHashMap();
    private Map<Entry<String,String>,Entry<Range,Boolean>> rangesForPatterns = Maps.newHashMap();

    private boolean allowLeadingWildcard;

    public ShardIndexQueryConfiguration() {}

    public ShardIndexQueryConfiguration(ShardIndexQueryConfiguration other) {
        super(other);
        setNormalizedPatterns(other.getNormalizedPatterns());
        setNormalizedTerms(other.getNormalizedTerms());
        setRangesForPatterns(other.getRangesForPatterns());
        setRangesForTerms(other.getRangesForTerms());
        setAllowLeadingWildcard(other.isAllowLeadingWildcard());
    }

    public ShardIndexQueryConfiguration(ShardIndexQueryTable logic, Query query) {
        this(logic.getConfig());
        setQuery(query);
    }

    public ShardIndexQueryConfiguration(ShardIndexQueryConfiguration other, Collection<QueryData> queries) {
        super(other, queries);
        setNormalizedPatterns(other.getNormalizedPatterns());
        setNormalizedTerms(other.getNormalizedTerms());
        setRangesForPatterns(other.getRangesForPatterns());
        setRangesForTerms(other.getRangesForTerms());
        setAllowLeadingWildcard(other.isAllowLeadingWildcard());
    }

    /**
     * Factory method that instantiates a fresh ShardIndexQueryConfiguration
     *
     * @return - a clean ShardIndexQueryConfiguration
     */
    public static ShardIndexQueryConfiguration create() {
        return new ShardIndexQueryConfiguration();
    }

    /**
     * Factory method that returns a deep copy of the provided ShardIndexQueryConfiguration
     *
     * @param other
     *            - another instance of a ShardIndexQueryConfiguration
     * @return - copy of provided ShardIndexQueryConfiguration
     */
    public static ShardIndexQueryConfiguration create(ShardIndexQueryConfiguration other) {
        return new ShardIndexQueryConfiguration(other);
    }

    /**
     * Factory method that creates a ShardIndexQueryConfiguration deep copy from a ShardIndexQueryLogic
     *
     * @param shardQueryLogic
     *            - a configured ShardIndexQueryLogic
     * @return - a ShardIndexQueryConfiguration
     */
    public static ShardIndexQueryConfiguration create(ShardIndexQueryTable shardQueryLogic) {
        ShardIndexQueryConfiguration config = create(shardQueryLogic.getConfig());
        return config;
    }

    /**
     * Factory method that creates a ShardIndexQueryConfiguration from a ShardIndexQueryLogic and a Query
     *
     * @param shardQueryLogic
     *            - a configured ShardIndexQueryLogic
     * @param query
     *            - a configured Query object
     * @return - a ShardIndexQueryConfiguration
     */
    public static ShardIndexQueryConfiguration create(ShardIndexQueryTable shardQueryLogic, Query query) {
        ShardIndexQueryConfiguration config = create(shardQueryLogic);
        config.setQuery(query);
        return config;
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

    @Override
    public ShardIndexQueryConfiguration checkpoint() {
        // Create a new config that only contains what is needed to execute the ranges
        return new ShardIndexQueryConfiguration(this, getQueries());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        ShardIndexQueryConfiguration that = (ShardIndexQueryConfiguration) o;
        return Objects.equals(normalizedTerms, that.normalizedTerms) && Objects.equals(normalizedPatterns, that.normalizedPatterns)
                        && Objects.equals(rangesForTerms, that.rangesForTerms) && Objects.equals(rangesForPatterns, that.rangesForPatterns)
                        && Objects.equals(allowLeadingWildcard, that.allowLeadingWildcard);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), normalizedTerms, normalizedPatterns, rangesForTerms, rangesForPatterns, allowLeadingWildcard);
    }
}
