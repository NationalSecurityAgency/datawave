package datawave.query.planner;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;

import org.apache.commons.jexl3.parser.ASTJexlScript;

import datawave.core.query.configuration.QueryData;
import datawave.microservice.query.Query;
import datawave.query.CloseableIterable;

public class ThreadedRangeBundler implements CloseableIterable<QueryData> {

    private final QueryData original;
    private final CloseableIterable<QueryPlan> ranges;
    private final long maxRanges;
    private final Query settings;
    private final ASTJexlScript queryTree;
    private final Collection<Comparator<QueryPlan>> queryPlanComparators;
    private ThreadedRangeBundlerIterator iterator;

    /**
     * Creates and returns a new {@link Builder}.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    private ThreadedRangeBundler(Builder builder) {
        this.original = builder.original;
        this.ranges = builder.ranges;
        this.queryTree = builder.queryTree;
        this.maxRanges = builder.maxRanges;
        this.settings = builder.settings;
        this.queryPlanComparators = builder.queryPlanComparators;
    }

    public QueryData getOriginal() {
        return original;
    }

    public CloseableIterable<QueryPlan> getRanges() {
        return ranges;
    }

    public long getMaxRanges() {
        return maxRanges;
    }

    public Query getSettings() {
        return settings;
    }

    public ASTJexlScript getQueryTree() {
        return queryTree;
    }

    public Collection<Comparator<QueryPlan>> getQueryPlanComparators() {
        return queryPlanComparators;
    }

    /**
     * Builds and returns a new {@link ThreadedRangeBundlerIterator}.
     *
     * @return the new {@link ThreadedRangeBundlerIterator}
     * @throws IllegalStateException
     *             if ThreadedRangeBundler has already been called once
     */
    @Override
    public ThreadedRangeBundlerIterator iterator() {
        if (null != iterator) {
            throw new IllegalStateException("iterator() was already called once");
        }

        // @formatter:off
        iterator = new ThreadedRangeBundlerIterator.Builder()
                .setOriginal(original)
                .setQueryTree(queryTree)
                .setRanges(ranges)
                .setMaxRanges(maxRanges)
                .setSettings(settings)
                .setQueryPlanComparators(queryPlanComparators)
                .build();
        // @formatter:on

        return iterator;
    }

    /**
     * Closes the underlying iterator.
     *
     * @throws IOException
     *             when an error occurs while closing the underlying iterator
     */
    @Override
    public void close() throws IOException {
        if (null != iterator) {
            iterator.close();
        }
    }

    /**
     * Builder class for {@link ThreadedRangeBundler}.
     */
    public static class Builder {

        private QueryData original;
        private CloseableIterable<QueryPlan> ranges;
        private long maxRanges;
        private Query settings;
        private ASTJexlScript queryTree;
        private boolean docSpecificLimitOverride;
        private Collection<Comparator<QueryPlan>> queryPlanComparators;

        public Builder setOriginal(QueryData original) {
            this.original = original;
            return this;
        }

        public Builder setRanges(CloseableIterable<QueryPlan> ranges) {
            this.ranges = ranges;
            return this;
        }

        public Builder setMaxRanges(long maxRanges) {
            this.maxRanges = maxRanges;
            return this;
        }

        public Builder setSettings(Query settings) {
            this.settings = settings;
            return this;
        }

        public Builder setQueryTree(ASTJexlScript queryTree) {
            this.queryTree = queryTree;
            return this;
        }

        public Builder setQueryPlanComparators(Collection<Comparator<QueryPlan>> queryPlanComparators) {
            this.queryPlanComparators = queryPlanComparators;
            return this;
        }

        /**
         * Builds and returns a new {@link ThreadedRangeBundler}.
         *
         * @return the new {@link ThreadedRangeBundler}
         */
        public ThreadedRangeBundler build() {
            return new ThreadedRangeBundler(this);
        }
    }
}
