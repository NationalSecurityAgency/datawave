package datawave.query.planner;

import datawave.query.CloseableIterable;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.QueryData;
import org.apache.commons.jexl2.parser.ASTJexlScript;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

public class ThreadedRangeBundler implements CloseableIterable<QueryData> {

    private final QueryData original;
    private final CloseableIterable<QueryPlan> ranges;
    private final long maxRanges;
    private final Query settings;
    private final ASTJexlScript queryTree;
    private final Collection<Comparator<QueryPlan>> queryPlanComparators;
    private final int numRangesToBuffer;
    private final long rangeBufferTimeoutMillis;
    private final long rangeBufferPollMillis;
    private final long maxRangeWaitMillis;
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
        this.maxRangeWaitMillis = builder.maxRangeWaitMillis;
        this.queryPlanComparators = builder.queryPlanComparators;
        this.numRangesToBuffer = builder.numRangesToBuffer;
        this.rangeBufferTimeoutMillis = builder.rangeBufferTimeoutMillis;
        this.rangeBufferPollMillis = builder.rangeBufferPollMillis;
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

    public int getNumRangesToBuffer() {
        return numRangesToBuffer;
    }

    public long getRangeBufferTimeoutMillis() {
        return rangeBufferTimeoutMillis;
    }

    public long getRangeBufferPollMillis() {
        return rangeBufferPollMillis;
    }

    public long getMaxRangeWaitMillis() {
        return maxRangeWaitMillis;
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
                .setMaxWaitValue(maxRangeWaitMillis)
                .setMaxWaitUnit(TimeUnit.MILLISECONDS)
                .setSettings(settings)
                .setQueryPlanComparators(queryPlanComparators)
                .setNumRangesToBuffer(numRangesToBuffer)
                .setRangeBufferTimeoutMillis(rangeBufferTimeoutMillis)
                .setRangeBufferPollMillis(rangeBufferPollMillis)
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
        private long maxRangeWaitMillis = 50L;
        private Collection<Comparator<QueryPlan>> queryPlanComparators;
        private int numRangesToBuffer;
        private long rangeBufferTimeoutMillis;
        private long rangeBufferPollMillis = 100L;

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

        public Builder setMaxRangeWaitMillis(long maxRangeWaitMillis) {
            this.maxRangeWaitMillis = maxRangeWaitMillis;
            return this;
        }

        public Builder setQueryPlanComparators(Collection<Comparator<QueryPlan>> queryPlanComparators) {
            this.queryPlanComparators = queryPlanComparators;
            return this;
        }

        public Builder setNumRangesToBuffer(int numRangesToBuffer) {
            this.numRangesToBuffer = numRangesToBuffer;
            return this;
        }

        public Builder setRangeBufferTimeoutMillis(long rangeBufferTimeoutMillis) {
            this.rangeBufferTimeoutMillis = rangeBufferTimeoutMillis;
            return this;
        }

        public Builder setRangeBufferPollMillis(long rangeBufferPollMillis) {
            this.rangeBufferPollMillis = rangeBufferPollMillis;
            return this;
        }

        /**
         * Builds and returns a new {@link ThreadedRangeBundler}. The following default values will be used unless specified otherwise in the builder.
         *
         * <ul>
         * <li>{@link ThreadedRangeBundler#maxRangeWaitMillis}: 50</li>
         * <li>{@link ThreadedRangeBundler#rangeBufferPollMillis}: 100</li>
         * </ul>
         *
         * @return the new {@link ThreadedRangeBundler}
         */
        public ThreadedRangeBundler build() {
            return new ThreadedRangeBundler(this);
        }
    }
}
