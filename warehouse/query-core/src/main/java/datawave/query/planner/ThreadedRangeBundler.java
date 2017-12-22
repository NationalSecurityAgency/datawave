package datawave.query.planner;

import datawave.query.CloseableIterable;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.QueryData;
import org.apache.commons.jexl2.parser.ASTJexlScript;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * 
 */
public class ThreadedRangeBundler implements CloseableIterable<QueryData> {
    protected final QueryData original;
    protected final CloseableIterable<QueryPlan> ranges;
    protected final long maxRanges;
    protected final Query settings;
    protected ThreadedRangeBundlerIterator iterator = null;
    protected ASTJexlScript queryTree;
    protected boolean docSpecificLimitOverride;
    protected int docsToCombine;
    private long maxRangeWaitMillis;
    protected Collection<Comparator<QueryPlan>> queryPlanComparators;
    protected int numRangesToBuffer;
    protected long rangeBufferTimeoutMillis;
    protected long rangeBufferPollMillis;
    
    private ThreadedRangeBundler(Builder builder) {
        this.original = builder.getOriginal();
        this.ranges = builder.getRanges();
        this.queryTree = builder.getQueryTree();
        this.maxRanges = builder.getMaxRanges();
        this.settings = builder.getSettings();
        this.docSpecificLimitOverride = builder.isDocSpecificLimitOverride();
        this.docsToCombine = builder.getDocsToCombine();
        this.maxRangeWaitMillis = builder.getMaxRangeWaitMillis();
        this.queryPlanComparators = builder.getQueryPlanComparators();
        this.numRangesToBuffer = builder.getNumRangesToBuffer();
        this.rangeBufferTimeoutMillis = builder.getRangeBufferTimeoutMillis();
        this.rangeBufferPollMillis = builder.getRangeBufferPollMillis();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Iterable#iterator()
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
                .setDocsToCombine(docsToCombine)
                .setMaxWaitValue(maxRangeWaitMillis)
                .setMaxWaitUnit(TimeUnit.MILLISECONDS)
                .setSettings(settings)
                .setDocSpecificLimitOverride(docSpecificLimitOverride)
                .setQueryPlanComparators(queryPlanComparators)
                .setNumRangesToBuffer(numRangesToBuffer)
                .setRangeBufferTimeoutMillis(rangeBufferTimeoutMillis)
                .setRangeBufferPollMillis(rangeBufferPollMillis)
                .build();
        // @formatter:on
        
        return iterator;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        if (null != iterator) {
            iterator.close();
        }
    }
    
    public static class Builder {
        protected QueryData original;
        protected CloseableIterable<QueryPlan> ranges;
        protected long maxRanges;
        protected Query settings;
        protected ASTJexlScript queryTree;
        protected boolean docSpecificLimitOverride = false;
        protected int docsToCombine = -1;
        private long maxRangeWaitMillis = 50;
        protected Collection<Comparator<QueryPlan>> queryPlanComparators = null;
        protected int numRangesToBuffer = 0;
        protected long rangeBufferTimeoutMillis = 0;
        protected long rangeBufferPollMillis = 100;
        
        public QueryData getOriginal() {
            return original;
        }
        
        public Builder setOriginal(QueryData original) {
            this.original = original;
            return this;
        }
        
        public CloseableIterable<QueryPlan> getRanges() {
            return ranges;
        }
        
        public Builder setRanges(CloseableIterable<QueryPlan> ranges) {
            this.ranges = ranges;
            return this;
        }
        
        public long getMaxRanges() {
            return maxRanges;
        }
        
        public Builder setMaxRanges(long maxRanges) {
            this.maxRanges = maxRanges;
            return this;
        }
        
        public Query getSettings() {
            return settings;
        }
        
        public Builder setSettings(Query settings) {
            this.settings = settings;
            return this;
        }
        
        public ASTJexlScript getQueryTree() {
            return queryTree;
        }
        
        public Builder setQueryTree(ASTJexlScript queryTree) {
            this.queryTree = queryTree;
            return this;
        }
        
        public boolean isDocSpecificLimitOverride() {
            return docSpecificLimitOverride;
        }
        
        public Builder setDocSpecificLimitOverride(boolean docSpecificLimitOverride) {
            this.docSpecificLimitOverride = docSpecificLimitOverride;
            return this;
        }
        
        public int getDocsToCombine() {
            return docsToCombine;
        }
        
        public Builder setDocsToCombine(int docsToCombine) {
            this.docsToCombine = docsToCombine;
            return this;
        }
        
        public long getMaxRangeWaitMillis() {
            return maxRangeWaitMillis;
        }
        
        public Builder setMaxRangeWaitMillis(long maxRangeWaitMillis) {
            this.maxRangeWaitMillis = maxRangeWaitMillis;
            return this;
        }
        
        public Collection<Comparator<QueryPlan>> getQueryPlanComparators() {
            return queryPlanComparators;
        }
        
        public Builder setQueryPlanComparators(Collection<Comparator<QueryPlan>> queryPlanComparators) {
            this.queryPlanComparators = queryPlanComparators;
            return this;
        }
        
        public int getNumRangesToBuffer() {
            return numRangesToBuffer;
        }
        
        public Builder setNumRangesToBuffer(int numRangesToBuffer) {
            this.numRangesToBuffer = numRangesToBuffer;
            return this;
        }
        
        public long getRangeBufferTimeoutMillis() {
            return rangeBufferTimeoutMillis;
        }
        
        public Builder setRangeBufferTimeoutMillis(long rangeBufferTimeoutMillis) {
            this.rangeBufferTimeoutMillis = rangeBufferTimeoutMillis;
            return this;
        }
        
        public long getRangeBufferPollMillis() {
            return rangeBufferPollMillis;
        }
        
        public Builder setRangeBufferPollMillis(long rangeBufferPollMillis) {
            this.rangeBufferPollMillis = rangeBufferPollMillis;
            return this;
        }
        
        public ThreadedRangeBundler build() {
            return new ThreadedRangeBundler(this);
        }
    }
}
