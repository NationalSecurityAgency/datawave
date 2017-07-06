package datawave.query.planner;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import datawave.query.CloseableIterable;
import org.apache.commons.jexl2.parser.ASTJexlScript;

import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.QueryData;

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
    protected boolean docSpecificLimitOverride = false;
    protected int docsToCombine;
    private long maxRangeWaitMillis = 50;
    
    public ThreadedRangeBundler(QueryData original, ASTJexlScript queryTree, CloseableIterable<QueryPlan> ranges, long maxRanges, Query settings) {
        this(original, queryTree, ranges, maxRanges, -1, settings, false, 50);
    }
    
    public ThreadedRangeBundler(QueryData original, ASTJexlScript queryTree, CloseableIterable<QueryPlan> ranges, long maxRanges, Query settings,
                    boolean docSpecificLimitOverride) {
        this(original, queryTree, ranges, maxRanges, -1, settings, false, 50);
    }
    
    public ThreadedRangeBundler(QueryData original, ASTJexlScript queryTree, CloseableIterable<QueryPlan> ranges, long maxRanges, int docsToCombine,
                    Query settings) {
        this(original, queryTree, ranges, maxRanges, docsToCombine, settings, false, 50);
    }
    
    public ThreadedRangeBundler(QueryData original, ASTJexlScript queryTree, CloseableIterable<QueryPlan> ranges, long maxRanges, int docsToCombine,
                    Query settings, boolean docSpecificLimitOverride, long maxRangeWaitMillis) {
        this.original = original;
        this.ranges = ranges;
        this.queryTree = queryTree;
        this.maxRanges = maxRanges;
        this.settings = settings;
        this.docSpecificLimitOverride = docSpecificLimitOverride;
        this.docsToCombine = docsToCombine;
        this.maxRangeWaitMillis = maxRangeWaitMillis;
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
        
        iterator = new ThreadedRangeBundlerIterator(original, queryTree, ranges, maxRanges, docsToCombine, maxRangeWaitMillis, TimeUnit.MILLISECONDS, settings,
                        docSpecificLimitOverride);
        
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
    
}
