package nsa.datawave.query.rewrite.iterator.filter;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import nsa.datawave.edge.util.EdgeKey;
import nsa.datawave.query.config.RewriteEdgeQueryConfiguration;

/**
 *
 */
public class DateTypeFilter extends Filter {
    protected RewriteEdgeQueryConfiguration.dateType dateType = RewriteEdgeQueryConfiguration.dateType.ACQUISITION;
    
    protected boolean[] state;
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        DateTypeFilter result = (DateTypeFilter) super.deepCopy(env);
        result.dateType = dateType;
        result.state = state;
        
        return result;
    }
    
    /**
     * Method to setup expression from the iterator options for evaluation.
     *
     * @param options
     */
    private void initOptions(Map<String,String> options) throws IOException {
        
        String e = options.get(RewriteEdgeQueryConfiguration.DATE_RANGE_TYPE);
        if (RewriteEdgeQueryConfiguration.dateType.ACTIVITY.name().equals(e) || RewriteEdgeQueryConfiguration.dateType.ACTIVITY_LOAD.name().equals(e)) {
            dateType = RewriteEdgeQueryConfiguration.dateType.ACTIVITY;
        } else if (RewriteEdgeQueryConfiguration.dateType.ANY.name().equals(e) || RewriteEdgeQueryConfiguration.dateType.ANY_LOAD.name().equals(e)) {
            dateType = RewriteEdgeQueryConfiguration.dateType.ANY;
        } else { // we default to acquisition
            dateType = RewriteEdgeQueryConfiguration.dateType.ACQUISITION;
        }
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws java.io.IOException {
        super.init(source, options, env);
        initOptions(options);
        
        // Pre compute a state transition table so that the accept method can be as fast as possible
        state = new boolean[4];
        
        for (int i = 0; i < state.length; i++) {
            state[i] = true;
        }
        
        if (dateType == RewriteEdgeQueryConfiguration.dateType.ACQUISITION) {
            state[EdgeKey.DATE_TYPE.ACTIVITY_ONLY.ordinal()] = false;
        } else if (dateType == RewriteEdgeQueryConfiguration.dateType.ACTIVITY) {
            state[EdgeKey.DATE_TYPE.ACQUISITION_ONLY.ordinal()] = false;
            state[EdgeKey.DATE_TYPE.OLD_ACQUISITION.ordinal()] = false;
        }
    }
    
    /**
     * Determines if the edge key matches the desired date type.<br>
     * <strong>note:</strong> Some edges qualify as both an acquisition and an activity edge. Hence, can't simply negate the return value of isAcquisitionEdge
     * or isActivityEdge.
     *
     * @param k
     * @param V
     * @return boolean - true if it is a match.
     */
    @Override
    public boolean accept(Key k, Value V) {
        
        return (state[EdgeKey.getDateType(k).ordinal()]);
    }
    
}
