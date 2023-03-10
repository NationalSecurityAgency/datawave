package datawave.ingest.table.aggregator;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

public class CardinalityAggregator extends PropogatingCombiner {
    
    public static final Value EMPTY_VALUE = new Value(new byte[0]);
    private static final Logger log = Logger.getLogger(CardinalityAggregator.class);
    
    @Override
    public Value reduce(Key key, Iterator<Value> iter) {
        if (log.isTraceEnabled())
            log.trace("has next ? " + iter.hasNext());
        
        try {
            if (iter.hasNext()) {
                HyperLogLogPlus combinedHllp = HyperLogLogPlus.Builder.build(iter.next().get());
                while (iter.hasNext()) {
                    combinedHllp.addAll(HyperLogLogPlus.Builder.build(iter.next().get()));
                }
                return new Value(combinedHllp.getBytes());
            }
        } catch (IOException | CardinalityMergeException e) {
            throw new RuntimeException("Unexpected error aggregating cardinalities " + e.toString(), e);
        }
        
        this.propogate = false;
        return EMPTY_VALUE;
    }
    
    @Override
    public void reset() {
        if (log.isDebugEnabled())
            log.debug("Resetting CardinalityAggregator");
        this.propogate = true;
    }
}
