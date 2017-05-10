package datawave.ingest.table.aggregator;

import java.util.TreeSet;

import datawave.ingest.protobuf.TermWeight;

import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * An Aggregator to merge together a list of term offsets and one normalized term frequency This aggregator does <b>not</b> allow duplicate term offsets and
 * will do its best to remove any duplicate offsets and correct the normalized term frequency.
 * 
 * 
 * 
 */
public class TextIndexAggregator extends PropogatingCombiner {
    private static final Logger log = Logger.getLogger(TextIndexAggregator.class);
    
    private TreeSet<Integer> offsets = new TreeSet<Integer>();
    private TermWeight.Info.Builder builder = TermWeight.Info.newBuilder();
    
    @Override
    public Value aggregate() {
        for (Integer offset : offsets) {
            builder.addTermOffset(offset);
        }
        
        return new Value(builder.build().toByteArray());
    }
    
    /**
     * Determines whether or not to propogate the key depending on the result of the value
     *
     * @return
     */
    public boolean propogateKey() {
        return true;
    }
    
    @Override
    public void collect(Value value) {
        // Make sure we don't aggregate something else
        if (value == null || value.get().length == 0) {
            return;
        }
        
        TermWeight.Info info;
        
        try {
            info = TermWeight.Info.parseFrom(value.get());
        } catch (InvalidProtocolBufferException e) {
            log.error("Value passed to aggregator was not of type TermWeight.Info", e);
            return;
        }
        
        // Add each offset into the list maintaining sorted order
        for (int offset : info.getTermOffsetList()) {
            offsets.add(offset);
        }
    }
    
    @Override
    public void reset() {
        this.offsets.clear();
        this.builder = TermWeight.Info.newBuilder();
    }
    
}
