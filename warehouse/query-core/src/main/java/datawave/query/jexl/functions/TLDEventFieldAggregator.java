package datawave.query.jexl.functions;

import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.tld.TLD;
import org.apache.accumulo.core.data.ByteSequence;

import java.util.ArrayList;

public class TLDEventFieldAggregator extends EventFieldAggregator {
    public TLDEventFieldAggregator(String field, EventDataQueryFilter filter, int maxNextCount) {
        super(field, filter, maxNextCount);
    }
    
    @Override
    protected ByteSequence parsePointer(ByteSequence columnFamily) {
        // find the null between the dataType and Uid
        ArrayList<Integer> nulls = TLD.instancesOf(0, columnFamily, 1);
        final int start = nulls.get(0) + 1;
        
        // uid is from the null byte to the end of the cf
        ByteSequence uid = columnFamily.subSequence(start, columnFamily.length());
        
        // find the end of the tld if it exists
        ArrayList<Integer> dots = TLD.instancesOf('.', uid);
        if (dots.size() > 2) {
            // reduce to the TLD
            return columnFamily.subSequence(0, start + dots.get(2));
        } else {
            // no reduction necessary, already tld
            return columnFamily;
        }
    }
}
