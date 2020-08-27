package datawave.query.jexl.functions;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.tld.TLD;
import datawave.query.util.Tuple2;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class EventFieldAggregator extends IdentityAggregator {
    public EventFieldAggregator(String field, EventDataQueryFilter filter, int maxNextCount) {
        super(Collections.singleton(field), filter, maxNextCount);
    }
    
    @Override
    protected Tuple2<String,String> parserFieldNameValue(Key topKey) {
        String cq = topKey.getColumnQualifier().toString();
        int nullIndex1 = cq.indexOf('\u0000');
        String field = cq.substring(0, nullIndex1);
        String value = cq.substring(nullIndex1 + 1);
        return new Tuple2<>(field, value);
    }
    
    @Override
    protected ByteSequence parseFieldNameValue(ByteSequence cf, ByteSequence cq) {
        ArrayList<Integer> nulls = TLD.instancesOf(0, cq, 1);
        final int startFv = nulls.get(0) + 1;
        final int stopFn = nulls.get(0);
        
        byte[] fnFv = new byte[cq.length()];
        System.arraycopy(cq.getBackingArray(), 0, fnFv, 0, stopFn);
        System.arraycopy(cq.getBackingArray(), startFv, fnFv, stopFn + 1, cq.length() - startFv);
        
        return new ArrayByteSequence(fnFv);
    }
    
    @Override
    protected ByteSequence getPointerData(Key key) {
        return key.getColumnFamilyData();
    }
    
    @Override
    protected ByteSequence parsePointer(ByteSequence columnFamily) {
        return columnFamily;
    }
    
    @Override
    public Key apply(SortedKeyValueIterator<Key,Value> itr, Document doc, AttributeFactory attrs) throws IOException {
        Key result = super.apply(itr, doc, attrs);
        
        // for each thing in the doc, mark it as to-keep false because it will ultimately come from the document aggregation, otherwise there will be duplicates
        for (Attribute<?> attr : doc.getDictionary().values()) {
            attr.setToKeep(false);
        }
        
        return result;
    }
}
