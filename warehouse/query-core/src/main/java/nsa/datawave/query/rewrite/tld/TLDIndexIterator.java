package nsa.datawave.query.rewrite.tld;

import nsa.datawave.query.rewrite.iterator.logic.IndexIterator;
import nsa.datawave.query.rewrite.jexl.functions.FieldIndexAggregator;
import nsa.datawave.query.rewrite.predicate.TimeFilter;
import nsa.datawave.query.util.TypeMetadata;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

public class TLDIndexIterator extends IndexIterator {
    
    public TLDIndexIterator(Text field, Text value, SortedKeyValueIterator<Key,Value> source, TimeFilter timeFilter) {
        this(field, value, source, timeFilter, null, false, Predicates.<Key> alwaysTrue(), new TLDFieldIndexAggregator(null, null));
    }
    
    public TLDIndexIterator(Text field, Text value, SortedKeyValueIterator<Key,Value> source, TimeFilter timeFilter, TypeMetadata typeMetadata,
                    boolean buildDocument, Predicate<Key> datatypeFilter, FieldIndexAggregator aggregator) {
        super(field, value, source, timeFilter, typeMetadata, buildDocument, datatypeFilter, aggregator);
    }
    
    @Override
    protected Range buildIndexRange(Range r) {
        Key start = r.getStartKey();
        Key end = r.getEndKey();
        String endCf = (end == null || end.getColumnFamily() == null ? "" : end.getColumnFamily().toString());
        String startCf = (start == null || start.getColumnFamily() == null ? "" : start.getColumnFamily().toString());
        
        // if the end key inclusively includes a datatype/0UID or has datatype/0UID/0, then move the end key past the children
        if (endCf.length() > 0 && (r.isEndKeyInclusive() || endCf.charAt(endCf.length() - 1) == '\0')) {
            String row = end.getRow().toString().intern();
            if (endCf.charAt(endCf.length() - 1) == '\0') {
                endCf = endCf.substring(0, endCf.length() - 1);
            }
            Key postDoc = new Key(row, endCf + "\uffff");
            r = new Range(r.getStartKey(), r.isStartKeyInclusive(), postDoc, false);
        }
        
        // if the start key is not inclusive, and we have a datatype/0UID, then move the start past the children thereof
        if (!r.isStartKeyInclusive() && startCf.length() > 0) {
            // we need to bump append 0xff to that byte array because we want to skip the children
            String row = start.getRow().toString().intern();
            
            Key postDoc = new Key(row, startCf + "\uffff");
            // if this puts us past the end of the range, then adjust appropriately
            if (r.contains(postDoc)) {
                r = new Range(postDoc, false, r.getEndKey(), r.isEndKeyInclusive());
            } else {
                r = new Range(r.getEndKey(), false, r.getEndKey().followingKey(PartialKey.ROW_COLFAM), false);
            }
        }
        
        return super.buildIndexRange(r);
    }
}
