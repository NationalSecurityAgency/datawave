package datawave.query.ancestor;

import datawave.query.iterator.logic.IndexIterator;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.jexl.functions.IdentityAggregator;
import datawave.query.predicate.TimeFilter;
import datawave.query.tld.TLD;
import datawave.query.util.TypeMetadata;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

public class AncestorIndexIterator extends IndexIterator {
    
    public AncestorIndexIterator(Text field, Text value, SortedKeyValueIterator<Key,Value> source, TimeFilter timeFilter) {
        this(field, value, source, timeFilter, null, false, Predicates.<Key> alwaysTrue(), new IdentityAggregator(null, null));
    }
    
    public AncestorIndexIterator(Text field, Text value, SortedKeyValueIterator<Key,Value> source, TimeFilter timeFilter, TypeMetadata typeMetadata,
                    boolean buildDocument, Predicate<Key> datatypeFilter, FieldIndexAggregator aggregator) {
        super(field, value, source, timeFilter, typeMetadata, buildDocument, datatypeFilter, aggregator);
    }
    
    @Override
    protected Range buildIndexRange(Range r) {
        Key start = r.getStartKey();
        Key end = r.getEndKey();
        String endCf = (end == null || end.getColumnFamily() == null ? "" : end.getColumnFamily().toString());
        String startCf = (start == null || start.getColumnFamily() == null ? "" : start.getColumnFamily().toString());
        
        // if the start key is inclusive, and contains a datatype/0UID, then move back to the top level ancestor
        if (r.isStartKeyInclusive() && startCf.length() > 0) {
            // parse out the uid and replace with the root parent uid
            int index = startCf.indexOf('\0');
            if (index > 0) {
                String datatype = startCf.substring(0, index);
                String uid = startCf.substring(index + 1);
                uid = TLD.parseRootPointerFromId(uid);
                startCf = datatype + '\0' + uid;
                
                // we need to bump append 0xff to that byte array because we want to skip the children
                String row = start.getRow().toString();
                Key tldDoc = new Key(row, startCf);
                r = new Range(tldDoc, true, r.getEndKey(), r.isEndKeyInclusive());
            }
        }
        
        return super.buildIndexRange(r);
    }
}
