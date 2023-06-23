package datawave.query.ancestor;

import datawave.query.iterator.logic.IndexIterator;
import datawave.query.tld.TLD;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

public class AncestorIndexIterator extends IndexIterator {

    public static class Builder<B extends Builder<B>> extends IndexIterator.Builder<B> {

        Builder(Text field, Text value, SortedKeyValueIterator<Key,Value> source) {
            super(field, value, source);
        }

        public AncestorIndexIterator build() {
            return new AncestorIndexIterator(this);
        }
    }

    public static Builder<?> builder(Text field, Text value, SortedKeyValueIterator<Key,Value> source) {
        return new Builder(field, value, source);
    }

    protected AncestorIndexIterator(Builder builder) {
        super(builder);
    }

    @Override
    protected Range buildIndexRange(Range r) {
        Key start = r.getStartKey();
        Key end = r.getEndKey();
        String endCf = (end == null || end.getColumnFamily() == null ? "" : end.getColumnFamily().toString());
        String startCf = (start == null || start.getColumnFamily() == null ? "" : start.getColumnFamily().toString());

        // if the start key is inclusive, and contains a datatype/0UID, then move back to the top level ancestor
        if (r.isStartKeyInclusive() && !startCf.isEmpty()) {
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
