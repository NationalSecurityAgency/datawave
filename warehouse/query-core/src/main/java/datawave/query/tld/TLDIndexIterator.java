package datawave.query.tld;

import datawave.query.iterator.logic.IndexIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

public class TLDIndexIterator extends IndexIterator {

    public static class Builder<B extends Builder<B>> extends IndexIterator.Builder<B> {

        Builder(Text field, Text value, SortedKeyValueIterator<Key,Value> source) {
            super(field, value, source);
        }

        public TLDIndexIterator build() {
            return new TLDIndexIterator(this);
        }
    }

    public static Builder<?> builder(Text field, Text value, SortedKeyValueIterator<Key,Value> source) {
        return new Builder(field, value, source);
    }

    protected TLDIndexIterator(Builder builder) {
        super(builder);
    }

    @Override
    protected Range buildIndexRange(Range r) {
        Key start = r.getStartKey();
        Key end = r.getEndKey();
        String endCf = (end == null || end.getColumnFamily() == null ? "" : end.getColumnFamily().toString());
        String startCf = (start == null || start.getColumnFamily() == null ? "" : start.getColumnFamily().toString());

        // if the end key inclusively includes a datatype/0UID or has datatype/0UID/0, then move the end key past the children
        if (!endCf.isEmpty() && (r.isEndKeyInclusive() || endCf.charAt(endCf.length() - 1) == '\0')) {
            String row = end.getRow().toString().intern();
            if (endCf.charAt(endCf.length() - 1) == '\0') {
                endCf = endCf.substring(0, endCf.length() - 1);
            }
            Key postDoc = new Key(row, endCf + "\uffff");
            r = new Range(r.getStartKey(), r.isStartKeyInclusive(), postDoc, false);
        }

        // if the start key is not inclusive, and we have a datatype/0UID, then move the start past the children thereof
        if (!r.isStartKeyInclusive() && !startCf.isEmpty()) {
            // we need to bump append 0xff to that byte array because we want to skip the children
            String row = start.getRow().toString();

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
