package datawave.next.scanner;

import org.apache.accumulo.core.data.Key;

import datawave.core.query.configuration.QueryData;

/**
 * Simple object that allows state to travel with a document id
 */
public class KeyWithContext implements Comparable<KeyWithContext> {

    private final Key key;
    private final QueryData context;
    private final String compareKey;

    /**
     * Default constructor
     *
     * @param key
     *            the record id
     * @param context
     *            the context
     */
    public KeyWithContext(Key key, QueryData context) {
        this(key, context, false);
    }

    public KeyWithContext(Key key, QueryData context, boolean buildCompareKey) {
        this.key = key;
        this.context = context;

        if (buildCompareKey) {
            // the ColumnFamily is datatype \0 uid, invert this for priority queue
            String cf = key.getColumnFamily().toString();
            int index = cf.indexOf('\u0000');
            compareKey = cf.substring(index + 1) + cf.substring(0, index);
        } else {
            compareKey = null;
        }
    }

    public Key getKey() {
        return key;
    }

    public QueryData getContext() {
        return context;
    }

    @Override
    public int compareTo(KeyWithContext o) {
        if (compareKey == null) {
            return key.compareTo(o.key);
        }

        return compareKey.compareTo(o.compareKey);
    }
}
