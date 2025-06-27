package datawave.next.scanner;

import java.util.Set;

import org.apache.accumulo.core.data.Key;

import datawave.core.query.configuration.QueryData;

/**
 * Support for batching
 */
public class BulkKeyWithContext extends KeyWithContext {

    private final Set<Key> keys;

    public BulkKeyWithContext(Key key, Set<Key> keys, QueryData context) {
        super(key, context);
        this.keys = keys;
    }

    public BulkKeyWithContext(Key key, Set<Key> keys, QueryData context, boolean buildCompareKey) {
        super(key, context, buildCompareKey);
        this.keys = keys;
    }

    public Set<Key> getKeys() {
        return keys;
    }
}
