package datawave.next.scanner;

import org.apache.accumulo.core.data.Key;

import datawave.core.query.configuration.QueryData;

/**
 * Simple object that allows state to travel with a document id
 */
public class KeyWithContext {
    private final Key key;
    private final QueryData context;

    public KeyWithContext(Key key, QueryData context) {
        this.key = key;
        this.context = context;
    }

    public Key getKey() {
        return key;
    }

    public QueryData getContext() {
        return context;
    }
}
