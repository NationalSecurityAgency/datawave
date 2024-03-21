package datawave.query.iterator;

import static datawave.query.iterator.QueryOptions.QUERY_ID;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

public class QueryLogIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

    private static final Logger log = Logger.getLogger(QueryLogIterator.class);

    private static final String CLASS_NAME = QueryLogIterator.class.getSimpleName();
    private String queryID;
    private SortedKeyValueIterator<Key,Value> source;
    private IteratorEnvironment myEnvironment;

    public QueryLogIterator() {}

    public QueryLogIterator(QueryLogIterator other, IteratorEnvironment env) {
        this.myEnvironment = other.myEnvironment;
        this.queryID = other.queryID;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {

        try {
            this.queryID = options.get(QUERY_ID);
            this.source = source;
            this.myEnvironment = env;
            logStartOf("init()");
        } finally {
            logEndOf("init()");
        }
    }

    private void logStartOf(String methodName) {
        if (log.isInfoEnabled()) {
            log.info(CLASS_NAME + " " + methodName + " Started QueryID: " + this.queryID);
        }
    }

    private void logEndOf(String methodName) {
        if (log.isInfoEnabled()) {
            log.info(CLASS_NAME + " "  + methodName + " Ended QueryID: " + this.queryID);
        }
    }

    @Override
    public boolean hasTop() {

        boolean result;

        try {
            logStartOf("hasTop()");
            result = source.hasTop();
        } finally {
            logEndOf("hasTop()");
        }
        return result;
    }

    @Override
    public void next() throws IOException {
        try {
            logStartOf("next()");
            source.next();
        } finally {
            logEndOf("next()");
        }
    }

    @Override
    public Key getTopKey() {
        Key k;
        try {
            logStartOf("getTopKey()");
            k = source.getTopKey();
        } finally {
            logEndOf("getTopKey()");
        }
        return k;
    }

    @Override
    public Value getTopValue() {
        Value v;
        try {
            logStartOf("getTopValue()");
            v = source.getTopValue();
        } finally {
            logEndOf("getTopValue()");
        }
        return v;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment iteratorEnvironment) {

        QueryLogIterator copy;

        try {
            logStartOf("deepCopy()");
            copy = new QueryLogIterator(this, this.myEnvironment);
        } finally {
            logEndOf("deepCopy()");
        }
        return copy;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> collection, boolean b) throws IOException {

        try {
            logStartOf("seek()");
            this.source.seek(range, collection, b);
        } finally {
            logEndOf("seek()");
        }
    }

    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(QUERY_ID, "The QueryID to be logged as methods are invoked");

        return new IteratorOptions(getClass().getSimpleName(), "An iterator used to log the QueryID", options, null);
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        return options.containsKey(QUERY_ID);
    }
}
