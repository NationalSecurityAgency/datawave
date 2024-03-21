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

/**
 * An iterator used to log the start and end of each method run by the {@link SortedKeyValueIterator} above it in the iterator stack. Logs the QueryID
 * associated with each method. Logs are written on the TServer where the iterator is running
 */
public class QueryLogIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

    private static final Logger log = Logger.getLogger(QueryLogIterator.class);
    private static final String CLASS_NAME = QueryLogIterator.class.getSimpleName();

    private String queryID;
    private SortedKeyValueIterator<Key,Value> source;
    private IteratorEnvironment env;

    /**
     * Default constructor
     */
    public QueryLogIterator() {
        // no-arg constructor
    }

    /**
     * Class copy constructor
     */
    public QueryLogIterator(QueryLogIterator other, IteratorEnvironment env) {
        this.source = other.source.deepCopy(env);
        this.env = other.env;
        this.queryID = other.queryID;
    }

    /**
     * Wraps the init() method of the iterator above it, logging the start and end of the method along with its query id.
     */
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {

        try {
            logStartOf("init()");
            this.queryID = options.get(QUERY_ID);
            this.source = source;
            this.env = env;
        } finally {
            logEndOf("init()");
        }
    }

    /**
     * Logs the query id and {@link SortedKeyValueIterator} method name before the method is run.
     */
    private void logStartOf(String methodName) {
        if (log.isInfoEnabled()) {
            log.info(CLASS_NAME + " " + methodName + " Started QueryID: " + this.queryID);
        }
    }

    /**
     * Logs the query id and {@link SortedKeyValueIterator} method name after the method is run.
     */
    private void logEndOf(String methodName) {
        if (log.isInfoEnabled()) {
            log.info(CLASS_NAME + " " + methodName + " Ended QueryID: " + this.queryID);
        }
    }

    /**
     * Wraps the hasTop() method of the iterator above it, logging the start and end of the method along with its query id.
     */
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

    /**
     * Wraps the next() method of the iterator above it, logging the start and end of the method along with its query id.
     */
    @Override
    public void next() throws IOException {
        try {
            logStartOf("next()");
            source.next();
        } finally {
            logEndOf("next()");
        }
    }

    /**
     * Wraps the getTopKey() method of the iterator above it, logging the start and end of the method along with its query id.
     */
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

    /**
     * Wraps the getTopValue() method of the iterator above it, logging the start and end of the method along with its query id.
     */
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

    /**
     * Wraps the deepCopy() method of the iterator above it, logging the start and end of the method along with its query id.
     */
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment iteratorEnvironment) {

        QueryLogIterator copy;

        try {
            logStartOf("deepCopy()");
            copy = new QueryLogIterator(this, this.env);
        } finally {
            logEndOf("deepCopy()");
        }
        return copy;
    }

    /**
     * Wraps the seek() method of the iterator above it, logging the start and end of the method along with its query id.
     */
    @Override
    public void seek(Range range, Collection<ByteSequence> collection, boolean b) throws IOException {

        try {
            logStartOf("seek()");
            this.source.seek(range, collection, b);
        } finally {
            logEndOf("seek()");
        }
    }

    /**
     * Returns a {@link org.apache.accumulo.core.iterators.OptionDescriber.IteratorOptions} object containing a description of the iterator and an option for
     * the QueryID.
     */
    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(QUERY_ID, "The QueryID to be logged as methods are invoked");

        return new IteratorOptions(getClass().getSimpleName(), "An iterator used to log the QueryID", options, null);
    }

    /**
     * Returns true if the options provided contains the QueryID.
     */
    @Override
    public boolean validateOptions(Map<String,String> options) {
        return options.containsKey(QUERY_ID);
    }
}
