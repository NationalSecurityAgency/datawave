package datawave.query.iterator;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static datawave.query.iterator.QueryOptions.QUERY_ID;

public class QueryLogIterator implements SortedKeyValueIterator<Key, Value>{

    protected static final Logger log = Logger.getLogger(QueryLogIterator.class);

    protected SortedKeyValueIterator<Key,Value> source;
    protected SortedKeyValueIterator<Key,Value> sourceForDeepCopies;
    protected Map<String,String> queryOptions;
    protected NestedIterator<Key> initKeySource, seekKeySource;
    protected IteratorEnvironment myEnvironment;

    protected Range range;

    protected Key key;
    protected Value value;
    protected YieldCallback<Key> yield;

    public QueryLogIterator() { }

    public QueryLogIterator(QueryLogIterator other, IteratorEnvironment env) {
        this.source = other.source.deepCopy(env);
        this.sourceForDeepCopies = source.deepCopy(env);
        this.initKeySource = other.initKeySource;
        this.seekKeySource = other.seekKeySource;
        this.myEnvironment = other.myEnvironment;
        this.queryOptions = other.queryOptions;

    }
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {

        try{
            if (log.isInfoEnabled()) {
                log.info("QueryLogIterator: init() Started QueryID: " + options.get(QUERY_ID));
            }
            this.queryOptions = options;
            this.myEnvironment = env;
            this.source = source;
        }
        finally{
            if (log.isInfoEnabled()) {
                log.info("QueryLogIterator: init() Ended QueryID: " + options.get(QUERY_ID));
            }
        }
    }

    @Override
    public boolean hasTop() {

        boolean result;

        try {
            if(log.isInfoEnabled()){
                log.info("QueryLogIterator: hasTop() Started QueryID: " + this.queryOptions.get(QUERY_ID));
            }
            result = source.hasTop();
        }
        finally {
            if (log.isInfoEnabled()) {
                log.info("QueryLogIterator: hasTop() Ended QueryID: " + this.queryOptions.get(QUERY_ID));
            }
        }
        return result;
    }

    @Override
    public void next() throws IOException {
        try {
            if(log.isInfoEnabled()){
                log.info("QueryLogIterator: next() Started QueryID: " + this.queryOptions.get(QUERY_ID));
            }
            source.next();
        }
        finally {
            if (log.isInfoEnabled()) {
                log.info("QueryLogIterator: next() Ended QueryID: " + this.queryOptions.get(QUERY_ID));
            }
        }
    }

    @Override
    public Key getTopKey() {
        Key k;
        try {
            if(log.isInfoEnabled()){
                log.info("QueryLogIterator: getTopKey() Started QueryID: " + this.queryOptions.get(QUERY_ID));
            }
            k = source.getTopKey();
        }
        finally {
            if (log.isInfoEnabled()) {
                log.info("QueryLogIterator: getTopKey() Ended QueryID: " + this.queryOptions.get(QUERY_ID));
            }
        }
        return k;
    }

    @Override
    public Value getTopValue() {
        Value v;
        try {
            if(log.isInfoEnabled()){
                log.info("QueryLogIterator: getTopValue() Started QueryID: " + this.queryOptions.get(QUERY_ID));
            }
            v = source.getTopValue();
        }
        finally {
            if (log.isInfoEnabled()) {
                log.info("QueryLogIterator: getTopValue() Ended QueryID: " + this.queryOptions.get(QUERY_ID));
            }
        }
        return v;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment iteratorEnvironment) {

        QueryLogIterator newQLI = null;

        try {
            if(log.isInfoEnabled()){
                log.info("QueryLogIterator: deepCopy() Started QueryID: " + this.queryOptions.get(QUERY_ID));
                newQLI = new QueryLogIterator(this, this.myEnvironment);
            }
        }
        finally {
            if (log.isInfoEnabled()) {
                log.info("QueryLogIterator: deepCopy() Ended QueryID: " + this.queryOptions.get(QUERY_ID));
            }
        }
        return newQLI;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> collection, boolean b) throws IOException {

        try {
            if(log.isInfoEnabled()){
                log.info("QueryLogIterator: seek() Started QueryID: " + this.queryOptions.get(QUERY_ID));
                this.source.seek(range, collection, b);
            }
        }
        finally {
            if (log.isInfoEnabled()) {
                log.info("QueryLogIterator: seek() Ended QueryID: " + this.queryOptions.get(QUERY_ID));
            }
        }
    }
}