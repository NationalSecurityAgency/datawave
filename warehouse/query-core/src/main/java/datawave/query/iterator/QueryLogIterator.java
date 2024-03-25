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

    protected string queryID;
    protected NestedIterator<Key> initKeySource, seekKeySource;
    protected IteratorEnvironment myEnvironment;

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
            this.queryID = options.get(QUERY_ID);
            this.myEnvironment = env;
            this.source = source;
            StartMethod("init()");
        }
        finally{
            EndMethod("init()");
        }
    }

    private void StartMethod(string methodName){
        if (log.isInfoEnabled()) {
            log.info("QueryLogIterator: " + methodName + " Started QueryID: " + this.queryID);
        }
    }

    private void EndMethod(string methodName){
        if (log.isInfoEnabled()) {
            log.info("QueryLogIterator: " + methodName + " Ended QueryID: " + this.queryID);
        }
    }

    @Override
    public boolean hasTop() {

        boolean result;

        try {
            StartMethod("hasTop()");
            result = source.hasTop();
        }
        finally {
            EndMethod("hasTop()");
        }
        return result;
    }

    @Override
    public void next() throws IOException {
        try {
            StartMethod("next()");
            source.next();
        }
        finally {
            EndMethod("next()");
        }
    }

    @Override
    public Key getTopKey() {
        Key k;
        try {
            StartMethod("getTopKey()");
            k = source.getTopKey();
        }
        finally {
            EndMethod("getTopKey()");
        }
        return k;
    }

    @Override
    public Value getTopValue() {
        Value v;
        try {
            StartMethod("getTopValue()");
            v = source.getTopValue();
        }
        finally {
            EndMethod("getTopValue()");
        }
        return v;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment iteratorEnvironment) {

        QueryLogIterator newQLI = null;

        try {
            StartMethod("deepCopy()");
            newQLI = new QueryLogIterator(this, this.myEnvironment);
        }
        finally {
            EndMethod("deepCopy()");
        }
        return newQLI;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> collection, boolean b) throws IOException {

        try {
            StartMethod("seek()");
            this.source.seek(range, collection, b);
        }
        finally {
            EndMethod("seek()");
        }
    }
}