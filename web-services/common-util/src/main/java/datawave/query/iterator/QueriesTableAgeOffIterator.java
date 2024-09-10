package datawave.query.iterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class QueriesTableAgeOffIterator extends Filter {

    private long currentTime = 0;

    @Override
    public IteratorOptions describeOptions() {
        // There are no options
        Map<String,String> m = Collections.emptyMap();
        List<String> l = Collections.emptyList();
        return new IteratorOptions("QueriesTableAgeOffIterator", "rejects keys whose timestamp is less than now", m, l);
    }

    @Override
    public boolean accept(Key k, Value v) {
        // The timestamp in the key of the queries table is the expiration date. If it is less than the
        // current time, then remove the key.
        if (currentTime > k.getTimestamp())
            return false;
        else
            return true;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        this.currentTime = System.currentTimeMillis();
    }
}
