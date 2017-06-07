package nsa.datawave.query.rewrite.iterator.pipeline;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.YieldCallback;

import java.util.Iterator;
import java.util.Map.Entry;

/**
 * Created on 6/2/17.
 */
public interface YieldingIterator extends Iterator<Entry<Key,Value>> {
    void enableYielding(YieldCallback<Key> yieldCallback);
}
