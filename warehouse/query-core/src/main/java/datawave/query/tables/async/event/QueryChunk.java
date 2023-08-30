package datawave.query.tables.async.event;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.collect.Lists;

/**
 *
 */
public class QueryChunk implements Iterator<Entry<Key,Value>> {

    List<Entry<Key,Value>> kvList;

    Iterator<Entry<Key,Value>> kvIter = null;

    public QueryChunk(int size) {
        kvList = Lists.newArrayListWithCapacity(size);
    }

    public boolean addResult(Entry<Key,Value> kv) {
        return kvList.add(kv);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        if (null == kvIter) {
            kvIter = kvList.iterator();
        }
        return kvIter.hasNext();
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#next()
     */
    @Override
    public Entry<Key,Value> next() {
        return kvIter.next();
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        // do nothing
    }
}
