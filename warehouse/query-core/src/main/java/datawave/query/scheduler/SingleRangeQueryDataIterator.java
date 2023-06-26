package datawave.query.scheduler;

import datawave.core.query.configuration.QueryData;
import org.apache.accumulo.core.data.Range;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

public class SingleRangeQueryDataIterator implements Iterator<QueryData> {
    private final Iterator<QueryData> delegate;
    private Queue<QueryData> pending = new LinkedList<QueryData>();

    public SingleRangeQueryDataIterator(Iterator<QueryData> queries) {
        this.delegate = queries;
    }

    /**
     * Returns {@code true} if the iteration has more elements. (In other words, returns {@code true} if {@link #next} would return an element rather than
     * throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
        if (pending.isEmpty()) {
            return delegate.hasNext();
        } else {
            return true;
        }
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException
     *             if the iteration has no more elements
     */
    @Override
    public QueryData next() {
        if (pending.isEmpty()) {
            QueryData next = delegate.next();
            if (next.getRanges().size() == 1) {
                pending.add(next);
            } else {
                for (Range range : next.getRanges()) {
                    QueryData qd = new QueryData(next);
                    qd.setRanges(Collections.singleton(range));
                    pending.add(qd);
                }
            }
        }
        return pending.remove();
    }
}
