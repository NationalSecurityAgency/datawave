package datawave.query.util;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import datawave.query.CloseableIterable;

/**
 * An empty {@link CloseableIterable}
 *
 * @param <T>
 *            the type
 */
public class EmptyCloseableIterator<T> implements CloseableIterable<T> {

    @Override
    public void close() throws IOException {
        // no-op
    }

    @Override
    public Iterator<T> iterator() {
        return Collections.emptyIterator();
    }
}
