package datawave.query.index.lookup;

import java.util.Iterator;

public class AnonymousIterable<T> implements Iterable<T> {
    private Iterator<T> itr;

    public AnonymousIterable(Iterator<T> itr) {
        this.itr = itr;
    }

    public Iterator<T> iterator() {
        return itr;
    }
}
