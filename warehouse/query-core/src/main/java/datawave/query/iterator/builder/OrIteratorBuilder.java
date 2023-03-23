package datawave.query.iterator.builder;

import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.logic.OrIterator;

public class OrIteratorBuilder extends AbstractIteratorBuilder {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public <T> NestedIterator<T> build() {
        return new OrIterator(includes, excludes, waitWindowObserver);
    }

}
