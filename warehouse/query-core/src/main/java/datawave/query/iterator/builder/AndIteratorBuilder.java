package datawave.query.iterator.builder;

import datawave.query.iterator.logic.AndIterator;
import datawave.query.iterator.NestedIterator;

public class AndIteratorBuilder extends AbstractIteratorBuilder {
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> NestedIterator<T> build() {
        if (includes.isEmpty()) {
            throw new IllegalStateException("AndIterator has no inclusive sources!");
        }
        return new AndIterator(includes, excludes);
    }
}
