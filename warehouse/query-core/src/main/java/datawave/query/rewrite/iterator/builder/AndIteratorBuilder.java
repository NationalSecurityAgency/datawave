package datawave.query.rewrite.iterator.builder;

import datawave.query.rewrite.iterator.NestedIterator;
import datawave.query.rewrite.iterator.logic.AndIterator;

public class AndIteratorBuilder extends AbstractIteratorBuilder {
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> NestedIterator<T> build() {
        if (includes.isEmpty()) {
            throw new IllegalStateException("AndIterator has no inclusive sources!");
        }
        return new AndIterator(includes, excludes);
    }
}
