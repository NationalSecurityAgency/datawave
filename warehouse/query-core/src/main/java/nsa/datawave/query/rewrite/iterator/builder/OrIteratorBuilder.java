package nsa.datawave.query.rewrite.iterator.builder;

import nsa.datawave.query.rewrite.iterator.NestedIterator;
import nsa.datawave.query.rewrite.iterator.logic.OrIterator;

public class OrIteratorBuilder extends AbstractIteratorBuilder {
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public <T> NestedIterator<T> build() {
        return new OrIterator(includes, excludes, sortedUIDs);
    }
    
}
