package datawave.query.iterator.builder;

import datawave.query.iterator.NestedIterator;

/**
 * Used this to build negation subtrees.
 * 
 */
public class NegationBuilder extends AbstractIteratorBuilder {
    
    @Override
    public <T> NestedIterator<T> build() {
        throw new UnsupportedOperationException();
    }
    
}
