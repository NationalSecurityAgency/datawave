package nsa.datawave.query.rewrite.iterator.builder;

import nsa.datawave.query.rewrite.iterator.NestedIterator;

public interface IteratorBuilder {
    public <T> NestedIterator<T> build();
}
