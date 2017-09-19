package datawave.query.iterator.builder;

import datawave.query.iterator.NestedIterator;

public interface IteratorBuilder {
    public <T> NestedIterator<T> build();
}
