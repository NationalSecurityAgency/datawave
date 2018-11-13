package datawave.query.iterator.builder;

import datawave.query.iterator.NestedIterator;

public interface IteratorBuilder {
    <T> NestedIterator<T> build();
}
