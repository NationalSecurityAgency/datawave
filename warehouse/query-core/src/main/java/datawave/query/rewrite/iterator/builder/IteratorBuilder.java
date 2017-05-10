package datawave.query.rewrite.iterator.builder;

import datawave.query.rewrite.iterator.NestedIterator;

public interface IteratorBuilder {
    public <T> NestedIterator<T> build();
}
