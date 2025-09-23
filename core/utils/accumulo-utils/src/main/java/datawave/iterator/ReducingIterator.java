package datawave.iterator;

import org.apache.accumulo.core.iterators.Combiner;

public interface ReducingIterator {
    Class<? extends Combiner> getReducerClass();
}
