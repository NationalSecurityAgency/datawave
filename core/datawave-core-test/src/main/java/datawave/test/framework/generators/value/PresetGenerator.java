package datawave.test.framework.generators.value;

import java.util.List;

import datawave.test.framework.util.InfiniteIterator;

/**
 *
 */
public class PresetGenerator<E> implements ValueGenerator<E> {

    private final InfiniteIterator<E> values;

    public static <E> ValueGenerator<E> of(List<E> values) {
        return new PresetGenerator<>(values);
    }

    private PresetGenerator(List<E> values) {
        this.values = new InfiniteIterator<>(values);
    }

    @Override
    public E next() {
        return values.next();
    }
}
