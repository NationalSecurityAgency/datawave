package datawave.query.index.lookup;

import java.util.Comparator;

import datawave.query.util.Tuple2;

public class TupleComparator<A,B> implements Comparator<Tuple2<A,B>> {

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public int compare(Tuple2<A,B> o1, Tuple2<A,B> o2) {
        if (o1.first() instanceof Comparable) {
            return ((Comparable) o1.first()).compareTo(o2.first());
        } else {
            throw new IllegalArgumentException("First element in tuple must be comparable!");
        }
    }

}
