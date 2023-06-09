package datawave.common.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

public class MultiComparator<T> implements Comparator<T>, Serializable {
    private static final long serialVersionUID = 7403028374892043690L;
    
    private transient final Collection<Comparator<T>> comparators;
    
    public MultiComparator(Collection<Comparator<T>> comparators) {
        this.comparators = comparators;
    }
    
    public MultiComparator(Comparator<T>... comparators) {
        this((comparators != null && comparators.length > 0) ? Arrays.asList(comparators) : new ArrayList<>());
    }
    
    @Override
    public int compare(T o1, T o2) {
        for (Comparator<T> comparator : comparators) {
            int result = comparator.compare(o1, o2);
            if (result != 0)
                return result;
        }
        return 0;
    }
}
