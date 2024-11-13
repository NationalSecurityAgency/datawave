package datawave.query.util;

import java.util.Iterator;

import com.google.common.base.Function;

/**
 *
 */
public class TraceIterators {
    public static <F,T> Iterator<T> transform(final Iterator<F> from, final Function<? super F,? extends T> func, final String description) {
        return new TraceIterator<F,T>(from, description) {

            @Override
            public T tracedTransform(F from) {
                return func.apply(from);
            }

        };
    }
}
