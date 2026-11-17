package datawave.test.framework.generators.value;

import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;

import datawave.data.type.Type;

/**
 * Wraps a {@link ValueGenerator} so that no two values it returns share a normalized form.
 * <p>
 * Expected results are computed from raw values, while a query is matched against normalized values. Two raw values that normalize to the same term (e.g.
 * {@code Tt} and {@code tt} under {@code LcNoDiacriticsType}) would therefore make a query for one also return the events carrying the other, disagreeing with
 * the expected result. Candidates whose normalized form was already returned are skipped.
 */
public class DistinctNormalizedValueGenerator<E> implements ValueGenerator<E> {

    /**
     * Consecutive duplicate candidates tolerated before giving up, so a delegate whose value space is exhausted fails loudly rather than looping forever.
     */
    static final int MAX_ATTEMPTS = 1000;

    private final ValueGenerator<E> delegate;
    private final Type<?> normalizer;
    private final Set<String> seen = new HashSet<>();

    /**
     * Create a generator whose values are distinct after normalization
     *
     * @param delegate
     *            the source of candidate values
     * @param normalizer
     *            the normalizer applied to the field the values are written to
     * @return the generator
     */
    public static <E> ValueGenerator<E> of(ValueGenerator<E> delegate, Type<?> normalizer) {
        return new DistinctNormalizedValueGenerator<>(delegate, normalizer);
    }

    private DistinctNormalizedValueGenerator(ValueGenerator<E> delegate, Type<?> normalizer) {
        Preconditions.checkNotNull(delegate, "delegate cannot be null");
        Preconditions.checkNotNull(normalizer, "normalizer cannot be null");
        this.delegate = delegate;
        this.normalizer = normalizer;
    }

    @Override
    public E next() {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            E candidate = delegate.next();
            if (seen.add(normalizer.normalize(String.valueOf(candidate)))) {
                return candidate;
            }
        }
        throw new IllegalStateException("could not generate a value distinct after normalization in " + MAX_ATTEMPTS + " attempts; " + seen.size()
                        + " distinct values already generated");
    }
}
