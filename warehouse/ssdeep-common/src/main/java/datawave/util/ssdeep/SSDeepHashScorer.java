package datawave.util.ssdeep;

/**
 * An interface for things that take two hashes and compare them to produce some sort of result.
 *
 * @param <T>
 *            class type for hashes
 */
public interface SSDeepHashScorer<T> {
    public T apply(SSDeepHash signature1, SSDeepHash signature2);
}
