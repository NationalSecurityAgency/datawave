package datawave.visibility;

import java.util.Arrays;
import java.util.Collection;

import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Functions for combining and flattening Accumulo column visibilities.
 * <p>
 * Deploy exactly one provider JAR whose {@code META-INF/services/datawave.visibility.VisibilityFunctions} names its implementation. Stock applications use
 * {@code visibility-provider-standard}; replace it (do not supplement it) to use a different policy. The JAR must be visible to the class loader that loaded
 * this interface, including on tablet servers. There is no fallback or class-name override.
 * <p>
 * Implementations must be thread-safe and have a public no-argument constructor. Instances are not managed by any container, so implementations are responsible
 * for their own configuration and caching.
 */
public interface VisibilityFunctions {

    /**
     * Combine visibilities into a single visibility that is at least as restrictive as every input. Null and empty inputs are ignored.
     *
     * @param visibilities
     *            the visibilities to combine; may contain nulls
     * @return the combined visibility; never null. An empty {@link ColumnVisibility} is returned when there are no non-empty inputs.
     */
    ColumnVisibility combine(Collection<ColumnVisibility> visibilities);

    /**
     * Combine two visibilities. Unlike {@link #combine(Collection)}, this method preserves the absence of a visibility: if both inputs are null then null is
     * returned, and if only one input is null the other is returned unchanged. This matches how per-field visibilities are combined during ingest, where null
     * means "use the event visibility".
     *
     * @param first
     *            the first visibility, may be null
     * @param second
     *            the second visibility, may be null
     * @return the combined visibility, or null if both inputs are null
     */
    default ColumnVisibility combine(ColumnVisibility first, ColumnVisibility second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        return combine(Arrays.asList(first, second));
    }

    /**
     * Produce the flattened (normalized) byte form of a visibility, as written to Accumulo keys.
     *
     * @param visibility
     *            the visibility to flatten
     * @return the flattened visibility bytes
     */
    byte[] flatten(ColumnVisibility visibility);

    /**
     * @return the process-wide {@link VisibilityFunctions} instance, resolved once on first use
     * @throws IllegalStateException
     *             if no provider, more than one provider, or a broken provider is registered
     */
    static VisibilityFunctions getInstance() {
        return VisibilityProviderLoader.getInstance();
    }
}
