package datawave.query.function;

import java.util.List;

import org.apache.accumulo.core.data.Key;

/**
 * A tally of an event's descendants as output by the {@link DescendantCountFunction}
 */
public interface DescendantCount {

    /**
     * Returns the total number, if known, of event's descendants
     *
     * @return the total count of an event's descendants, or a negative value if such a count is not known
     */
    int getAllGenerationsCount();

    /**
     * Returns the number, if known, of an event's first generation of descendants. In other words, the returned value is the count of an event's immediate
     * children.
     *
     * @return the count of an event's first-generation descendants, or a negative value if such a count is not known
     */
    int getFirstGenerationCount();

    /**
     * Returns a non-null list of descendant-count Keys, which may or may not be generated based on available query options, input Keys, and document attributes
     *
     * @return a non-null list of descendant-count Keys
     *
     * @see DescendantCountFunction
     */
    List<Key> getKeys();

    /**
     * Returns true if an event has descendants, false if not
     *
     * @return true if an event has descendants, false if not
     */
    boolean hasDescendants();
}
