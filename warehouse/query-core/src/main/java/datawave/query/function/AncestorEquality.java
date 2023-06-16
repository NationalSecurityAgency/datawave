package datawave.query.function;

import org.apache.accumulo.core.data.Key;

import static datawave.data.hash.UIDConstants.DEFAULT_SEPARATOR;

/**
 * A key equality implementation that compares key prefixes to determine ancestor equality.
 *
 * For example, given a parent key "h1.h2.h3.a.b.c" a valid ancestor would be key "h1.h2.h3.a.b.c.d.e"
 */
public class AncestorEquality implements Equality {

    /**
     * Determines if the first key is an ancestor of the other key by checking for a common prefix.
     *
     * @param key
     *            - current key
     * @param other
     *            - candidate for ancestor check
     * @return - true if the provided key is an ancestor of the other key
     */
    @Override
    public boolean partOf(Key key, Key other) {
        String docId = key.getColumnFamily().toString();
        String otherId = other.getColumnFamily().toString();
        return docId.startsWith(otherId + DEFAULT_SEPARATOR) || docId.equals(otherId);
    }
}
