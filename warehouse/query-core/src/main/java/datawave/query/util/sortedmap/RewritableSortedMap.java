package datawave.query.util.sortedmap;

import java.util.SortedMap;

/**
 * A rewritable sorted map which will replace the value for a key dependent on a RewriteStrategy
 *
 * @param <K>
 *            key of the map
 * @param <V>
 *            value of the map
 */
public interface RewritableSortedMap<K,V> extends SortedMap<K,V> {

    FileSortedMap.RewriteStrategy<K,V> getRewriteStrategy();

    void setRewriteStrategy(FileSortedMap.RewriteStrategy<K,V> rewriteStrategy);

}
