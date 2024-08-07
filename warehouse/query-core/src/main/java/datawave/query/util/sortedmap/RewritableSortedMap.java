package datawave.query.util.sortedmap;

import java.util.SortedMap;

public interface RewritableSortedMap<K,V> extends SortedMap<K,V> {

    FileSortedMap.RewriteStrategy<K,V> getRewriteStrategy();

    void setRewriteStrategy(FileSortedMap.RewriteStrategy<K,V> rewriteStrategy);

}
