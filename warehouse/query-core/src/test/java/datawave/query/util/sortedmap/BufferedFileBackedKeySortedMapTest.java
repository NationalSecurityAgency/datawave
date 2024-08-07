package datawave.query.util.sortedmap;

import datawave.query.util.sortedset.FileKeySortedSet;
import org.apache.accumulo.core.data.Key;

import java.util.Comparator;

public class BufferedFileBackedKeySortedMapTest extends BufferedFileBackedSortedMapTest<Key> {

    @Override
    public Key createData(byte[] values) {
        return new Key(values);
    }

    @Override
    public Comparator<Key> getComparator() {
        return null;
    }

    @Override
    public FileSortedMap.FileSortedMapFactory<Key> getFactory() {
        return new FileKeySortedSet.Factory();
    }

}
