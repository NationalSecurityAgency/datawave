package datawave.query.util.sortedmap;

import datawave.query.util.sortedset.ByteArrayComparator;
import datawave.query.util.sortedset.FileSerializableSortedSet;
import datawave.query.util.sortedset.FileSortedSet;

import java.util.Comparator;

public class BufferedFileBackedByteArraySortedSetTest extends BufferedFileBackedSortedSetTest<byte[]> {

    @Override
    public byte[] createData(byte[] values) {
        return values;
    }

    @Override
    public Comparator<byte[]> getComparator() {
        return new ByteArrayComparator();
    }

    @Override
    public FileSortedMap.FileSortedMapFactory<byte[]> getFactory() {
        return new FileSerializableSortedSet.Factory();
    }
}
