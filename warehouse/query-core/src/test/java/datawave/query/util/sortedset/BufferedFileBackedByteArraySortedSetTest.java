package datawave.query.util.sortedset;

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
    public FileSortedSet.FileSortedSetFactory<byte[]> getFactory() {
        return new FileSerializableSortedSet.Factory();
    }
}
