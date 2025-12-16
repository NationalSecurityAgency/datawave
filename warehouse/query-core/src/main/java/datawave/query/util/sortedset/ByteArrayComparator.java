package datawave.query.util.sortedset;

import java.util.Comparator;

import org.apache.hadoop.io.WritableComparator;

public class ByteArrayComparator implements Comparator<byte[]> {

    @Override
    public int compare(byte[] data, byte[] term) {
        return WritableComparator.compareBytes(data, 0, data.length, term, 0, term.length);
    }
}
