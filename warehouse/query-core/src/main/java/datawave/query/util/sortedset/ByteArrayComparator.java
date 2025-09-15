package datawave.query.util.sortedset;

import org.apache.hadoop.io.WritableComparator;

import java.util.Comparator;

public class ByteArrayComparator implements Comparator<byte[]> {

    @Override
    public int compare(byte[] data, byte[] term) {
        return WritableComparator.compareBytes(data, 0, data.length, term, 0, term.length);
    }
}
