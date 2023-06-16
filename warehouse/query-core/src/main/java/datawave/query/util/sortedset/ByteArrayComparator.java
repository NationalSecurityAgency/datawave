package datawave.query.util.sortedset;

import java.util.Comparator;

public class ByteArrayComparator implements Comparator<byte[]> {

    @Override
    public int compare(byte[] data, byte[] term) {
        int minSize = data.length;
        if (term.length < minSize)
            minSize = term.length;
        int comparison = 0;
        for (int i = 0; i < minSize; i++) {
            comparison = Byte.valueOf(data[i]).compareTo(term[i]);
            if (comparison != 0)
                break;
        }
        if (comparison == 0) {
            if (minSize < data.length) {
                comparison = 1;
            } else if (minSize < term.length) {
                comparison = -1;
            }
        }
        return comparison;
    }
}
