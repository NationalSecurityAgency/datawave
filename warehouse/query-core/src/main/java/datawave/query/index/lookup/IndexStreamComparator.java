package datawave.query.index.lookup;

import java.util.Comparator;

/**
 * Intended for use in the {@link Intersection} to order IndexStreams
 */
public class IndexStreamComparator implements Comparator<IndexStream> {
    
    @Override
    public int compare(IndexStream left, IndexStream right) {
        int leftId = id(left);
        int rightId = id(right);
        
        int result = Integer.compare(leftId, rightId);
        if (result == 0) {
            return 1;
        } else {
            return result;
        }
    }
    
    /**
     * Map an IndexStream's class to an integer id
     *
     * @param stream
     *            an IndexStream
     * @return an integer id for the provided index stream
     */
    private int id(IndexStream stream) {
        if (stream instanceof ScannerStream) {
            return 1;
        } else if (stream instanceof Intersection) {
            return 2;
        } else if (stream instanceof Union) {
            return 3;
        } else {
            return 4;
        }
    }
}
