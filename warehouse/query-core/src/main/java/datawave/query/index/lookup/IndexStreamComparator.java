package datawave.query.index.lookup;

import java.util.Comparator;

/**
 * Intended for use in the {@link Intersection} to order IndexStreams
 * <p>
 * IndexStreams are ordered first by stream context, then by implementing subclass
 */
public class IndexStreamComparator implements Comparator<IndexStream> {
    
    @Override
    public int compare(IndexStream left, IndexStream right) {
        int leftId = id(left);
        int rightId = id(right);
        
        int result = Integer.compare(leftId, rightId);
        if (result == 0) {
            // a TreeMultimap is an implementation of a SortedKeySortedSetMultimap
            // element uniqueness is NOT determined by the stream class and context
            // return a 1 to insert this "equivalent" element after an existing element
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
        return getContextId(stream) + getClassId(stream);
    }
    
    /**
     * Secondary sort by class
     *
     * @param stream
     *            an IndexStream
     * @return an integer value between 1 and 4, inclusive
     */
    private int getClassId(IndexStream stream) {
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
    
    /**
     * Primary sort by IndexStream context
     *
     * @param stream
     *            an IndexStream
     * @return an integer value between 10 and 110, inclusive
     */
    private int getContextId(IndexStream stream) {
        switch (stream.context()) {
            case ABSENT: // ABSENT returns first so we can short-circuit intersections
                return 10;
            case PRESENT: // PRESENT provides the best chance for a shard to anchor on
                return 20;
            case VARIABLE: // mix of PRESENT and some form of DELAYED
                return 30;
            case EXCEEDED_VALUE_THRESHOLD: // from here on down, all forms of DELAYED
                return 40;
            case EXCEEDED_TERM_THRESHOLD:
                return 50;
            case IGNORED:
                return 60;
            case UNINDEXED:
                return 70;
            case DELAYED_FIELD:
                return 80;
            case UNKNOWN_FIELD:
                return 90;
            case NO_OP:
                return 100;
            case INITIALIZED:
                return 110;
            default:
                throw new IllegalStateException("cannot get context id for context: " + stream.context());
        }
    }
}
