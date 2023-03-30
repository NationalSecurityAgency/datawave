package datawave.query.ranges;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;

/**
 * This class provides a standard way of building ranges with clear intent.
 */
public class RangeFactory {
    
    public static final String NULL_BYTE_STRING = "\u0000";
    public static final String MAX_UNICODE_STRING = new String(Character.toChars(Character.MAX_CODE_POINT));
    private static final String FIRST_SHARD = "_0";
    
    /**
     * Builds a document range that can be passed to the {@link datawave.query.iterator.QueryIterator}
     *
     * Example: Given shard 20190314_4 and document docId0, will return tld doc range [20190314_4 docId0, 20190314_4 docId0x00)
     *
     * @param shard
     *            a shard
     * @param docId
     *            a document id
     * @return a document range
     */
    public static Range createDocumentSpecificRange(String shard, String docId) {
        Key start = new Key(shard, docId);
        Key end = start.followingKey(PartialKey.ROW_COLFAM);
        
        // Technically, we don't want to be inclusive of the start key,
        // however if we mark the startKey as non-inclusive, when we create
        // the fi\x00 range in IndexIterator, we lost the context of "do we
        // want a single event" or "did we get restarted and this is the last
        // event we returned.
        return new Range(start, true, end, false);
    }
    
    /**
     * Builds a tld document range that can be passed to the {@link datawave.query.iterator.QueryIterator}
     *
     * Example: Given shard 20190314_4 and document docId0, will return tld doc range [20190314_4 docId0, 20190314_4 docId0xff)
     *
     * @param shard
     *            a shard
     * @param docId
     *            a document id
     * @return a tld document range
     */
    public static Range createTldDocumentSpecificRange(String shard, String docId) {
        Key start = new Key(shard, docId);
        Key end = new Key(shard, docId + MAX_UNICODE_STRING);
        
        // Technically, we don't want to be inclusive of the start key,
        // however if we mark the startKey as non-inclusive, when we create
        // the fi\x00 range in IndexIterator, we lost the context of "do we
        // want a single event" or "did we get restarted and this is the last
        // event we returned.
        return new Range(start, true, end, false);
    }
    
    /**
     * Builds a shard range that can be passed to the {@link datawave.query.iterator.QueryIterator}
     * <p>
     * Example: Given shard 20190314_4, will return shard range [20190314_4, 20190314_4\x00)
     *
     * @param shard
     *            - represents a shard (yyyyMMdd_n)
     * @return - a shard range.
     */
    public static Range createShardRange(String shard) {
        Key start = new Key(shard);
        Key end = new Key(shard + NULL_BYTE_STRING);
        return new Range(start, true, end, false);
    }
    
    /**
     * Builds a day range that can be passed to the {@link datawave.query.iterator.QueryIterator}
     * <p>
     * Example: Given day 20190314, will return day range [20190314_0, 20190314\xff)
     *
     * @param shard
     *            - represents a day (yyyyMMdd)
     * @return - a day range
     */
    public static Range createDayRange(String shard) {
        Key start = new Key(shard + FIRST_SHARD);
        Key end = new Key(shard + MAX_UNICODE_STRING);
        return new Range(start, true, end, false);
    }
}
