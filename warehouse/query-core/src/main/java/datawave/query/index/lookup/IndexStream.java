package datawave.query.index.lookup;

import org.apache.commons.jexl3.parser.JexlNode;

import com.google.common.collect.PeekingIterator;

import datawave.query.util.Tuple2;

/**
 * IndexStreams must support the PeekingIterator interface.
 * <p>
 * All inheriting classes must support the ability to seek to a specific shard.
 */
public interface IndexStream extends PeekingIterator<Tuple2<String,IndexInfo>> {
    enum StreamContext {
        /**
         * INITIALIZED indicates that a given stream has yet to be evaluated against the global index
         */
        INITIALIZED,
        /**
         * PRESENT indicates that a given field and term has data in the global index.
         */
        PRESENT,
        /**
         * ABSENT means that we expected data to exist, but did not find any. This could be an indexed field that simply has no value in the global index, or it
         * could be that the value exists but not for the specified field, or neither the field nor the value is present in the global index.
         */
        ABSENT,
        /**
         * VARIABLE indicates that this index stream has a mix of delayed and non-delayed terms
         */
        VARIABLE,
        /**
         * NO_OP marks a node as a placeholder when it is consumed by a parent of the same node type
         */
        NO_OP,
        /**
         * DELAYED means this term or junction of terms is delayed for any number of reasons
         */
        DELAYED,
    }

    StreamContext context();

    /**
     * This method is used to get an explanation of how we arrived at the provided context().
     *
     * @return the context string
     */
    String getContextDebug();

    JexlNode currentNode();

    /**
     * Advance the underlying iterator to the first element that is greater than or equal to the <code>seekShard</code>.
     * <p>
     * If no data exists beyond the <code>seekShard</code> then a null value is returned, signifying the end of this index stream.
     *
     * @param seekShard
     *            the seek target
     * @return the top shard after seeking, or null if no more values exist
     */
    String seek(String seekShard);

    /**
     * Additional context is required for nested unions that have a VARIABLE stream context.
     * <p>
     * The active terms in a union may hit on a later shard than the provided shard; in this case any delayed terms need to be returned in order to maintain as
     * much of the original query as possible.
     *
     * @param context
     *            a shard
     * @return the next result
     */
    Tuple2<String,IndexInfo> next(String context);
}
