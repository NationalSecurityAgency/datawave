package datawave.query.index.lookup;

import org.apache.commons.jexl2.parser.JexlNode;

import datawave.query.util.Tuple2;

import com.google.common.collect.PeekingIterator;

/**
 * IndexStreams must support the PeekingIterator interface.
 *
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
         * ABSENT means that we expected data to exist, but did not find any.
         */
        ABSENT,
        
        /**
         * VARIABLE indicates that this particular stream will have some delayed and non delayed predicates
         */
        VARIABLE,
        
        /**
         * No op expression used to indicate that an expression is kept as a place holder.
         */
        NO_OP,
        
        /**
         * Delayed expression
         */
        DELAYED_FIELD,
        /**
         * UNINDEXED means that the given field is not present for any value in the index.
         */
        UNINDEXED,
        
        /**
         * UKNOWN_FIELD means that the field has never been tracked by the system.
         */
        UNKNOWN_FIELD,
        
        /**
         * EXCEEDED_THRESHOLD means that a we exceeded a term threshold somewhere
         */
        EXCEEDED_TERM_THRESHOLD,
        
        /**
         * EXCEEDED_THRESHOLD means that a we exceeded a value threshold somewhere
         */
        EXCEEDED_VALUE_THRESHOLD,
        
        /**
         * At some point in the processing chain, we determined that a node (range or regex) did not need to be expanded to satisfy the query using the field
         * index
         */
        IGNORED
    }
    
    StreamContext context();
    
    /**
     * This method is used to get an explanation of how we arrived at the provided context().
     */
    String getContextDebug();
    
    JexlNode currentNode();
    
    /**
     * Advance the underlying iterator to the first element that is greater than or equal to the <code>seekShard</code>.
     *
     * If no data exists beyond the <code>seekShard</code> then a null value is returned, signifying the end of this index stream.
     *
     * @param seekShard
     *            the seek target
     * @return the top shard after seeking, or null if no more values exist
     */
    String seek(String seekShard);
}
