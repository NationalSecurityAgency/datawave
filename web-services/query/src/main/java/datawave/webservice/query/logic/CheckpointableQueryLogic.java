package datawave.webservice.query.logic;

import datawave.microservice.common.storage.QueryCheckpoint;
import datawave.microservice.common.storage.QueryKey;
import org.apache.accumulo.core.client.Connector;

public interface CheckpointableQueryLogic {
    
    /**
     * This can be called at any point to get a checkpoint such that this query logic instance can be torn down to be rebuilt later. At a minimum this should be
     * called after the getTransformIterator is depleted of results.
     *
     * @param queryKey
     *            - the query key to include in the checkpoint
     * @return The query checkpoint
     */
    QueryCheckpoint checkpoint(QueryKey queryKey);
    
    /**
     * Implementations use the configuration to setup execution of a portion of their query. getTransformIterator should be used to get the partial results if
     * any.
     *
     * @param connection
     *            - The accumulo connector
     * @param checkpoint
     *            - Encapsulates all information needed to run a portion of the query.
     */
    void setupQuery(Connector connection, QueryCheckpoint checkpoint) throws Exception;
    
}
