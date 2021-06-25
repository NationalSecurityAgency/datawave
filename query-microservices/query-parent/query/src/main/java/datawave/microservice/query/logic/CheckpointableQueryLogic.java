package datawave.microservice.query.logic;

import org.apache.accumulo.core.client.Connector;

import java.util.List;

public interface CheckpointableQueryLogic {
    
    /**
     * This will allow us to check if a query logic is actually checkpointable. Even if the query logic supports it, the caller may have to tell the query logic
     * that it is going to be checkpointed.
     * 
     * @return true if checkpointable
     */
    boolean isCheckpointable();
    
    /**
     * This will tell the query logic that is is going to be checkpointed.
     * 
     * @param checkpointable
     *            true if this query logic is to be trated as checkpointable
     */
    void setCheckpointable(boolean checkpointable);
    
    /**
     * This can be called at any point to get a checkpoint such that this query logic instance can be torn down to be rebuilt later. At a minimum this should be
     * called after the getTransformIterator is depleted of results.
     *
     * @param queryKey
     *            - the query key to include in the checkpoint
     * @return The query checkpoints
     */
    List<QueryCheckpoint> checkpoint(QueryKey queryKey);
    
    /**
     * Implementations use the configuration to setup execution of a portion of their query. getTransformIterator should be used to get the partial results if
     * any.
     *
     * @param connection
     *            - The accumulo connector
     * @param checkpoint
     *            - Encapsulates all information needed to run a portion of the query.
     * @throws Exception
     *             on failure
     */
    void setupQuery(Connector connection, QueryCheckpoint checkpoint) throws Exception;
    
}
