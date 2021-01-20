package datawave.webservice.common.storage;

/**
 * A query task is an action to perform for a specified query.
 */
public interface QueryTask {
    
    public enum QUERY_ACTION {
        CREATE, NEXT, CLOSE
    }
    
    /**
     * The action to perform
     * 
     * @return the action
     */
    QUERY_ACTION getAction();
    
    /**
     * Get the query checkpoint on which to perform the next task
     * 
     * @return A query checkpoint
     */
    QueryCheckpoint getQueryState();
}
