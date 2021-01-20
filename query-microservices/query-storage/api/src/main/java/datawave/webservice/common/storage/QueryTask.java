package datawave.webservice.common.storage;

/**
 * A query task is an action to perform for a specified query.
 */
public class QueryTask {
    
    public enum QUERY_ACTION {
        CREATE, NEXT, CLOSE
    }

    private QUERY_ACTION action;
    private QueryCheckpoint queryCheckpoint;

    public QueryTask(QUERY_ACTION action, QueryCheckpoint queryCheckpoint) {
        this.action = action;
        this.queryCheckpoint = queryCheckpoint;
    }


    /**
     * The action to perform
     * 
     * @return the action
     */
    public QUERY_ACTION getAction() {
        return action;
    }
    
    /**
     * Get the query checkpoint on which to perform the next task
     * 
     * @return A query checkpoint
     */
    public QueryCheckpoint getQueryState() {
        return queryCheckpoint;
    }
}
