package nsa.datawave.query.rewrite.planner;

import nsa.datawave.query.model.QueryModel;

/**

 */
public interface QueryModelProvider {
    
    QueryModel getQueryModel();
    
    abstract class Factory {
        
        public abstract QueryModelProvider createQueryModelProvider();
        
    }
}
