package datawave.query.config;

import datawave.query.planner.QueryPlanner;
import datawave.services.query.configuration.GenericQueryConfiguration;
import datawave.services.query.logic.BaseQueryLogic;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.Map.Entry;

/**
 * Purpose: Provides a mechanism to provide a user configurable way to tune a user's query, such that he or she may take advantage of features that could have
 * negative implications on other user queries.
 * 
 * Design: Simple interface. User would select the profile through a bean
 * 
 *
 */
public interface Profile {
    /**
     * Tune the query logic
     * 
     * @param logic
     */
    void configure(BaseQueryLogic<Entry<Key,Value>> logic);
    
    /**
     * Tune the query planner.
     * 
     * @param planner
     */
    void configure(QueryPlanner planner);
    
    /**
     * Tune the query configuration object
     * 
     * @param configuration
     */
    void configure(GenericQueryConfiguration configuration);
}
