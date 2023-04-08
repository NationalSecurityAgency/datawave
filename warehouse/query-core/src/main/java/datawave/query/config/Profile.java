package datawave.query.config;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.query.planner.QueryPlanner;
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
     *            the query logic
     */
    void configure(BaseQueryLogic<Entry<Key,Value>> logic);
    
    /**
     * Tune the query planner.
     * 
     * @param planner
     *            the query planner.
     */
    void configure(QueryPlanner planner);
    
    /**
     * Tune the query configuration object
     * 
     * @param configuration
     *            the query configuration object
     */
    void configure(GenericQueryConfiguration configuration);
}
