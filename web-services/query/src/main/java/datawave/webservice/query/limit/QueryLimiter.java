package datawave.webservice.query.limit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

/**
 * This class is responsible for determining if any concurrent query limits are going to be exceeded for a user, system, or query logic when a new query is
 * submitted.
 */
@Component("queryLimiter")
public class QueryLimiter {
    
    private String zookeeperConfig;
    private QueryLimitProvider queryLimitProvider;
    private SnapshotProvider snapshotProvider;
    
    public String getZookeeperConfig() {
        return zookeeperConfig;
    }
    
    public void setZookeeperConfig(String zookeeperConfig) {
        this.zookeeperConfig = zookeeperConfig;
    }
    
    public QueryLimitProvider getQueryLimitProvider() {
        return queryLimitProvider;
    }
    
    @Autowired
    public void setQueryLimitProvider(QueryLimitProvider queryLimitProvider) {
        this.queryLimitProvider = queryLimitProvider;
    }
    
    public SnapshotProvider getSnapshotProvider() {
        // If the snapshot provider is null, create an implementation that will return a snapshot from the ActiveQueryTracker.
        if(snapshotProvider == null) {
            snapshotProvider = (userDn, system, queryLogic) -> {
                try (ActiveQueryTracker tracker = new ActiveQueryTracker(zookeeperConfig, 120000)) {
                    return tracker.getSnapshot(userDn, system, queryLogic);
                }
            };
        }
        return snapshotProvider;
    }
    
    /**
     * Return whether submitting a new query for the given user, on the given system, based on the given query logic, would exceed a configured concurrent query
     * limit.
     * @param userDn the user DN
     * @param system the system name
     * @param queryLogic the query logic
     * @return the response
     */
    public QueryLimiterResponse checkForLimits(String userDn, String system, String queryLogic) throws Exception {
        ActiveQuerySnapshot snapshot = getSnapshotProvider().getSnapshot(userDn, system, queryLogic);
        
        int totalQueriesForUser = getTotalQueriesThatCountAgainstLimit(snapshot);
        boolean userOverrodeQueryLogicLimit = false;
        
        // Check if the user has reached their max concurrent query limit across all systems.
        UserQueryLimit userLimit = queryLimitProvider.getUserLimit(userDn);
        if (totalQueriesForUser >= userLimit.getQueryLimit()) {
            String message = "User '" + userDn + "' has reached max " + (userLimit.getSource() == LimitSource.DEFAULTS ? "default " : "") + "user query limit of " + userLimit.getQueryLimit();
            return QueryLimiterResponse.exceedsLimit(message);
        }
        
        // Check if the user has reached their max concurrent query limit for any overridden query logic groups.
        if (userLimit.overridesAnyQueryLogicLimits()) {
            Optional<QueryLogicGroupLimit> optionalGroupLimit = queryLimitProvider.getOverriddenQueryLogicGroupLimit(userDn, queryLogic,
                            userLimit.getQueryLogicGroupLimits());
            if (optionalGroupLimit.isPresent()) {
                userOverrodeQueryLogicLimit = true;
                QueryLogicGroupLimit groupLimit = optionalGroupLimit.get();
                if (snapshot.getTotalUserQueriesForQueryLogic() >= groupLimit.getQueryLimit()) {
                    String message = "User '" + userDn + "' has reached max user query limit of " + groupLimit.getQueryLimit() + " for query logic " + queryLogic;
                    return QueryLimiterResponse.exceedsLimit(message);
                }
            }
        }
        
        // Check if the system has reached its max concurrent query limit.
        SystemQueryLimit systemLimit = queryLimitProvider.getSystemLimit(system);
        if (snapshot.getTotalSystemQueries() >= systemLimit.getQueryLimit()) {
            String message = "System '" + system + "' has reached max " + (systemLimit.getSource() ==  LimitSource.DEFAULTS ? "default " : "") + "system query limit of " + systemLimit.getQueryLimit();
            return QueryLimiterResponse.exceedsLimit(message);
        }
        
        // Check if the system has reached its max concurrent query limit for any overridden query logic groups.
        if (systemLimit.overridesAnyQueryLogicLimits()) {
            Optional<QueryLogicGroupLimit> optionalGroupLimit = queryLimitProvider.getOverriddenQueryLogicGroupLimit(system, queryLogic,
                            systemLimit.getQueryLogicGroupLimits());
            if (optionalGroupLimit.isPresent()) {
                QueryLogicGroupLimit groupLimit = optionalGroupLimit.get();
                if (snapshot.getTotalSystemQueriesForQueryLogic() >= groupLimit.getQueryLimit()) {
                    String message = "System '" + system + "' has reached max query limit of " + groupLimit.getQueryLimit() + " for query logic " + queryLogic;
                    return QueryLimiterResponse.exceedsLimit(message);
                }
            }
        }
        
        // If the user did not have any overridden query logic groups that matched the query logic, check if the user has reached the max concurrent query limit
        // for a query logic group's default limit.
        if(!userOverrodeQueryLogicLimit) {
            Optional<QueryLogicGroupLimit> optionalGroupLimit = queryLimitProvider.getQueryLogicGroupLimit(queryLogic);
            if (optionalGroupLimit.isPresent()) {
                QueryLogicGroupLimit groupLimit = optionalGroupLimit.get();
                if(snapshot.getTotalUserQueriesForQueryLogic() >= groupLimit.getQueryLimit()) {
                    String message = "User '" + userDn + "' has reached max default query limit of " + groupLimit.getQueryLimit() + " for query logic " + queryLogic;
                    return QueryLimiterResponse.exceedsLimit(message);
                }
            }
        }
        
        // The query can be submitted.
        return QueryLimiterResponse.doesNotExceedLimit();
    }
    
    /**
     * Return the total queries the user has running that count towards their query limit.
     * @param snapshot the snapshot to extract query metrics from
     * @return the total queries
     */
    private int getTotalQueriesThatCountAgainstLimit(ActiveQuerySnapshot snapshot) {
        // The total number of queries a user has running, and the total number of those queries that count towards the user's query limit may differ. It
        // depends on the systems the queries were submitted on, and whether queries on the systems count towards the user's query limit. Evaluate each system
        // that the user has queries on, and only sum up the queries on systems that count towards the user's query limit.
        int totalQueries = 0;
        Map<String,Integer> totalUserQueriesPerSystem = snapshot.getTotalUserQueriesPerSystem();
        for(Map.Entry<String,Integer> entry : totalUserQueriesPerSystem.entrySet()) {
            String system = entry.getKey();
            if (queryLimitProvider.systemCountsAgainstUserLimit(system)) {
                totalQueries += entry.getValue();
            }
        }
        return totalQueries;
    }
    
    /**
     * A simple interface for providing an {@link ActiveQueryTracker}. Used to allow ease of mock injection for testing purposes.
     */
    public interface SnapshotProvider {
        ActiveQuerySnapshot getSnapshot(String userDn, String system, String queryLogic) throws Exception;
    }
}

