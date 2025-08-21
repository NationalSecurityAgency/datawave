package datawave.webservice.query.limit;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Represents a snapshots of active queries that can be used to determine if a user will exceed any query limits when attempting to submit a new query.
 */
public class ActiveQuerySnapshot {
    
    // The user submitting the query.
    private final String userDn;
    
    // The system name the query was submitted on.
    private final String systemName;
    
    // The query logic the query is based on.
    private final String queryLogic;
    
    // The total number of active queries for the user across all systems based on the query logic.
    private final int totalUserQueriesForQueryLogic;
    
    // The total queries the user has running per system.
    private final Map<String,Integer> totalUserQueriesPerSystem;
    
    // The total active queries on the system.
    private final int totalSystemQueries;
    
    // The total queries the system has based on the query logic.
    private final int totalSystemQueriesForQueryLogic;
    
    // The total queries across all systems based on the query logic.
    private final int totalQueriesForQueryLogic;
    
    // The timestamp when this snapshot was captured.
    private final long timestamp;
    
    public static Builder builder(String userDn, String systemName, String queryLogic) {
        return new Builder(userDn, systemName, queryLogic);
    }
    
    protected ActiveQuerySnapshot(Builder builder) {
        this.userDn = builder.userDn;
        this.systemName = builder.systemName;
        this.queryLogic = builder.queryLogic;
        this.totalUserQueriesForQueryLogic = builder.userQueriesForQueryLogic;
        this.totalUserQueriesPerSystem = Map.copyOf(builder.userQueriesPerSystem);
        this.totalSystemQueries = builder.systemQueries;
        this.totalSystemQueriesForQueryLogic = builder.systemQueriesForQueryLogic;
        this.totalQueriesForQueryLogic = builder.queriesForQueryLogic;
        this.timestamp = builder.timestamp;
    }
    
    public String getUserDn() {
        return userDn;
    }
    
    public String getSystemName() {
        return systemName;
    }
    
    public String getQueryLogic() {
        return queryLogic;
    }
    
    public int getTotalUserQueriesOnSystem() {
        return totalUserQueriesPerSystem.getOrDefault(systemName, 0);
    }
    
    public int getTotalUserQueriesForQueryLogic() {
        return totalUserQueriesForQueryLogic;
    }
    
    public Map<String,Integer> getTotalUserQueriesPerSystem() {
        return totalUserQueriesPerSystem;
    }
    
    public int getTotalSystemQueries() {
        return totalSystemQueries;
    }
    
    public int getTotalSystemQueriesForQueryLogic() {
        return totalSystemQueriesForQueryLogic;
    }
    
    public int getTotalQueriesForQueryLogic() {
        return totalQueriesForQueryLogic;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public static class Builder {
        private final String userDn;
        private final String systemName;
        private final String queryLogic;
        private final Set<String> alreadyCapturedQueryIds = new HashSet<>();
        
        private final Map<String, Integer> userQueriesPerSystem = new HashMap<>();
        
        private int userQueriesForQueryLogic;
        private int systemQueries;
        private int systemQueriesForQueryLogic;
        private int queriesForQueryLogic;
        
        private long timestamp = System.currentTimeMillis();
        
        public Builder(String userDn, String systemName, String queryLogic) {
            this.userDn = userDn;
            this.systemName = systemName;
            this.queryLogic = queryLogic;
        }
        
        public Builder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }
        
        public Builder capture(String queryId, String userDn, String systemName, String queryLogic) {
            boolean notYetCaptured = alreadyCapturedQueryIds.add(queryId);
            if(notYetCaptured) {
                boolean userMatch = this.userDn.equals(userDn);
                boolean systemMatch = this.systemName.equals(systemName);
                boolean queryLogicMatch = this.queryLogic.equals(queryLogic);
                
                if(userMatch) {
                    userQueriesPerSystem.compute(systemName, (key, value) -> value == null ? 1 : value + 1);
                    
                    if(queryLogicMatch) {
                        userQueriesForQueryLogic++;
                    }
                }
                if(systemMatch) {
                    systemQueries++;
                    if(queryLogicMatch) {
                        systemQueriesForQueryLogic++;
                    }
                }
                if(queryLogicMatch) {
                    queriesForQueryLogic++;
                }
            }
            return this;
        }
        
        public ActiveQuerySnapshot build() {
            return new ActiveQuerySnapshot(this);
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ActiveQuerySnapshot snapshot = (ActiveQuerySnapshot) o;
        return totalUserQueriesForQueryLogic == snapshot.totalUserQueriesForQueryLogic && totalSystemQueries == snapshot.totalSystemQueries
                        && totalSystemQueriesForQueryLogic == snapshot.totalSystemQueriesForQueryLogic
                        && totalQueriesForQueryLogic == snapshot.totalQueriesForQueryLogic && timestamp == snapshot.timestamp && Objects.equals(userDn,
                        snapshot.userDn) && Objects.equals(systemName, snapshot.systemName) && Objects.equals(queryLogic, snapshot.queryLogic)
                        && Objects.equals(totalUserQueriesPerSystem, snapshot.totalUserQueriesPerSystem);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(userDn, systemName, queryLogic, totalUserQueriesForQueryLogic, totalUserQueriesPerSystem, totalSystemQueries,
                        totalSystemQueriesForQueryLogic, totalQueriesForQueryLogic, timestamp);
    }
    
    @Override
    public String toString() {
        return new StringJoiner(", ", ActiveQuerySnapshot.class.getSimpleName() + "[", "]").add("userDn='" + userDn + "'")
                        .add("systemName='" + systemName + "'").add("queryLogic='" + queryLogic + "'")
                        .add("totalUserQueriesForQueryLogic=" + totalUserQueriesForQueryLogic).add("totalUserQueriesPerSystem=" + totalUserQueriesPerSystem)
                        .add("totalSystemQueries=" + totalSystemQueries).add("totalSystemQueriesForQueryLogic=" + totalSystemQueriesForQueryLogic)
                        .add("totalQueriesForQueryLogic=" + totalQueriesForQueryLogic).add("timestamp=" + timestamp).toString();
    }
}
