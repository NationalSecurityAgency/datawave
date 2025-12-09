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
    private final String system;

    // The query logics targeted for capture in this snapshot.
    private final Set<String> queryLogics;

    // The total queries the user has running per system.
    private final Map<String,Integer> totalUserQueriesPerSystem;

    private final Map<String,Map<String,Integer>> totalUserQueriesPerSystemPerQueryLogic;

    // The total active queries on the system.
    private final int totalSystemQueries;

    // A map of query logics to non-zero counts of active queries for the user.
    private final Map<String,Integer> totalUserQueriesPerQueryLogic;

    // A map of query logics to non-zero counts of active queries for the system.
    private final Map<String,Integer> totalSystemQueriesPerQueryLogic;

    // The timestamp when this snapshot was captured.
    private final long timestamp;

    public static Builder builder(String userDn, String systemName, Set<String> queryLogics) {
        return new Builder(userDn, systemName, queryLogics);
    }

    protected ActiveQuerySnapshot(Builder builder) {
        this.userDn = builder.userDn;
        this.system = builder.system;
        this.queryLogics = builder.queryLogics;
        this.totalUserQueriesPerSystem = Map.copyOf(builder.userQueriesPerSystem);
        this.totalUserQueriesPerSystemPerQueryLogic = Map.copyOf(builder.userQueriesPerSystemPerQueryLogic);
        this.totalSystemQueries = builder.systemQueries;
        this.totalUserQueriesPerQueryLogic = Map.copyOf(builder.userQueriesPerQueryLogic);
        this.totalSystemQueriesPerQueryLogic = Map.copyOf(builder.systemQueriesPerQueryLogic);
        this.timestamp = builder.timestamp;
    }

    /**
     * Return the user the candidate query was submitted by.
     *
     * @return the user DN
     */
    public String getUserDn() {
        return userDn;
    }

    /**
     * Return the system the candidate query was submitted on.
     *
     * @return the system
     */
    public String getSystem() {
        return system;
    }

    /**
     * Return the set of query logics that match against the best-matching groups for the query logic of the candidate query.
     *
     * @return the query logics
     */
    public Set<String> getQueryLogics() {
        return queryLogics;
    }

    /**
     * Return a map of systems to the total number of queries the user has actively running on the system.
     *
     * @return the map
     */
    public Map<String,Integer> getUserQueriesPerSystem() {
        return totalUserQueriesPerSystem;
    }

    /**
     * Return a map of systems to a map of query logics to the total number of queries the user has actively running per query logic per system.
     *
     * @return the map
     */
    public Map<String,Map<String,Integer>> getTotalUserQueriesPerSystemPerQueryLogic() {
        return totalUserQueriesPerSystemPerQueryLogic;
    }

    /**
     * Return the total number of queries actively running on the system.
     *
     * @return the total
     */
    public int getTotalSystemQueries() {
        return totalSystemQueries;
    }

    /**
     * Return a map of query logics to the total number of actively running queries for those query logics on the system
     *
     * @return the map
     */
    public Map<String,Integer> getTotalSystemQueriesPerQueryLogic() {
        return totalSystemQueriesPerQueryLogic;
    }

    /**
     * Return the timestamp of when this snapshot was collected
     *
     * @return the snapshot timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    public static class Builder {

        private final String userDn;
        private final String system;
        private final Set<String> queryLogics;
        private final Set<String> alreadyCapturedQueryIds = new HashSet<>();

        private final Map<String,Integer> userQueriesPerSystem = new HashMap<>();
        private final Map<String,Map<String,Integer>> userQueriesPerSystemPerQueryLogic = new HashMap<>();
        private final Map<String,Integer> userQueriesPerQueryLogic = new HashMap<>();
        private final Map<String,Integer> systemQueriesPerQueryLogic = new HashMap<>();

        private int systemQueries;

        private long timestamp = System.currentTimeMillis();

        public Builder(String userDn, String system, Set<String> queryLogics) {
            this.userDn = userDn;
            this.system = system;
            this.queryLogics = queryLogics;
        }

        public Builder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder capture(String queryId, String userDn, String systemName, String queryLogic) {
            boolean notYetCaptured = alreadyCapturedQueryIds.add(queryId);
            if (notYetCaptured) {
                if (this.userDn.equals(userDn)) {
                    userQueriesPerSystem.compute(systemName, (key, value) -> value == null ? 1 : value + 1);
                    userQueriesPerQueryLogic.compute(queryLogic, (key, value) -> value == null ? 1 : value + 1);
                    Map<String,Integer> queryLogicCounts = userQueriesPerSystemPerQueryLogic.computeIfAbsent(systemName, key -> new HashMap<>());
                    queryLogicCounts.compute(queryLogic, (key, value) -> value == null ? 1 : value + 1);
                }
                if (this.system.equals(systemName)) {
                    systemQueries++;
                    systemQueriesPerQueryLogic.compute(queryLogic, (key, value) -> value == null ? 1 : value + 1);
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
        ActiveQuerySnapshot that = (ActiveQuerySnapshot) o;
        return totalSystemQueries == that.totalSystemQueries && timestamp == that.timestamp && Objects.equals(userDn, that.userDn)
                        && Objects.equals(system, that.system) && Objects.equals(queryLogics, that.queryLogics)
                        && Objects.equals(totalUserQueriesPerSystem, that.totalUserQueriesPerSystem)
                        && Objects.equals(totalUserQueriesPerSystemPerQueryLogic, that.totalUserQueriesPerSystemPerQueryLogic)
                        && Objects.equals(totalUserQueriesPerQueryLogic, that.totalUserQueriesPerQueryLogic)
                        && Objects.equals(totalSystemQueriesPerQueryLogic, that.totalSystemQueriesPerQueryLogic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userDn, system, queryLogics, totalUserQueriesPerSystem, totalUserQueriesPerSystemPerQueryLogic, totalSystemQueries,
                        totalUserQueriesPerQueryLogic, totalSystemQueriesPerQueryLogic, timestamp);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ActiveQuerySnapshot.class.getSimpleName() + "[", "]").add("userDn='" + userDn + "'").add("systemName='" + system + "'")
                        .add("queryLogics=" + queryLogics).add("totalUserQueriesPerSystem=" + totalUserQueriesPerSystem)
                        .add("totalUserQueriesPerSystemPerQueryLogic=" + totalUserQueriesPerSystemPerQueryLogic).add("totalSystemQueries=" + totalSystemQueries)
                        .add("totalUserQueriesPerQueryLogic=" + totalUserQueriesPerQueryLogic)
                        .add("totalSystemQueriesPerQueryLogic=" + totalSystemQueriesPerQueryLogic).add("timestamp=" + timestamp).toString();
    }
}
