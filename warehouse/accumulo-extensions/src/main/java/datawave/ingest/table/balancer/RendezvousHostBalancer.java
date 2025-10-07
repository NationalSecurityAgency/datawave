package datawave.ingest.table.balancer;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.balancer.BalancerEnvironment;
import org.apache.accumulo.core.spi.balancer.TabletBalancer;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;

import com.google.common.base.Preconditions;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.primitives.UnsignedBytes;

/**
 * Rendezvous hashing server partitioning balancer. This balancer has the following goals.
 *
 * <ul>
 * <li>Spread tablets in the same group across different host names. If a hostname has multiple tablet servers then avoid placing multiple tablets on that
 * host.</li>
 * <li>Allow assigning a group of tablets to a group of tablet servers.</li>
 * <li>Minimize the amount of rebalancing needed when the set of tablet servers changes.</li>
 * <li>Avoid uneven tablet assignment, where the tablets per tserver for a server are significantly different than the average. This balancer uses hashing, so
 * there should be a small deviation from the average.</li>
 * </ul>
 *
 * <p>
 * For a given group name (like 20250618) this balancer computes hash(group_name+hostname) for each host and then sorts on the hashes. This gives each group a
 * consistent and unique ordering across host. Within a host, hash(group_name+hostname+port) is computed for each tserver and sorted by the hashes giving unique
 * per group order for the tservers on the host..
 * </p>
 *
 */
public abstract class RendezvousHostBalancer implements TabletBalancer {
    private BalancerEnvironment env;

    protected final TableId tableIdToBalance;

    protected long lastRunTime = 0;

    public RendezvousHostBalancer(TableId tableId) {
        this.tableIdToBalance = tableId;
    }

    /**
     * For a given tabletId returns a group name. This balancer places tablets in the same group on different hosts.
     *
     * @param tabletId
     *            the requested tablet id
     * @return The corresponding tablet group for the tablet id.
     */
    protected abstract String getTabletGroup(TabletId tabletId);

    protected abstract int getMaxMigrations();

    /**
     * Creates a function that determines what tservers should be used for a named group of tablets. The function should map a name returned by
     * {@link #getTabletGroup(TabletId)} to a subset of tablet servers.
     *
     * @param allTservers
     *            set of tservers to use for partitioning
     * @return The function which will partition the group of tservers
     */
    protected abstract Function<String,List<TabletServerId>> getServerPartitioner(Collection<TabletServerId> allTservers);

    @Override
    public void init(BalancerEnvironment balancerEnvironment) {
        this.env = balancerEnvironment;
    }

    private static class TServers {

        Map<Integer,Map<String,List<TabletServerId>>> tservers = new HashMap<>();

        TServers(List<TabletServerId> tabletServers) {
            Map<String,List<TabletServerId>> tserversPerHost = new HashMap<>();
            for (var tserver : tabletServers) {
                tserversPerHost.computeIfAbsent(tserver.getHost(), h -> new ArrayList<>()).add(tserver);
            }

            tserversPerHost.forEach((host, tsList) -> {
                tservers.computeIfAbsent(tsList.size(), s -> new HashMap<>()).put(host, tsList);
            });
        }

        Map<String,List<TabletServerId>> getTserverWithNHost(int numHost) {
            return tservers.get(numHost);
        }

        int getTotalNumberOfTservers() {
            int total = 0;
            for (var entry : tservers.entrySet()) {
                total += entry.getKey() * entry.getValue().size();
            }
            return total;
        }

        public boolean isEmpty() {
            return tservers.isEmpty();
        }

        /**
         * If there are 1000 tablets and there are 40 hosts with 5 tservers and 10 hosts with 4 tservers then this function will do the following
         *
         * <ul>
         * <li>compute there are a total of 40*5+10*4 = 240 tablet servers.</li>
         * <li>For the 40 host with 5 tservers, they are 200/240=83.33% of the tservers. So give 833 of the 1000 tablets to this set of tservers.</li>
         * <li>For the 10 host with 4 tservers, they are 40/240=16.66% of the tservers. So give 167 of the 1000 tablets to this set of tservers.</li>
         * </ul>
         *
         * <p>
         * For the example above would return Map.of(5, 833, 4, 167);
         */
        public Map<Integer,Integer> computeTabletAssignmentCountsByHostCount(int numTablets) {
            if (tservers.size() == 1) {
                return Map.of(tservers.keySet().iterator().next(), numTablets);
            }

            Map<Integer,Integer> counts = new HashMap<>();

            double totalTservers = getTotalNumberOfTservers();

            int tabletsLeft = numTablets;

            for (var entry : tservers.entrySet()) {
                int tserversPerHost = entry.getKey();
                int numHost = entry.getValue().size();
                double numTservers = tserversPerHost * numHost;

                double percent = numTservers / totalTservers;

                int tablets = Math.min(tabletsLeft, (int) Math.round(numTablets * percent));
                if (tablets > 0) {
                    counts.put(tserversPerHost, tablets);
                    tabletsLeft -= tablets;
                }
            }

            Preconditions.checkState(tabletsLeft == 0, tabletsLeft + " tablets are unassigned. Ensure the tserver regex patterns are correct");

            return counts;
        }
    }

    @Override
    public void getAssignments(AssignmentParameters assignmentParameters) {

        var currentTservers = assignmentParameters.currentStatus();
        if (currentTservers.isEmpty()) {
            return;
        }

        Map<String,TabletServerId> tserverHostPortMap = new HashMap<>();

        currentTservers.keySet().forEach(tserverId -> {
            tserverHostPortMap.put(tserverId.getHost() + ":" + tserverId.getPort(), tserverId);
        });

        // FOLLOW_ON this reads all tablet locations because that is the only option. Should read only the tablet locations related to the tablet groups that
        // are being assigned. This would require Accumulo changes.
        Map<TabletId,TabletServerId> allLocations = new HashMap<>(env.listTabletLocations(tableIdToBalance));
        Map<TabletId,TabletServerId> lastLocations = assignmentParameters.unassignedTablets();
        lastLocations.forEach((tablet, last) -> {
            allLocations.put(tablet, last == null ? null : tserverHostPortMap.get(last.getHost() + ":" + last.getPort()));
        });
        Map<String,List<TabletId>> tabletsToAssign = groupTablets(lastLocations.keySet());
        Map<String,List<TabletId>> allTabletsGrouped = groupTablets(allLocations.keySet(), tabletsToAssign::containsKey);

        Function<String,List<TabletServerId>> tabletServerPartitioner = getServerPartitioner(currentTservers.keySet());
        Map<String,TServers> computedTservers = new HashMap<>();

        tabletsToAssign.forEach((tabletGroup, tablets) -> {
            TServers tserversForGroup = computedTservers.computeIfAbsent(tabletGroup, g -> new TServers(tabletServerPartitioner.apply(tabletGroup)));

            if (!tserversForGroup.isEmpty()) {
                // Must pass the set of all tablets for the group to compute the desired location, not just the subset of tablets being assigned
                var allTablets = allTabletsGrouped.get(tabletGroup);
                Map<TabletId,TabletServerId> desiredLocs = getDesiredLocationsForTabletGroup(tabletGroup, allTablets, tserversForGroup, allLocations);

                tablets.forEach(tabletId -> {
                    assignmentParameters.addAssignment(tabletId, desiredLocs.get(tabletId));
                });
            } // else there are no tservers so can not assign
        });

    }

    private boolean shouldBalance(SortedMap<TabletServerId,TServerStatus> current, Set<TabletId> migrations) {
        if (current.size() < 2) {
            return false;
        }
        return migrations.stream().noneMatch(t -> t.getTable().equals(tableIdToBalance));
    }

    /**
     * The amount of time to wait between balancing.
     *
     * @return the static time to wait
     */
    protected long getWaitTime() {
        return 60000;
    }

    @Override
    public long balance(BalanceParameters balanceParameters) {
        var currentTservers = balanceParameters.currentStatus();

        if (!shouldBalance(currentTservers, balanceParameters.currentMigrations())) {
            return 5000;
        }

        if (System.currentTimeMillis() - lastRunTime < getWaitTime()) {
            return 5000;
        }

        if (currentTservers.isEmpty()) {
            return 5000;
        }

        Map<TabletId,TabletServerId> allLocations = env.listTabletLocations(tableIdToBalance);

        Map<String,List<TabletId>> tabletsPerGroup = groupTablets(allLocations.keySet());

        Function<String,List<TabletServerId>> tabletServerPartitioner = getServerPartitioner(currentTservers.keySet());
        Map<String,TServers> computedTservers = new HashMap<>();

        // read once so a consistent max is used for the entire balance call
        final int maxMigrations = getMaxMigrations();

        int migrationsAdded = 0;
        outer: for (var tpgEntry : tabletsPerGroup.entrySet()) {
            String tabletGroup = tpgEntry.getKey();
            List<TabletId> tablets = tpgEntry.getValue();
            TServers tserversForGroup = computedTservers.computeIfAbsent(tabletGroup, g -> new TServers(tabletServerPartitioner.apply(tabletGroup)));

            if (!tserversForGroup.isEmpty()) {
                Map<TabletId,TabletServerId> desiredLocs = getDesiredLocationsForTabletGroup(tabletGroup, tablets, tserversForGroup, allLocations);

                for (var dlEntry : desiredLocs.entrySet()) {
                    TabletId tabletId = dlEntry.getKey();
                    TabletServerId tabletServerId = dlEntry.getValue();
                    // This code could handle assigning tablets w/o a location, however the balancer framework does not support this so ignore tablets w/o a
                    // location. Normally accumulo will only call the balancer when all tablets are assigned. However if a tablet becomes unassigned during
                    // balancing a null location may be seen.
                    var currTsever = allLocations.get(tabletId);
                    if (currTsever != null && !currTsever.equals(tabletServerId)) {
                        balanceParameters.migrationsOut().add(new TabletMigration(tabletId, currTsever, tabletServerId));
                        migrationsAdded++;
                        if (migrationsAdded >= maxMigrations) {
                            break outer;
                        }
                    }
                }
            } // else there are not tservers so can not balance
        }

        lastRunTime = System.currentTimeMillis();

        return 5000;
    }

    private Map<String,List<TabletId>> groupTablets(Set<TabletId> tablets) {
        Map<String,List<TabletId>> tabletsPerGroup = new HashMap<>();
        tablets.forEach(tabletId -> {
            var group = getTabletGroup(tabletId);
            tabletsPerGroup.computeIfAbsent(group, g -> new ArrayList<>()).add(tabletId);
        });
        return tabletsPerGroup;
    }

    private Map<String,List<TabletId>> groupTablets(Set<TabletId> tablets, Predicate<String> groupFilter) {
        Map<String,List<TabletId>> tabletsPerGroup = new HashMap<>();
        tablets.forEach(tabletId -> {
            var group = getTabletGroup(tabletId);
            if (groupFilter.test(group)) {
                tabletsPerGroup.computeIfAbsent(group, g -> new ArrayList<>()).add(tabletId);
            }
        });
        return tabletsPerGroup;
    }

    /**
     * @param tabletGroupName
     *            Name of tablet group
     * @param tabletsInGroup
     *            List of tablets in the tablet group
     * @param tserversForGroup
     *            Map of tservers that tablets can be assigned to, the map is expected to be keyed on hostname
     * @param currentLocations
     *            map of current tablet locations
     * @return Map of desired locations for a given tablet group name and list of tablets.
     */
    private Map<TabletId,TabletServerId> getDesiredLocationsForTabletGroup(String tabletGroupName, List<TabletId> tabletsInGroup, TServers tserversForGroup,
                    Map<TabletId,TabletServerId> currentLocations) {

        Map<TabletServerId,Integer> tserverGoalCounts = new LinkedHashMap<>();

        // If rendezvous balance was done across host w/ different numbers of tablets, then it would lead to hosts with less tservers getting more tablets per
        // tserver. To deal with this hosts with different numbers of tablets servers are partitioned. Then rendezvous balancing is done across each partition.
        Map<Integer,Integer> tabletsCountsPerHostCount = tserversForGroup.computeTabletAssignmentCountsByHostCount(tabletsInGroup.size());

        for (var entry : tabletsCountsPerHostCount.entrySet()) {
            int tserversPerHost = entry.getKey();
            // the number of tablets for this
            int numTablets = entry.getValue();
            // all of the tservers that have this number of tablets per host
            Map<String,List<TabletServerId>> tabletServers = tserversForGroup.getTserverWithNHost(tserversPerHost);

            // Use rendezvous hashing to first determine the goal number of tablets that each hostname should have
            Map<String,Integer> hostGoalCounts = getGoalCounts(tabletGroupName, numTablets, tabletServers.keySet(), h -> h);
            // For each hostname, use rendezvous hashing to determine how many tablets each tserver on that host should have
            hostGoalCounts.forEach((hostname, goalTablets) -> {
                var goalCounts = getGoalCounts(tabletGroupName, goalTablets, tabletServers.get(hostname), tsi -> tsi.getHost() + ":" + tsi.getPort());
                Preconditions.checkState(Collections.disjoint(tserverGoalCounts.keySet(), goalCounts.keySet()));
                tserverGoalCounts.putAll(goalCounts);
            });
        }

        Map<TabletId,TabletServerId> desiredLocations = new HashMap<>();
        // find all tablets that can remain at their current or last location
        for (var tabletId : tabletsInGroup) {
            var currentTserverId = currentLocations.get(tabletId);
            if (currentTserverId != null) {
                int goalCount = tserverGoalCounts.getOrDefault(currentTserverId, 0);
                if (goalCount > 0) {
                    desiredLocations.put(tabletId, currentTserverId);
                    goalCount--;
                    if (goalCount == 0) {
                        tserverGoalCounts.remove(currentTserverId);
                    } else {
                        tserverGoalCounts.put(currentTserverId, goalCount);
                    }
                }
            }
        }

        if (desiredLocations.size() == tabletsInGroup.size()) {
            Preconditions.checkState(tserverGoalCounts.isEmpty());
            return desiredLocations;
        }

        // assign tablets to new locations
        for (var tabletId : tabletsInGroup) {
            if (!desiredLocations.containsKey(tabletId)) {
                var iter = tserverGoalCounts.entrySet().iterator();
                Map.Entry<TabletServerId,Integer> next = iter.next();
                TabletServerId nextTserver = next.getKey();
                int nextCount = next.getValue();
                Preconditions.checkState(nextCount > 0);
                desiredLocations.put(tabletId, nextTserver);
                nextCount--;
                if (nextCount > 0) {
                    next.setValue(nextCount);
                } else {
                    iter.remove();
                }
            }
        }

        Preconditions.checkState(tserverGoalCounts.isEmpty());

        return desiredLocations;
    }

    private <T> Map<T,Integer> getGoalCounts(String tabletGroup, int numTablets, Collection<T> destinations, Function<T,String> destToStringFunction) {
        List<Map.Entry<HashCode,T>> hashCodes = new ArrayList<>(destinations.size());

        // Compute rendezvous hashes
        for (T dest : destinations) {
            var destString = destToStringFunction.apply(dest);
            HashCode hashCode = Hashing.murmur3_128().hashString(destString + ":" + tabletGroup, StandardCharsets.UTF_8);
            hashCodes.add(new AbstractMap.SimpleImmutableEntry<>(hashCode, dest));
        }

        // order by rendezvous hashes
        var comparator = UnsignedBytes.lexicographicalComparator();
        hashCodes.sort((e1, e2) -> comparator.compare(e1.getKey().asBytes(), e2.getKey().asBytes()));

        int numTabletsOnAll = numTablets / destinations.size();
        int numDestinationsWithOneMore = numTablets % destinations.size();

        Map<T,Integer> goalCounts = new LinkedHashMap<>();

        for (int i = 0; i < numDestinationsWithOneMore; i++) {
            goalCounts.put(hashCodes.get(i).getValue(), numTabletsOnAll + 1);
        }

        if (numTabletsOnAll > 0) {
            for (int i = numDestinationsWithOneMore; i < hashCodes.size(); i++) {
                goalCounts.put(hashCodes.get(i).getValue(), numTabletsOnAll);
            }
        }

        return goalCounts;
    }
}
