package datawave.ingest.table.balancer;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.master.balancer.GroupBalancer;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A custom tablet balancer designed to work with a date-partitioned (sharded) table. This balancer is based on the {@link GroupBalancer}, which spreads tablets
 * belonging to a group out across the tablet servers. If there are fewer tablet servers than group members for a given group, then the extra tablets are still
 * spread out evenly across the tablet servers.
 * <p>
 * The partition function used by this balancer does not simply group shards by date (all shards for a given date belong to a group). Although that would ensure
 * that all the pieces of a given day are spread across tablet servers, there are no guarantees about how different groups are spread across tablet servers.
 * Therefore, a legal balance could have successive days on the same tablet servers. For example, if a day were partitioned into 100 pieces and the cluster had
 * 500 tablet servers, a legal balance of 20 days of data could have days 1-5 all on the first 100 tablet servers, days 6-10 on the second 100 tablet servers,
 * and so on. This is not ideal, since the real goal is to spread data out across the cluster as much as possible.
 */
public class ShardedTableTabletBalancer extends GroupBalancer {
    private static final String SHARDED_PROPERTY_PREFIX = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "sharded.balancer.";
    public static final String SHARDED_MAX_MIGRATIONS = SHARDED_PROPERTY_PREFIX + "max.migrations";
    public static final int MAX_MIGRATIONS_DEFAULT = 10000;
    
    private static final Logger log = Logger.getLogger(ShardedTableTabletBalancer.class);
    private Collection<Pair<KeyExtent,Location>> tabletLocationCache;
    private Function<KeyExtent,String> partitioner;
    private String tableId;
    
    public ShardedTableTabletBalancer(String tableId) {
        super(tableId);
        this.tableId = tableId;
    }
    
    // synchronized to ensure exclusivity between getAssignments and balance calls
    @Override
    public synchronized void getAssignments(SortedMap<TServerInstance,TabletServerStatus> current, Map<KeyExtent,TServerInstance> unassigned,
                    Map<KeyExtent,TServerInstance> assignments) {
        // During getAssignments, we'll just partition using the shard's day.
        partitioner = new ShardDayPartitioner();
        
        super.getAssignments(current, unassigned, assignments);
    }
    
    // synchronized to ensure exclusivity between getAssignments and balance calls
    @Override
    public synchronized long balance(SortedMap<TServerInstance,TabletServerStatus> current, Set<KeyExtent> migrations, List<TabletMigration> migrationsOut) {
        // Clear the location cache so we're sure to rebuild for this balancer pass
        tabletLocationCache = null;
        
        // During balancing, we actually want to balance using groups that include multiple days, in order to ensure that
        // data doesn't cluster weeks or months of data on a subset of the cluster.
        final int numTservers = current.size();
        partitioner = new ShardGroupPartitioner(numTservers, getLocationProvider());
        
        return super.balance(current, migrations, migrationsOut);
    }
    
    @Override
    protected Function<KeyExtent,String> getPartitioner() {
        return partitioner;
    }
    
    @Override
    protected Iterable<Pair<KeyExtent,Location>> getLocationProvider() {
        // Cache metadata locations so we only scan the metadata table once per balancer pass
        if (tabletLocationCache == null) {
            tabletLocationCache = new LinkedList<>();
            Iterables.addAll(tabletLocationCache, getRawLocationProvider());
            tabletLocationCache = Collections.unmodifiableCollection(tabletLocationCache);
        }
        return tabletLocationCache;
    }
    
    @Override
    protected int getMaxMigrations() {
        int maxMigrations = MAX_MIGRATIONS_DEFAULT;
        try {
            String maxMigrationsProp = this.configuration.getTableConfiguration(this.tableId).get(SHARDED_MAX_MIGRATIONS);
            if (maxMigrationsProp != null && !maxMigrationsProp.isEmpty()) {
                try {
                    maxMigrations = Integer.parseInt(maxMigrationsProp);
                } catch (Exception e) {
                    log.error("Unable to parse " + SHARDED_MAX_MIGRATIONS + " value (" + maxMigrationsProp + ") as an integer.  Defaulting to " + maxMigrations);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to get " + SHARDED_MAX_MIGRATIONS + ".  Defaulting to " + maxMigrations, e);
        }
        return maxMigrations;
    }
    
    /**
     * Gets the raw location provider. By default this just delegates to the parent class' {@link #getLocationProvider()} which scans the metadata table.
     * However, test cases might override in order to replace the parent metadata location provider whilst still allowing the caching mechanism in use here.
     * 
     * @return iterable location provider
     */
    protected Iterable<Pair<KeyExtent,Location>> getRawLocationProvider() {
        return super.getLocationProvider();
    }
    
    /**
     * Partitions extents into groups according to the "day" portion of the end row. That is, the end row is expected to be in the form yyyymmdd_x, and the
     * partitioner returns yyyymmdd.
     */
    protected static class ShardDayPartitioner implements Function<KeyExtent,String> {
        @Override
        public String apply(KeyExtent extent) {
            String date = "null"; // Don't return null
            if (extent != null) {
                Text endRow = extent.getEndRow();
                if (endRow != null) {
                    int sepIdx = endRow.find("_");
                    if (sepIdx < 0)
                        sepIdx = endRow.getLength();
                    date = new String(endRow.getBytes(), 0, sepIdx);
                }
            }
            return date;
        }
    }
    
    /**
     * Partitions extents for this table into groups as follows:
     * <ul>
     * <li>All pieces of a given day are in the same group</li>
     * <li>All pieces of multiple days are in the same group. The number of days included in a given group is as many as will fit into 95% of all the tservers.
     * We don't use 100% to minimize re-balancing when the number of tservers changes.</li>
     * </ul>
     */
    protected static class ShardGroupPartitioner implements Function<KeyExtent,String> {
        private TreeMap<KeyExtent,String> groupIDs = new TreeMap<>();
        
        /**
         * @param numTservers
         *            the number of active tablet servers
         * @param tabletLocations
         *            the sorted list of tablet and current/previous location pairs
         */
        public ShardGroupPartitioner(int numTservers, Iterable<Pair<KeyExtent,Location>> tabletLocations) {
            int groupSize = Math.round(numTservers * 0.95f);
            int numInGroup = 0;
            int groupNumber = 1;
            
            String groupID = String.format("g%04d", groupNumber);
            KeyExtent groupStartExtent = tabletLocations.iterator().next().getFirst();
            int groupStartNumber = numInGroup;
            byte[] prevDate = retrieveDate(groupStartExtent);
            groupIDs.put(groupStartExtent, groupID);
            for (Pair<KeyExtent,Location> pair : tabletLocations) {
                KeyExtent extent = pair.getFirst();
                
                // The date changed, so save this extent as a (potential) new group start extent
                if (!sameDate(extent, prevDate)) {
                    groupStartExtent = extent;
                    groupStartNumber = numInGroup;
                    prevDate = retrieveDate(extent);
                }
                
                // If we just exceeded the number of entries in a group, then add a new group and use
                // the most recent group start extent as the beginning of the new group. This ensures that
                // all pieces of a given day go into the same group, but we add as many consecutive days as
                // possible to each group (as many as will fit on 95% of the available tservers).
                ++numInGroup;
                if (numInGroup > groupSize) {
                    numInGroup -= groupStartNumber;
                    ++groupNumber;
                    groupID = String.format("g%04d", groupNumber);
                    groupIDs.put(groupStartExtent, groupID);
                }
            }
            
            // If the final group is small enough that we can merge it into the previous group
            // without the previous group having more members than tservers, then do so.
            int tooSmallGroupSize = numTservers - groupSize;
            if (numInGroup <= tooSmallGroupSize && groupIDs.size() > 1) {
                groupIDs.remove(groupIDs.lastKey());
            }
        }
        
        @Override
        public String apply(KeyExtent input) {
            // Finds the group start that is nearest (less than or equal to) to the supplied extent, and uses that calculated group value.
            Entry<KeyExtent,String> entry = groupIDs.floorEntry(input);
            return entry == null ? "extra" : entry.getValue();
        }
        
        private byte[] retrieveDate(KeyExtent extent) {
            Text endRow = extent.getEndRow();
            if (endRow == null)
                endRow = extent.getPrevEndRow();
            if (endRow == null) {
                log.warn("Attempting to retrieve date from empty extent " + extent + ". Is your sharded table pre-split?");
                return "null".getBytes();
            } else {
                int idx = endRow.find("_");
                if (idx <= 0) {
                    idx = endRow.getLength();
                    log.warn("Extent " + extent + " does not conform to sharded date scheme yyyyMMdd_num");
                }
                return Arrays.copyOf(endRow.getBytes(), idx);
            }
        }
        
        private boolean sameDate(KeyExtent extent, byte[] date) {
            Text endRow = extent.getEndRow();
            if (endRow == null)
                endRow = extent.getPrevEndRow();
            if (endRow == null) {
                log.warn("Attempting to compare date from empty extent " + extent + ". Is your sharded table pre-split?");
                return date == null || date.length == 0;
            } else {
                return WritableComparator.compareBytes(endRow.getBytes(), 0, date.length, date, 0, date.length) == 0;
            }
        }
    }
}
