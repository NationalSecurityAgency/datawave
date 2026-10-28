package datawave.core.common.connection;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;

/**
 * Utility class that centralizes Accumulo table metadata operations.
 * <p>
 * This class replaces direct usage of non-public Accumulo internals ({@code ClientContext}, {@code MetadataServicer}, {@code TabletLocator}, etc.) with public
 * API equivalents where possible. Methods that still require non-public APIs are marked with {@code NON-PUBLIC API} comments.
 *
 * @see <a href="https://github.com/NationalSecurityAgency/datawave/issues/2443">Issue #2443</a>
 */
public final class AccumuloTableUtils {

    private AccumuloTableUtils() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Get the {@link TableId} for a table by name.
     *
     * @param client
     *            the Accumulo client
     * @param tableName
     *            the table name to look up
     * @return the TableId, or {@code null} if the table does not exist
     */
    public static TableId getTableId(AccumuloClient client, String tableName) {
        return getTableId(client.tableOperations(), tableName);
    }

    /**
     * Get the {@link TableId} for a table by name.
     *
     * @param tableOperations
     *            the table operations to use
     * @param tableName
     *            the table name to look up
     * @return the TableId, or {@code null} if the table does not exist
     */
    public static TableId getTableId(TableOperations tableOperations, String tableName) {
        String id = tableOperations.tableIdMap().get(tableName);
        return id == null ? null : TableId.of(id);
    }

    /**
     * Locate tablets for the given ranges and return them grouped by tablet server location, keyed by {@link KeyExtent}.
     * <p>
     * This replaces the {@code TabletLocator.binRanges()} pattern. The returned structure maps tablet server location strings to a map of {@link KeyExtent} to
     * the ranges assigned to that extent, matching the structure expected by downstream consumers like {@code clipRanges()}.
     *
     * @param client
     *            the Accumulo client
     * @param tableName
     *            the table to locate tablets for
     * @param ranges
     *            the ranges to bin into tablet locations
     * @return a map of {@code location -> (extent -> ranges)}
     * @throws AccumuloException
     *             if a general Accumulo error occurs
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    // NON-PUBLIC API: Return type uses KeyExtent (org.apache.accumulo.core.dataImpl) which is non-public.
    // This is required for compatibility with downstream consumers (clipRanges, binOfflineTable).
    // When callers are migrated to use TabletId (public API), this method can be updated.
    //
    // locate() replaces the old TabletLocator.binRanges() retry loop: it invalidates the tablet cache, retries until every range is binned, and checks
    // that the table still exists and is online on each retry.
    public static Map<String,Map<KeyExtent,List<Range>>> locateTablets(AccumuloClient client, String tableName, List<Range> ranges)
                    throws AccumuloException, TableNotFoundException {
        return locateTablets(client.tableOperations(), tableName, ranges);
    }

    /**
     * Locate tablets for the given ranges and return them grouped by tablet server location, keyed by {@link KeyExtent}.
     *
     * @param tableOperations
     *            the table operations to use
     * @param tableName
     *            the table to locate tablets for
     * @param ranges
     *            the ranges to bin into tablet locations
     * @return a map of {@code location -> (extent -> ranges)}
     * @throws AccumuloException
     *             if a general Accumulo error occurs
     * @throws TableNotFoundException
     *             if the table does not exist
     * @see #locateTablets(AccumuloClient, String, List)
     */
    public static Map<String,Map<KeyExtent,List<Range>>> locateTablets(TableOperations tableOperations, String tableName, List<Range> ranges)
                    throws AccumuloException, TableNotFoundException {
        Locations locations;
        try {
            locations = tableOperations.locate(tableName, ranges);
        } catch (AccumuloSecurityException e) {
            throw new AccumuloException(e);
        }
        Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<>();
        for (Map.Entry<TabletId,List<Range>> entry : locations.groupByTablet().entrySet()) {
            TabletId tabletId = entry.getKey();
            String location = locations.getTabletLocation(tabletId);
            if (location == null) {
                location = "";
            }
            KeyExtent extent = KeyExtent.fromTabletId(tabletId);
            binnedRanges.computeIfAbsent(location, k -> new HashMap<>()).put(extent, entry.getValue());
        }
        return binnedRanges;
    }

    /**
     * Get the split points and their tablet server locations for a table.
     * <p>
     * Returns a sorted map of end-row to tablet server location for each tablet. Tablets with no end-row (the last tablet) are excluded. This replaces the
     * {@code MetadataServicer.getTabletLocations()} pattern.
     *
     * @param client
     *            the Accumulo client
     * @param tableName
     *            the table name
     * @return a sorted map of split point (end-row) to tablet server location
     * @throws AccumuloException
     *             if a general Accumulo error occurs
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    public static Map<Text,String> getSplitsWithLocations(AccumuloClient client, String tableName) throws AccumuloException, TableNotFoundException {
        return getSplitsWithLocations(client.tableOperations(), tableName);
    }

    /**
     * Get the split points and their tablet server locations for a table.
     *
     * @param tableOperations
     *            the table operations to use
     * @param tableName
     *            the table name
     * @return a sorted map of split point (end-row) to tablet server location
     * @throws AccumuloException
     *             if a general Accumulo error occurs
     * @throws TableNotFoundException
     *             if the table does not exist
     * @see #getSplitsWithLocations(AccumuloClient, String)
     */
    public static Map<Text,String> getSplitsWithLocations(TableOperations tableOperations, String tableName) throws AccumuloException, TableNotFoundException {
        Locations locations;
        try {
            locations = tableOperations.locate(tableName, Collections.singletonList(new Range()));
        } catch (AccumuloSecurityException e) {
            throw new AccumuloException(e);
        }
        Map<Text,String> result = new TreeMap<>();
        for (Map.Entry<TabletId,List<Range>> entry : locations.groupByTablet().entrySet()) {
            TabletId tabletId = entry.getKey();
            Text endRow = tabletId.getEndRow();
            if (endRow != null) {
                String location = locations.getTabletLocation(tabletId);
                result.put(endRow, location == null ? "" : location);
            }
        }
        return result;
    }

    /**
     * Get the count of running major compactions across all compactors.
     * <p>
     * Accumulo 4 removed the manager stats RPC ({@code ManagerMonitorInfo}) that reported queued and running major compactions per table, so this counts the
     * running non-minor compactions reported by the public {@code InstanceOperations.getActiveCompactions} API. Queued compactions are not included.
     *
     * @param client
     *            the Accumulo client
     * @return the number of running major compactions
     * @throws AccumuloException
     *             if a general Accumulo error occurs
     */
    // ACCUMULO4_TODO this used to get running+queued compactions from the manager. getActiveCompactions() reaches out to every compactor and asks what it
    // is compacting, and only reports running compactions. It would be better to get this from a place where the information is already summarized, like
    // the metrics system, monitor, or manager. See apache/accumulo#4939.
    public static int getMajorCompactionCount(AccumuloClient client) throws AccumuloException {
        try {
            return (int) client.instanceOperations().getActiveCompactions(client.instanceOperations().getServers(ServerId.Type.COMPACTOR)).stream()
                            .filter(activeCompaction -> activeCompaction.getType() != ActiveCompaction.CompactionType.MINOR).count();
        } catch (AccumuloSecurityException e) {
            throw new AccumuloException("Unable to retrieve major compaction stats", e);
        }
    }
}
