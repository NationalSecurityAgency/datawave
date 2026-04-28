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
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
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
        String id = client.tableOperations().tableIdMap().get(tableName);
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
     * @throws AccumuloSecurityException
     *             if a security error occurs
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    // NON-PUBLIC API: Return type uses KeyExtent (org.apache.accumulo.core.dataImpl) which is non-public.
    // This is required for compatibility with downstream consumers (clipRanges, binOfflineTable).
    // When callers are migrated to use TabletId (public API), this method can be updated.
    //
    // REVIEW: The original TabletLocator.binRanges() used a retry loop for partial binning failures.
    // The public locate() API may handle this internally. Verify retry semantics are equivalent.
    public static Map<String,Map<KeyExtent,List<Range>>> locateTablets(AccumuloClient client, String tableName, List<Range> ranges)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        Locations locations = client.tableOperations().locate(tableName, ranges);
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
     * @throws AccumuloSecurityException
     *             if a security error occurs
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    public static Map<Text,String> getSplitsWithLocations(AccumuloClient client, String tableName)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        Locations locations = client.tableOperations().locate(tableName, Collections.singletonList(new Range()));
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

    // NON-PUBLIC API: Uses Thrift RPC to get queued + running compaction counts.
    // No public Accumulo API exists for queued compactions as of 2.1.x.
    // Tracking: https://github.com/apache/accumulo/issues/5965
    //
    // To swap out: when a public API for compaction counts is available,
    // replace this method body and remove the Thrift imports above.
    /**
     * Get the count of queued and running major compactions across all tablet servers.
     *
     * @param client
     *            the Accumulo client
     * @return the number of queued and running major compactions
     * @throws AccumuloException
     *             if a general Accumulo error occurs
     */
    public static int getMajorCompactionCount(AccumuloClient client) throws AccumuloException {
        int majC = 0;

        ClientContext context = (ClientContext) client;
        ManagerClientService.Client managerClient = null;
        try {
            managerClient = ThriftClientTypes.MANAGER.getConnection(context);
            ManagerMonitorInfo mmi = managerClient.getManagerStats(null, context.rpcCreds());
            Map<String,TableInfo> tableStats = mmi.getTableMap();

            for (Map.Entry<String,TableInfo> e : tableStats.entrySet()) {
                majC += e.getValue().getMajors().getQueued();
                majC += e.getValue().getMajors().getRunning();
            }
        } catch (Exception e) {
            throw new AccumuloException("Unable to retrieve major compaction stats", e);
        } finally {
            if (managerClient != null) {
                ThriftUtil.close(managerClient, context);
            }
        }

        return majC;
    }
}
