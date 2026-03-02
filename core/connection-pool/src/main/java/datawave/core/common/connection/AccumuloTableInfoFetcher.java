package datawave.core.common.connection;

import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;

/**
 * Facade that centralizes Accumulo table metadata operations behind public APIs.
 * <p>
 * This class replaces direct usage of non-public Accumulo internals ({@code ClientContext}, {@code ThriftClientTypes}, {@code TabletLocator},
 * {@code MetadataServicer}, etc.) with their public API equivalents. All methods delegate to {@link org.apache.accumulo.core.client.admin.TableOperations} or
 * {@link org.apache.accumulo.core.client.admin.InstanceOperations}.
 *
 * @see <a href="https://github.com/NationalSecurityAgency/datawave/issues/2443">Issue #2443</a>
 */
public class AccumuloTableInfoFetcher {

    private final AccumuloClient client;

    public AccumuloTableInfoFetcher(AccumuloClient client) {
        this.client = client;
    }

    /**
     * Get the TableId for a table name using the public {@code tableIdMap()} API.
     *
     * @param tableName
     *            the table name
     * @return the TableId
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    public TableId getTableId(String tableName) throws TableNotFoundException {
        String id = client.tableOperations().tableIdMap().get(tableName);
        if (id == null) {
            throw new TableNotFoundException(null, tableName, "Table not found in tableIdMap");
        }
        return TableId.of(id);
    }

    /**
     * Check if a table exists using the public {@code exists()} API.
     *
     * @param tableName
     *            the table name
     * @return true if the table exists
     */
    public boolean tableExists(String tableName) {
        return client.tableOperations().exists(tableName);
    }

    /**
     * Check if a table is online using the public {@code isOnline()} API.
     *
     * @param tableName
     *            the table name
     * @return true if the table is online
     * @throws TableNotFoundException
     *             if the table does not exist
     * @throws AccumuloException
     *             if a general Accumulo error occurs
     */
    public boolean isTableOnline(String tableName) throws TableNotFoundException, AccumuloException {
        return client.tableOperations().isOnline(tableName);
    }

    /**
     * Get tablet locations for the given ranges using the public {@code locate()} API.
     *
     * @param tableName
     *            the table name
     * @param ranges
     *            the ranges to locate
     * @return the Locations result
     * @throws TableNotFoundException
     *             if the table does not exist
     * @throws AccumuloException
     *             if a general Accumulo error occurs
     * @throws AccumuloSecurityException
     *             if a security error occurs
     */
    public Locations getTabletLocations(String tableName, Collection<Range> ranges)
                    throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        return client.tableOperations().locate(tableName, ranges);
    }

    /**
     * Get the count of running major compactions across all tablet servers using the public {@code getActiveCompactions()} API.
     * <p>
     * Note: This counts only running compactions (not queued), which differs slightly from the original Thrift-based implementation that also counted queued
     * compactions. This is acceptable because the MAJC_THRESHOLD default is 3000 (a high safety margin) and this is polled on each bulk load cycle.
     *
     * @return the number of active major compactions
     * @throws AccumuloException
     *             if a general Accumulo error occurs
     * @throws AccumuloSecurityException
     *             if a security error occurs
     */
    public int getMajorCompactionCount() throws AccumuloException, AccumuloSecurityException {
        int count = 0;
        List<ActiveCompaction> compactions = client.instanceOperations().getActiveCompactions();
        for (ActiveCompaction compaction : compactions) {
            if (compaction.getType() == ActiveCompaction.CompactionType.MAJOR || compaction.getType() == ActiveCompaction.CompactionType.FULL) {
                count++;
            }
        }
        return count;
    }
}
