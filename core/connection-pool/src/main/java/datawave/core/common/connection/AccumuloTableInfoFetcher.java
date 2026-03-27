package datawave.core.common.connection;

import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
// @formatter:off
// ============================================================================================
// NON-PUBLIC ACCUMULO API FACADE (Issue #2443)
// ============================================================================================
// The following imports use non-public Accumulo APIs. They are intentionally isolated in this
// facade class so that callers throughout Datawave never reference non-public APIs directly.
//
// WHY THIS EXISTS:
//   There is no public Accumulo API for queued compaction counts (only running compactions
//   via getActiveCompactions()). The Thrift RPC below is the only way to get queued + running
//   counts. See: https://github.com/apache/accumulo/issues/5965
//
// HOW TO SWAP OUT INCREMENTALLY:
//   Each method in this class that uses non-public APIs is marked with a
//   "NON-PUBLIC API" comment block. When a public API alternative becomes available
//   (e.g., in Accumulo 4.0):
//     1. Replace the method body with the public API equivalent
//     2. Remove the non-public imports that are no longer used
//     3. Update the import-control-accumulo.xml if the non-public class was allowed there
//
// HOW TO KNOW WHEN THIS FACADE CAN BE REMOVED:
//   When ALL methods in this class use only public APIs (no "NON-PUBLIC API" markers remain):
//     1. Remove these non-public imports
//     2. Remove this comment block
//     3. Inline the method(s) back into callers — the facade is no longer load-bearing
//     4. Verify by running: grep -r 'NON-PUBLIC API' AccumuloTableInfoFetcher.java
//        — if no results, the migration is complete
// ============================================================================================
// @formatter:on
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;

/**
 * Facade that isolates non-public Accumulo API usage behind a clean interface.
 * <p>
 * This class exists solely to contain Accumulo internals that have no public API equivalent. Operations that can use public APIs (e.g., table existence, online
 * status, tablet locations) should call {@link org.apache.accumulo.core.client.admin.TableOperations} or
 * {@link org.apache.accumulo.core.client.admin.InstanceOperations} directly — they do not belong here.
 * <p>
 * See the import block above for the incremental migration plan.
 *
 * @see <a href="https://github.com/NationalSecurityAgency/datawave/issues/2443">Issue #2443</a>
 */
public class AccumuloTableInfoFetcher {

    private final AccumuloClient client;

    public AccumuloTableInfoFetcher(AccumuloClient client) {
        this.client = client;
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
     * @return the number of queued and running major compactions
     * @throws AccumuloException
     *             if a general Accumulo error occurs
     * @throws AccumuloSecurityException
     *             if a security error occurs
     */
    public int getMajorCompactionCount() throws AccumuloException, AccumuloSecurityException {
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
