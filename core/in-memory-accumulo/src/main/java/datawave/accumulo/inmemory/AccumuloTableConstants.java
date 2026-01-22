package datawave.accumulo.inmemory;

import org.apache.accumulo.core.data.TableId;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Constants for Accumulo system table names and IDs. Replaces non-public classes:
 * <ul>
 * <li>org.apache.accumulo.core.metadata.MetadataTable</li>
 * <li>org.apache.accumulo.core.metadata.RootTable</li>
 * <li>org.apache.accumulo.core.replication.ReplicationTable</li>
 * <li>org.apache.accumulo.core.util.tables.TableNameUtil</li>
 * </ul>
 */
public final class AccumuloTableConstants {

    private AccumuloTableConstants() {
        throw new UnsupportedOperationException("Utility class");
    }

    // Metadata table constants
    public static final String METADATA_TABLE_NAME = "accumulo.metadata";
    public static final TableId METADATA_TABLE_ID = TableId.of("!0");

    // Root table constants
    public static final String ROOT_TABLE_NAME = "accumulo.root";
    public static final TableId ROOT_TABLE_ID = TableId.of("+r");

    // Replication table constants
    public static final String REPLICATION_TABLE_NAME = "accumulo.replication";

    /**
     * Qualify a table name by splitting it into namespace and table name parts. Replaces TableNameUtil.qualify().
     *
     * @param tableName
     *            the fully qualified table name (e.g., "namespace.table" or just "table")
     * @return a Pair of (namespace, tableName), where namespace is empty string if not specified
     */
    public static Pair<String,String> qualifyTableName(String tableName) {
        int dotIndex = tableName.lastIndexOf('.');
        if (dotIndex == -1) {
            return Pair.of("", tableName);
        }
        return Pair.of(tableName.substring(0, dotIndex), tableName.substring(dotIndex + 1));
    }
}
