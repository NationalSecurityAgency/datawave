package datawave.accumulo.inmemory;

import org.apache.accumulo.core.data.TableId;

/**
 * Constants for Accumulo system table names and IDs. Replaces non-public classes:
 * <ul>
 * <li>org.apache.accumulo.core.metadata.MetadataTable</li>
 * <li>org.apache.accumulo.core.metadata.RootTable</li>
 * <li>org.apache.accumulo.core.replication.ReplicationTable</li>
 * </ul>
 *
 * Note: TableNameUtil.qualify() replacement moved to AccumuloValidators per review feedback, so it can leverage name validation.
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
}
