package datawave.table.constants;

import org.apache.accumulo.core.data.TableId;

/**
 * Constants for Accumulo system table names and IDs. Replaces the non-public {@code org.apache.accumulo.core.metadata.SystemTables} enum.
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
}
