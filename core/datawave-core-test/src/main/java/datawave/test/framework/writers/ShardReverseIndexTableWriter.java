package datawave.test.framework.writers;

import static datawave.test.framework.util.MetadataColumn.RI;

import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;

import datawave.table.constants.TableName;
import datawave.test.framework.FieldMetadata;

/**
 * Writes reverse-indexed fields using reversed normalized values and the same UID aggregation as the forward index.
 */
public class ShardReverseIndexTableWriter {

    private ShardReverseIndexTableWriter() {
        // enforce static access
    }

    /**
     * Write reverse index entries for fields carrying the reverse index metadata column.
     *
     * @param client
     *            the accumulo client
     * @param fields
     *            the field metadata
     * @param numShards
     *            the number of shards to distribute events across
     */
    public static void write(AccumuloClient client, List<FieldMetadata> fields, int numShards) {
        ShardIndexTableWriter.writeIndex(client, fields, numShards, TableName.SHARD_RINDEX, RI, true);
    }
}
