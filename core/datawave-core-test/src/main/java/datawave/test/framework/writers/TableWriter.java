package datawave.test.framework.writers;

import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;

import datawave.test.framework.FieldMetadata;

/**
 * A utility that converts a list of {@link FieldMetadata} to entries in an accumulo table
 */
public class TableWriter {

    private TableWriter() {
        // enforce static access
    }

    /**
     * Write entries for field metadata to all configured tables
     *
     * @param client
     *            the accumulo client
     * @param metadata
     *            the list of field metadata
     * @param numShards
     *            the number of shards per day to distribute events across
     * @param numDays
     *            the number of days to distribute events across
     */
    public static void write(AccumuloClient client, List<FieldMetadata> metadata, int numShards, int numDays) {
        MetadataTableWriter.write(client, MetadataTableConverter.convert(metadata));
        ShardIndexTableWriter.write(client, metadata, numShards, numDays);
        ShardTableWriter.write(client, metadata, numShards, numDays);
    }
}
