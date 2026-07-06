package datawave.test.framework.writers;

import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;

import datawave.test.framework.FieldMetadata;
import datawave.test.framework.util.ShardKeyUtil;
import datawave.util.time.DateHelper;

/**
 * A utility that converts a list of {@link FieldMetadata} to entries in an accumulo table
 */
public class TableWriter {

    public static final Long TIMESTAMP = DateHelper.parse(ShardKeyUtil.DATE).getTime();

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
     *            the number of shards to distribute events across
     */
    public static void write(AccumuloClient client, List<FieldMetadata> metadata, int numShards) {
        MetadataTableWriter.write(client, MetadataTableConverter.convert(metadata));
        ShardIndexTableWriter.write(client, metadata, numShards);
        ShardTableWriter.write(client, metadata, numShards);
    }
}
