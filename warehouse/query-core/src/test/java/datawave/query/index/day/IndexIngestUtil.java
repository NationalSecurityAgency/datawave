package datawave.query.index.day;

import static org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;

import datawave.core.iterators.compress.event.EventSerializationIterator;
import datawave.query.util.DayIndexIngest;
import datawave.query.util.NoUidIndexIngest;
import datawave.query.util.TestIndexTableNames;
import datawave.query.util.YearIndexIngest;
import datawave.test.MacTestUtil;
import datawave.util.TableName;

/**
 * A wrapper that allows multiple types of index tables to be written given a source index table in standard format
 */
public class IndexIngestUtil {

    private String noUidIndexTableName = TestIndexTableNames.NO_UID_INDEX;
    private String truncatedIndexTableName = TableName.TRUNCATED_SHARD_INDEX;
    private String shardedDayIndexTableName = TableName.SHARD_DAY_INDEX;
    private String shardedYearIndexTableName = TableName.SHARD_YEAR_INDEX;

    // flag that determines if the event column is compressed
    private final boolean compressEvents = true;
    // flag that determines if the field index is compressed
    private final boolean compressFieldIndex = false;

    public IndexIngestUtil() {
        // no-op
    }

    public void write(AccumuloClient client, Authorizations auths) {
        write(client, auths, TableName.SHARD_INDEX);
    }

    public void write(AccumuloClient client, Authorizations auths, String source) {
        NoUidIndexIngest noUidIndex = new NoUidIndexIngest();
        TruncatedIndexIngest truncatedIndex = new TruncatedIndexIngest();
        DayIndexIngest shardedDayIndex = new DayIndexIngest();
        YearIndexIngest shardedYearIndex = new YearIndexIngest();

        noUidIndex.convert(client, auths, source, noUidIndexTableName);
        truncatedIndex.convert(client, auths, source, truncatedIndexTableName);
        shardedDayIndex.convert(client, auths, source, shardedDayIndexTableName);
        shardedYearIndex.convert(client, auths, source, shardedYearIndexTableName);

        if (compressEvents) {
            compressEventColumn(client);
        }

        if (compressFieldIndex) {
            compressFieldIndex(client);
        }
    }

    /**
     * Configure the {@link EventSerializationIterator}. The net effect is document keys are serialized into the accumulo Value.
     *
     * @param client
     *            the AccumuloClient
     */
    protected void compressEventColumn(AccumuloClient client) {
        // every scope gets the serialization iterator
        TableOperations tops = client.tableOperations();
        Map<String,String> properties = new HashMap<>();
        for (IteratorScope scope : IteratorScope.values()) {
            String compressIter = "table.iterator." + scope.name() + ".serialize";
            String compressOpt = "18,datawave.core.iterators.compress.event.EventSerializationIterator";
            properties.put(compressIter, compressOpt);

            String version = "table.iterator." + scope.name() + ".serialize.opt.version";
            properties.put(version, "2");

            String compressionThreshold = "table.iterator." + scope.name() + ".serialize.opt.threshold";
            properties.put(compressionThreshold, "512");

            String compressionAlgorithm = "table.iterator." + scope.name() + ".serialize.opt.algorithm";
            properties.put(compressionAlgorithm, "gzip");
        }

        // only scan gets the decompression iterator. minc and majc scopes should produce compressed keys
        IteratorScope scope = IteratorScope.scan;
        String decompressIter = "table.iterator." + scope.name() + ".deserialize";
        String decompressOpt = "19,datawave.core.iterators.compress.event.EventDeserializationIterator";
        properties.put(decompressIter, decompressOpt);

        MacTestUtil.addPropertiesAndWait(tops, TableName.SHARD, properties);
    }

    protected void compressFieldIndex(AccumuloClient client) {
        try {
            // every scope gets the serialization iterator
            TableOperations tops = client.tableOperations();
            for (IteratorScope scope : IteratorScope.values()) {
                String compressIter = "table.iterator." + scope.name() + ".compressfi";
                String compressOpt = "16,datawave.core.iterators.compress.fi.FieldIndexSerializationIterator";
                tops.setProperty(TableName.SHARD, compressIter, compressOpt);

                String version = "table.iterator." + scope.name() + ".compressfi.opt.version";
                tops.setProperty(TableName.SHARD, version, "1");
            }

            // only scan gets the decompression iterator. minc and majc scopes should produce compressed keys
            IteratorScope scope = IteratorScope.scan;
            String decompressIter = "table.iterator." + scope.name() + ".decompressfi";
            String decompressOpt = "17,datawave.core.iterators.compress.fi.FieldIndexDeserializationIterator";
            tops.setProperty(TableName.SHARD, decompressIter, decompressOpt);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public void setNoUidIndexTableName(String noUidIndexTableName) {
        this.noUidIndexTableName = noUidIndexTableName;
    }

    public void setTruncatedIndexTableName(String truncatedIndexTableName) {
        this.truncatedIndexTableName = truncatedIndexTableName;
    }

    public void setShardedDayIndexTableName(String shardedDayIndexTableName) {
        this.shardedDayIndexTableName = shardedDayIndexTableName;
    }

    public void setShardedYearIndexTableName(String shardedYearIndexTableName) {
        this.shardedYearIndexTableName = shardedYearIndexTableName;
    }
}
