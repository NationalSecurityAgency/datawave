package datawave.query.index.day;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

import datawave.query.util.DayIndexIngest;
import datawave.query.util.NoUidIndexIngest;
import datawave.query.util.TestIndexTableNames;
import datawave.query.util.YearIndexIngest;
import datawave.util.TableName;

/**
 * A wrapper that allows multiple types of index tables to be written given a source index table in standard format
 */
public class IndexIngestUtil {

    private String noUidIndexTableName = TestIndexTableNames.NO_UID_INDEX;
    private String truncatedIndexTableName = TableName.TRUNCATED_SHARD_INDEX;
    private String shardedDayIndexTableName = TableName.SHARD_DAY_INDEX;
    private String shardedYearIndexTableName = TableName.SHARD_YEAR_INDEX;

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
