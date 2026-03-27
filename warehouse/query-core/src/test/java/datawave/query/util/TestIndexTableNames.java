package datawave.query.util;

import java.util.List;

import datawave.util.TableName;

/**
 * This utility allows unit tests to iterate through all possible global index table formats
 * <p>
 * A standard shard index contains uids
 * <p>
 * A 'no uid' index does not contain uids
 * <p>
 * A truncated index collapses all keys for a day and tracks shard offsets in a bitset
 */
public class TestIndexTableNames {

    public static final String SHARD_INDEX = TableName.SHARD_INDEX;
    public static final String NO_UID_INDEX = "noUidIndex";
    public static final String TRUNCATED_INDEX = TableName.TRUNCATED_SHARD_INDEX;

    private TestIndexTableNames() {
        // enforce static access
    }

    public static List<String> names() {
        return List.of(SHARD_INDEX, NO_UID_INDEX, TRUNCATED_INDEX);
    }
}
