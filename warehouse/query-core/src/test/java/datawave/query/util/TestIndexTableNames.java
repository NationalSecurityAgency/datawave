package datawave.query.util;

import java.util.List;

import datawave.util.TableName;

public class TestIndexTableNames {

    public static final String SHARD_INDEX = TableName.SHARD_INDEX;
    public static final String NO_UID_INDEX = "noUidIndex";
    public static final String TRUNCATED_INDEX = TableName.TRUNCATED_SHARD_INDEX;
    public static final String SHARDED_DAY_INDEX = TableName.SHARD_DAY_INDEX;
    public static final String SHARDED_YEAR_INDEX = TableName.SHARD_YEAR_INDEX;

    private TestIndexTableNames() {
        // enforce static access
    }

    public static List<String> names() {
        return List.of(SHARD_INDEX, NO_UID_INDEX, TRUNCATED_INDEX, SHARDED_DAY_INDEX, SHARDED_YEAR_INDEX);
    }
}
