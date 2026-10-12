package datawave.table.constants;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TableNameTest {

    @Test
    public void testVerifyConstants() {
        verifyConstant(TableName.DATE_INDEX, "dateIndex");
        verifyConstant(TableName.EDGE, "edge");
        verifyConstant(TableName.ERROR_SHARD, "errorShard");
        verifyConstant(TableName.INDEX_STATS, "shardIndexStats");
        verifyConstant(TableName.LOAD_DATES, "LoadDates");
        verifyConstant(TableName.METADATA, "DatawaveMetadata");
        verifyConstant(TableName.SHARD, "shard");
        verifyConstant(TableName.SHARD_INDEX, "shardIndex");
        verifyConstant(TableName.SHARD_RINDEX, "shardReverseIndex");
        verifyConstant(TableName.TRUNCATED_SHARD_INDEX, "truncatedShardIndex");
        verifyConstant(TableName.SHARD_YEAR_INDEX, "shardYearIndex");
        verifyConstant(TableName.SHARD_DAY_INDEX, "shardDayIndex");
    }

    /**
     * Verify the constant did not change
     *
     * @param constant
     *            the constant
     * @param expected
     *            the expected value
     */
    private void verifyConstant(String constant, String expected) {
        assertEquals(expected, constant, "Constant changed unexpectedly");
    }
}
