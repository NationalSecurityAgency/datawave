package datawave.table.key.parser;

import org.junit.jupiter.api.Test;

/**
 * Interface that ensures each index key conversion iterators can handle every shard index key format
 */
public interface IndexKeyConversionTests {

    @Test
    void testStandardShardIndexKey();

    @Test
    void testTruncatedShardIndexKey();

    @Test
    void testShardedDayIndexKey();

    @Test
    void testShardedYearIndexKey();
}
