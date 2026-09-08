package datawave.test.framework.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ShardKeyUtilTest {

    @Test
    void testRowPairsTheShardDateWithTheShard() {
        assertEquals(ShardKeyUtil.DATE + "_1", ShardKeyUtil.buildRow(1, 10));
        assertEquals(ShardKeyUtil.DATE + "_9", ShardKeyUtil.buildRow(9, 10));
    }

    /**
     * The shard is the event id modulo the shard count, which is what spreads events deterministically and lets a test predict which row an event lands in.
     */
    @Test
    void testShardIsTheEventIdModuloTheShardCount() {
        assertEquals(ShardKeyUtil.DATE + "_0", ShardKeyUtil.buildRow(10, 10));
        assertEquals(ShardKeyUtil.DATE + "_1", ShardKeyUtil.buildRow(11, 10));
        assertEquals(ShardKeyUtil.DATE + "_3", ShardKeyUtil.buildRow(23, 10));
    }

    /**
     * Events separated by a multiple of the shard count collide in the same row - the property the shard index aggregator exists to handle.
     */
    @Test
    void testEventsAMultipleOfTheShardCountApartShareARow() {
        assertEquals(ShardKeyUtil.buildRow(1, 10), ShardKeyUtil.buildRow(21, 10));
    }

    @Test
    void testASingleShardPlacesEveryEventInOneRow() {
        assertEquals(ShardKeyUtil.DATE + "_0", ShardKeyUtil.buildRow(1, 1));
        assertEquals(ShardKeyUtil.DATE + "_0", ShardKeyUtil.buildRow(999, 1));
    }

    /**
     * A shard count of zero would divide by zero and a negative one would produce a negative shard, so both are rejected rather than producing an unusable row.
     */
    @Test
    void testNonPositiveShardCountIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> ShardKeyUtil.buildRow(1, 0));
        assertThrows(IllegalArgumentException.class, () -> ShardKeyUtil.buildRow(1, -1));
    }

    /**
     * Java's {@code %} keeps the sign of the dividend, so a negative event id would yield a negative shard and a row no ingest path writes to. An event id is a
     * counter, so a negative one is a caller bug worth failing on rather than quietly mapping into a valid-looking row.
     */
    @Test
    void testNegativeEventIdIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> ShardKeyUtil.buildRow(-1, 10));
        assertThrows(IllegalArgumentException.class, () -> ShardKeyUtil.buildRow(Integer.MIN_VALUE, 10));
    }

    @Test
    void testEventIdOfZeroIsAccepted() {
        assertEquals(ShardKeyUtil.DATE + "_0", ShardKeyUtil.buildRow(0, 10));
    }
}
