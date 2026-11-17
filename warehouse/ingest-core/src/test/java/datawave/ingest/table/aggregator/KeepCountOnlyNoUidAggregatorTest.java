package datawave.ingest.table.aggregator;

import static java.util.Arrays.asList;

import static datawave.ingest.table.aggregator.UidTestUtils.countOnlyList;
import static datawave.ingest.table.aggregator.UidTestUtils.removeUidList;
import static datawave.ingest.table.aggregator.UidTestUtils.valueToUidList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.ingest.protobuf.Uid;

public class KeepCountOnlyNoUidAggregatorTest {

    private static final Key KEY = new Key("key");

    private final KeepCountOnlyNoUidAggregator agg = new KeepCountOnlyNoUidAggregator();

    @BeforeEach
    public void setup() {
        agg.reset();
    }

    @Test
    public void testKeepKeyWhenCountReachesZero() {
        List<Value> values = asList(countOnlyList(2), removeUidList("uid1", "uid2"));

        Uid.List result = valueToUidList(agg.reduce(KEY, values.iterator()));

        assertEquals(0, result.getCOUNT());
        assertTrue(result.getIGNORE());
        assertTrue(agg.propogateKey());
    }

    @Test
    public void testKeepKeyWhenCountGoesNegative() {
        List<Value> values = asList(countOnlyList(1), removeUidList("uid1", "uid2"));
        Uid.List result = valueToUidList(agg.reduce(KEY, values.iterator()));

        assertEquals(-1, result.getCOUNT());
        assertTrue(result.getIGNORE());
        assertTrue(agg.propogateKey());
    }

    @Test
    public void testKeepKeyWhenCountIsPositive() {
        List<Value> values = asList(countOnlyList(3), countOnlyList(1));
        Uid.List result = valueToUidList(agg.reduce(KEY, values.iterator()));

        assertEquals(4, result.getCOUNT());
        assertTrue(result.getIGNORE());
        assertTrue(agg.propogateKey());
    }
}
