package datawave.query.tables;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Tests the copying of a {@link CountingShardQueryLogic}.
 */
public class CountingShardQueryLogicTest {

    /**
     * The page wait time is configured on the prototype logic, so a copy that does not carry it over waits zero milliseconds for the count and returns an
     * intermediate result every time.
     */
    @Test
    public void testCopyRetainsPageWaitTime() {
        CountingShardQueryLogic logic = new CountingShardQueryLogic();
        logic.setPageWaitTimeMillis(3_600_000L);

        assertEquals(3_600_000L, logic.clone().getPageWaitTimeMillis());
    }
}
