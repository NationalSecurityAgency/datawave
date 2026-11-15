package datawave.query.tables;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import datawave.next.scanner.DocumentScannerConfig;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.planner.DefaultQueryPlanner;

/**
 * Tests the copying and scheduler selection of a {@link CountingShardQueryLogic}.
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

    @Test
    public void testCountSchedulerIsUsedWhenConfigured() {
        assertTrue(useCountScheduler(false));
    }

    /**
     * A full table scan is the one query the field index cannot bound, so the count scheduler must not take it. Without this the scan fails rather than
     * counting, since a count of zero for an unbounded query would be a silently wrong answer.
     */
    @Test
    public void testFullTableScanDoesNotUseTheCountScheduler() {
        assertFalse(useCountScheduler(true));
    }

    private boolean useCountScheduler(boolean fullTableScanEnabled) {
        CountingShardQueryLogic logic = new CountingShardQueryLogic();
        logic.setQueryPlanner(new DefaultQueryPlanner());
        logic.setFullTableScanEnabled(fullTableScanEnabled);

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setDocumentScannerConfig(new DocumentScannerConfig());
        config.setUseDocumentScheduler(true);

        return logic.useCountScheduler(config);
    }
}
