package datawave.query.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.junit.jupiter.api.Test;

public class ExecutionHintHelperTest {

    private final Map<String,String> tableBasedHints = createTableBasedHints();
    private final Map<String,String> stageBasedHints = createStageBasedHints();
    private final Map<String,String> defaultHints = createDefaultHints();
    private final Map<String,Map<String,String>> executionHints = createExecutionHints();

    private final Map<String,ConsistencyLevel> consistencyLevels = createConsistencyLevels();

    private Map<String,String> createTableBasedHints() {
        Map<String,String> hints = new HashMap<>();
        hints.put(ExecutionHintHelper.SCAN_TYPE, "executor-pool-a");
        hints.put(ExecutionHintHelper.PRIORITY, "1");
        return hints;
    }

    private Map<String,String> createStageBasedHints() {
        Map<String,String> hints = new HashMap<>();
        hints.put(ExecutionHintHelper.SCAN_TYPE, "executor-pool-b");
        hints.put(ExecutionHintHelper.PRIORITY, "2");
        return hints;
    }

    private Map<String,String> createDefaultHints() {
        Map<String,String> hints = new HashMap<>();
        hints.put(ExecutionHintHelper.SCAN_TYPE, "executor-pool-c");
        hints.put(ExecutionHintHelper.PRIORITY, "3");
        return hints;
    }

    private Map<String,Map<String,String>> createExecutionHints() {
        Map<String,Map<String,String>> hints = new HashMap<>();
        hints.put("tableBased", tableBasedHints);
        hints.put("stageBased", stageBasedHints);
        hints.put("default", defaultHints);
        return hints;
    }

    private Map<String,ConsistencyLevel> createConsistencyLevels() {
        Map<String,ConsistencyLevel> levels = new HashMap<>();
        levels.put("tableBased", ConsistencyLevel.EVENTUAL);
        levels.put("stageBased", ConsistencyLevel.IMMEDIATE);
        levels.put("default", ConsistencyLevel.IMMEDIATE);
        return levels;
    }

    @Test
    public void testGetExecutionHintsSingleKeyNullMap() {
        Map<String,String> hints = ExecutionHintHelper.getExecutionHints("tableBased", null);
        assertNull(hints);
    }

    @Test
    public void testGetExecutionHintsSingleKeyNullKey() {
        Map<String,String> hints = ExecutionHintHelper.getExecutionHints(null, executionHints);
        assertNull(hints);
    }

    @Test
    public void testGetExecutionHintsSingleKeyNoMatch() {
        Map<String,String> hints = ExecutionHintHelper.getExecutionHints("dateBased", executionHints);
        assertNull(hints);
    }

    @Test
    public void testGetExecutionHintsSingleKeyGoodMatch() {
        Map<String,String> hints = ExecutionHintHelper.getExecutionHints("tableBased", executionHints);
        assertNotNull(hints);
        assertScanType("executor-pool-a", hints);
        assertScanPriority("1", hints);
    }

    @Test
    public void testGetExecutionHintsPrimaryKeySecondaryKeyNullMap() {
        Map<String,String> hints = ExecutionHintHelper.getExecutionHints("tableBased", "default", null);
        assertNull(hints);
    }

    @Test
    public void testGetExecutionHintsPrimaryNullKeySecondaryNullKey() {
        Map<String,String> hints = ExecutionHintHelper.getExecutionHints(null, null, executionHints);
        assertNull(hints);
    }

    @Test
    public void testGetExecutionHintsPrimaryNullKeySecondaryKeyMatches() {
        Map<String,String> hints = ExecutionHintHelper.getExecutionHints(null, "default", executionHints);
        assertEquals(defaultHints, hints);
    }

    @Test
    public void testGetExecutionHintsPrimaryKeyNotFoundSecondaryKeyMatches() {
        Map<String,String> hints = ExecutionHintHelper.getExecutionHints("timeBased", "stageBased", executionHints);
        assertEquals(stageBasedHints, hints);
    }

    @Test
    public void testGetExecutionHintsPrimaryKeyMatchesSecondaryKeyNotNeeded() {
        Map<String,String> hints = ExecutionHintHelper.getExecutionHints("stageBased", "default", executionHints);
        assertEquals(stageBasedHints, hints);
    }

    @Test
    public void testGetScanTypeNullMap() {
        String scanType = ExecutionHintHelper.getScanType(null);
        assertNull(scanType);
    }

    @Test
    public void testGetScanTypeNoMatch() {
        Map<String,String> hints = Map.of("priority", "7");
        String scanType = ExecutionHintHelper.getScanType(hints);
        assertNull(scanType);
    }

    @Test
    public void testGetScanTypeMatches() {
        Map<String,String> hints = Map.of("scan_type", "executor-pool-a");
        String scanType = ExecutionHintHelper.getScanType(hints);
        assertEquals("executor-pool-a", scanType);
    }

    @Test
    public void testGetPriorityNullMap() {
        int priority = ExecutionHintHelper.getPriority(null);
        assertEquals(Integer.MAX_VALUE, priority);
    }

    @Test
    public void testGetPriorityNoMatch() {
        Map<String,String> hints = Map.of("scan_type", "executor-pool-c");
        int priority = ExecutionHintHelper.getPriority(hints);
        assertEquals(Integer.MAX_VALUE, priority);
    }

    @Test
    public void testGetPriorityMatches() {
        Map<String,String> hints = Map.of("priority", "2");
        int priority = ExecutionHintHelper.getPriority(hints);
        assertEquals(2, priority);
    }

    @Test
    public void testGetConsistencyLevelNullMap() {
        ConsistencyLevel level = ExecutionHintHelper.getConsistencyLevel("tableBased", null);
        assertNull(level);
    }

    @Test
    public void testGetConsistencyLevelNullKey() {
        ConsistencyLevel level = ExecutionHintHelper.getConsistencyLevel(null, consistencyLevels);
        assertNull(level);
    }

    @Test
    public void testGetConsistencyLevelKeyNotFound() {
        ConsistencyLevel level = ExecutionHintHelper.getConsistencyLevel("timeBased", consistencyLevels);
        assertNull(level);
    }

    @Test
    public void testGetConsistencyLevel() {
        assertEquals(ConsistencyLevel.EVENTUAL, ExecutionHintHelper.getConsistencyLevel("tableBased", consistencyLevels));
        assertEquals(ConsistencyLevel.IMMEDIATE, ExecutionHintHelper.getConsistencyLevel("stageBased", consistencyLevels));
        assertEquals(ConsistencyLevel.IMMEDIATE, ExecutionHintHelper.getConsistencyLevel("default", consistencyLevels));
    }

    private void assertScanType(String expected, Map<String,String> hints) {
        assertTrue(hints.containsKey(ExecutionHintHelper.SCAN_TYPE));
        assertEquals(expected, hints.get(ExecutionHintHelper.SCAN_TYPE));
    }

    private void assertScanPriority(String expected, Map<String,String> hints) {
        assertTrue(hints.containsKey(ExecutionHintHelper.PRIORITY));
        assertEquals(expected, hints.get(ExecutionHintHelper.PRIORITY));
    }
}
