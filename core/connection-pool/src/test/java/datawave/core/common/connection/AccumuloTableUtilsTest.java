package datawave.core.common.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import datawave.accumulo.inmemory.InMemoryAccumulo;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;

public class AccumuloTableUtilsTest {

    private AccumuloClient client;

    @Before
    public void setup() throws Exception {
        InMemoryAccumulo instance = new InMemoryAccumulo();
        client = new InMemoryAccumuloClient("root", instance);
    }

    @Test
    public void testGetMajorCompactionCountWithNoCompactors() throws Exception {
        // InMemoryAccumulo has no compactors, so there are no running compactions to count.
        assertEquals(0, AccumuloTableUtils.getMajorCompactionCount(client));
    }

    @Test
    public void testGetTableIdForExistingTable() {
        // accumulo.metadata always exists
        TableId id = AccumuloTableUtils.getTableId(client, "accumulo.metadata");
        assertNotNull("Table ID should not be null for existing table", id);
    }

    @Test
    public void testGetTableIdForNonexistentTable() {
        TableId id = AccumuloTableUtils.getTableId(client, "nonexistent_table");
        assertNull("Table ID should be null for nonexistent table", id);
    }

    @Test
    public void testGetTableIdFromTableOperations() {
        TableId id = AccumuloTableUtils.getTableId(client.tableOperations(), "accumulo.metadata");
        assertEquals(AccumuloTableUtils.getTableId(client, "accumulo.metadata"), id);
    }

    @Test
    public void testLocateTabletsForExistingTable() throws Exception {
        // Create a test table
        client.tableOperations().create("testTable");
        List<Range> ranges = Collections.singletonList(new Range());
        Map<String,Map<KeyExtent,List<Range>>> result = AccumuloTableUtils.locateTablets(client, "testTable", ranges);
        assertNotNull("Result should not be null", result);
        // With a new table there should be exactly one tablet (the default tablet)
        int totalExtents = result.values().stream().mapToInt(Map::size).sum();
        assertEquals("New table should have exactly one tablet", 1, totalExtents);
    }

    @Test
    public void testGetSplitsWithLocationsForExistingTable() throws Exception {
        client.tableOperations().create("testSplitsTable");
        Map<Text,String> result = AccumuloTableUtils.getSplitsWithLocations(client, "testSplitsTable");
        assertNotNull("Result should not be null", result);
        // A new table with no splits has one tablet with no end-row, so result should be empty
        assertTrue("New table with no splits should return empty map", result.isEmpty());
    }
}
