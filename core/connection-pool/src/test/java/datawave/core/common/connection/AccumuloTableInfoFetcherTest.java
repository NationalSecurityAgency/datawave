package datawave.core.common.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.junit.Before;
import org.junit.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;

public class AccumuloTableInfoFetcherTest {

    private static final String TEST_TABLE = "testTable";
    private AccumuloClient client;
    private AccumuloTableInfoFetcher fetcher;

    @Before
    public void setup() throws Exception {
        InMemoryInstance instance = new InMemoryInstance();
        client = new InMemoryAccumuloClient("root", instance);
        client.tableOperations().create(TEST_TABLE);
        fetcher = new AccumuloTableInfoFetcher(client);
    }

    @Test
    public void testGetTableId() throws Exception {
        TableId tableId = fetcher.getTableId(TEST_TABLE);
        assertNotNull(tableId);
        // Verify it matches the tableIdMap entry
        String expectedId = client.tableOperations().tableIdMap().get(TEST_TABLE);
        assertEquals(expectedId, tableId.canonical());
    }

    @Test(expected = TableNotFoundException.class)
    public void testGetTableIdNonExistent() throws Exception {
        fetcher.getTableId("nonExistentTable");
    }

    @Test
    public void testTableExists() {
        assertTrue(fetcher.tableExists(TEST_TABLE));
        assertFalse(fetcher.tableExists("nonExistentTable"));
    }

    @Test
    public void testIsTableOnline() throws Exception {
        // InMemoryAccumuloClient.isOnline() returns false, but the API call should succeed
        boolean online = fetcher.isTableOnline(TEST_TABLE);
        // InMemory always returns false; just verify no exception is thrown
        assertFalse(online);
    }

    @Test
    public void testGetTabletLocations() throws Exception {
        Locations locations = fetcher.getTabletLocations(TEST_TABLE, Collections.singletonList(new Range()));
        assertNotNull(locations);
        Map<TabletId,List<Range>> byTablet = locations.groupByTablet();
        assertNotNull(byTablet);
        assertFalse(byTablet.isEmpty());
    }

    @Test
    public void testGetMajorCompactionCount() throws Exception {
        // InMemoryAccumuloClient returns empty list for getActiveCompactions
        int count = fetcher.getMajorCompactionCount();
        assertEquals(0, count);
    }
}
