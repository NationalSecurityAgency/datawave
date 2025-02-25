package datawave.security.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.security.iterator.ConfigurableVisibilityFilter;
import datawave.webservice.common.connection.WrappedAccumuloClient;

public class ScannerHelperTest {
    
    public static String TABLE_NAME = "DATA";
    private AccumuloClient mockConnector;
    
    @BeforeEach
    public void setUp() throws Exception {
        InMemoryInstance instance = new InMemoryInstance();
        mockConnector = new InMemoryAccumuloClient("root", instance);
        mockConnector.securityOperations().changeUserAuthorizations("root", new Authorizations("A", "B", "C", "D", "E", "F", "G", "H", "I"));
        mockConnector.tableOperations().create(TABLE_NAME);
        
        Mutation m = new Mutation("row");
        m.put("cf1", "cq1", new ColumnVisibility("A&B&C"), 1L, new Value(new byte[0]));
        m.put("cf1", "cq2", new ColumnVisibility("A&D&E"), 1L, new Value(new byte[0]));
        m.put("cf1", "cq3", new ColumnVisibility("A&F&G"), 1L, new Value(new byte[0]));
        m.put("cf2", "cq1", new ColumnVisibility("A"), 1L, new Value(new byte[0]));
        m.put("cf2", "cq2", new ColumnVisibility("B"), 1L, new Value(new byte[0]));
        m.put("cf2", "cq3", new ColumnVisibility("C"), 1L, new Value(new byte[0]));
        m.put("cf2", "cq4", new ColumnVisibility("D"), 1L, new Value(new byte[0]));
        m.put("cf2", "cq5", new ColumnVisibility("E"), 1L, new Value(new byte[0]));
        m.put("cf2", "cq6", new ColumnVisibility("F"), 1L, new Value(new byte[0]));
        m.put("cf2", "cq7", new ColumnVisibility("G"), 1L, new Value(new byte[0]));
        m.put("cf2", "cq8", new ColumnVisibility("H"), 1L, new Value(new byte[0]));
        m.put("cf2", "cq9", new ColumnVisibility("I"), 1L, new Value(new byte[0]));
        BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1024L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        BatchWriter bw = mockConnector.createBatchWriter(TABLE_NAME, bwConfig);
        bw.addMutation(m);
        bw.flush();
        bw.close();
    }
    
    @Test
    public void testVisibilityFiltersAdded() throws Exception {
        
        Authorizations a1 = new Authorizations("A", "B", "C");
        Authorizations a2 = new Authorizations("A", "D", "E");
        Authorizations a3 = new Authorizations("A", "F", "G");
        
        List<Key> expectedKeys = Lists.newArrayList(new Key("row", "cf2", "cq1", "A", 1L));
        Value expectedVal = new Value(new byte[0]);
        
        Scanner scanner = ScannerHelper.createScanner(new WrappedAccumuloClient(mockConnector, mockConnector), TABLE_NAME, Arrays.asList(a1, a2, a3));
        for (Entry<Key,Value> entry : scanner) {
            assertFalse(expectedKeys.isEmpty(), "Ran out of expected keys but got: " + entry.getKey());
            Key expectedKey = expectedKeys.remove(0);
            assertEquals(expectedKey, entry.getKey());
            assertEquals(expectedVal, entry.getValue());
        }
        assertTrue(expectedKeys.isEmpty(), "Scanner did not return all expected keys: " + expectedKeys);
    }
    
    @Test
    public void testVisibilityFilterClearImmutability() throws Exception {
        
        Authorizations a1 = new Authorizations("A", "B", "C");
        Authorizations a2 = new Authorizations("A", "D", "E");
        Authorizations a3 = new Authorizations("A", "F", "G");
        
        List<Key> expectedKeys = Lists.newArrayList(new Key("row", "cf2", "cq1", "A", 1L));
        Value expectedVal = new Value(new byte[0]);
        
        Scanner scanner = ScannerHelper.createScanner(new WrappedAccumuloClient(mockConnector, mockConnector), TABLE_NAME, Arrays.asList(a1, a2, a3));
        // Clearing the scan iterators should do nothing to the iterators added by ScannerHelper.createScanner
        scanner.clearScanIterators();
        for (Entry<Key,Value> entry : scanner) {
            assertFalse(expectedKeys.isEmpty(), "Ran out of expected keys but got: " + entry.getKey());
            Key expectedKey = expectedKeys.remove(0);
            assertEquals(expectedKey, entry.getKey());
            assertEquals(expectedVal, entry.getValue());
        }
        assertTrue(expectedKeys.isEmpty(), "Scanner did not return all expected keys: " + expectedKeys);
    }
    
    @Test
    public void testVisibilityFilterRemoveImmutability() throws Exception {
        
        Authorizations a1 = new Authorizations("A", "B", "C");
        Authorizations a2 = new Authorizations("A", "D", "E");
        Authorizations a3 = new Authorizations("A", "F", "G");
        
        List<Key> expectedKeys = Lists.newArrayList(new Key("row", "cf2", "cq1", "A", 1L));
        Value expectedVal = new Value(new byte[0]);
        
        Scanner scanner = ScannerHelper.createScanner(new WrappedAccumuloClient(mockConnector, mockConnector), TABLE_NAME, Arrays.asList(a1, a2, a3));
        // Removing the scan iterator should do nothing to the iterators added by ScannerHelper.createScanner
        scanner.removeScanIterator("visibilityFilter10");
        for (Entry<Key,Value> entry : scanner) {
            assertFalse(expectedKeys.isEmpty(), "Ran out of expected keys but got: " + entry.getKey());
            Key expectedKey = expectedKeys.remove(0);
            assertEquals(expectedKey, entry.getKey());
            assertEquals(expectedVal, entry.getValue());
        }
        assertTrue(expectedKeys.isEmpty(), "Scanner did not return all expected keys: " + expectedKeys);
    }
    
    @Test
    public void testVisibilityFilterModifyImmutability() throws Exception {
        
        Authorizations a1 = new Authorizations("A", "B", "C");
        Authorizations a2 = new Authorizations("A", "D", "E");
        Authorizations a3 = new Authorizations("A", "F", "G");
        
        List<Key> expectedKeys = Lists.newArrayList(new Key("row", "cf2", "cq1", "A", 1L));
        Value expectedVal = new Value(new byte[0]);
        
        Scanner scanner = ScannerHelper.createScanner(new WrappedAccumuloClient(mockConnector, mockConnector), TABLE_NAME, Arrays.asList(a1, a2, a3));
        // Removing the scan iterator should do nothing to the iterators added by ScannerHelper.createScanner
        scanner.updateScanIteratorOption("visibilityFilter10", ConfigurableVisibilityFilter.AUTHORIZATIONS_OPT, "A,B,C");
        scanner.updateScanIteratorOption("visibilityFilter11", ConfigurableVisibilityFilter.AUTHORIZATIONS_OPT, "A,B,C");
        for (Entry<Key,Value> entry : scanner) {
            assertFalse(expectedKeys.isEmpty(), "Ran out of expected keys but got: " + entry.getKey());
            Key expectedKey = expectedKeys.remove(0);
            assertEquals(expectedKey, entry.getKey());
            assertEquals(expectedVal, entry.getValue());
        }
        assertTrue(expectedKeys.isEmpty(), "Scanner did not return all expected keys: " + expectedKeys);
    }
    
    @Test
    public void testVisibilityFilterSystemNameRemoveIntegrity() throws Exception {
        
        Authorizations a1 = new Authorizations("A", "B", "C");
        Authorizations a2 = new Authorizations("A", "D", "E");
        Authorizations a3 = new Authorizations("A", "F", "G");
        
        Scanner scanner = ScannerHelper.createScanner(new WrappedAccumuloClient(mockConnector, mockConnector), TABLE_NAME, Arrays.asList(a1, a2, a3));
        // Removing the scan iterator should do nothing to the iterators added by ScannerHelper.createScanner
        assertThrows(IllegalArgumentException.class, () -> scanner.removeScanIterator("sys_visibilityFilter10"));
    }
    
    @Test
    public void testVisibilityFilterSystemNameModifyIntegrity() throws Exception {
        
        Authorizations a1 = new Authorizations("A", "B", "C");
        Authorizations a2 = new Authorizations("A", "D", "E");
        Authorizations a3 = new Authorizations("A", "F", "G");
        
        Scanner scanner = ScannerHelper.createScanner(new WrappedAccumuloClient(mockConnector, mockConnector), TABLE_NAME, Arrays.asList(a1, a2, a3));
        // Removing the scan iterator should do nothing to the iterators added by ScannerHelper.createScanner
        assertThrows(IllegalArgumentException.class,
                        () -> scanner.updateScanIteratorOption("sys_visibilityFilter10", ConfigurableVisibilityFilter.AUTHORIZATIONS_OPT, "A,B,C"));
    }
    
    @Test
    public void testVisibilityFilterSystemNameAddIntegrity() throws Exception {
        
        Authorizations a1 = new Authorizations("A", "B", "C");
        Authorizations a2 = new Authorizations("A", "D", "E");
        Authorizations a3 = new Authorizations("A", "F", "G");
        
        Scanner scanner = ScannerHelper.createScanner(new WrappedAccumuloClient(mockConnector, mockConnector), TABLE_NAME, Arrays.asList(a1, a2, a3));
        // Removing the scan iterator should do nothing to the iterators added by ScannerHelper.createScanner
        IteratorSetting cfg = new IteratorSetting(10, "dwSystem_mySystemIterator", ConfigurableVisibilityFilter.class);
        assertThrows(IllegalArgumentException.class, () -> scanner.addScanIterator(cfg));
    }
}
