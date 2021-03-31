package datawave.query.tables;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * This test spins up a mini accumulo to accurately test the effect of underlying Scanner/Batch scanners against the ScannerSession. InMemoryAccumulo makes some
 * simplifications that in the past have masked bugs
 */
public class ScannerSessionTest {
    
    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    private static final String PASSWORD = "";
    
    private static MiniAccumuloCluster instance;
    private static Connector connector;
    private static ResourceQueue resourceQueue;
    
    @BeforeClass
    public static void setupClass() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException,
                    InterruptedException {
        instance = new MiniAccumuloCluster(temporaryFolder.newFolder(), PASSWORD);
        instance.start();
        
        connector = instance.getConnector("root", PASSWORD);
        
        setupTable();
    }
    
    @AfterClass
    public static void teardownClass() throws IOException, InterruptedException {
        instance.stop();
    }
    
    private static void setupTable() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, InterruptedException,
                    IOException {
        connector.tableOperations().create("testTable");
        
        // create splits 1 to 99
        SortedSet<Text> splits = new TreeSet<>();
        for (int i = 0; i < 100; i++) {
            splits.add(new Text(String.valueOf(i)));
        }
        connector.tableOperations().addSplits("testTable", splits);
        
        // give the table a chance to be split
        Thread.sleep(10000);
        
        // force writing all the data or fail
        try {
            writeData();
        } catch (MutationsRejectedException e) {
            instance.stop();
            throw new RuntimeException("failed to write data", e);
        }
    }
    
    private static void writeData() throws TableNotFoundException, MutationsRejectedException {
        BatchWriter bw = connector.createBatchWriter("testTable", new BatchWriterConfig());
        
        // add CF 1000 to 1099 with CQ 10000 to 10099, or 10000 entries per row
        for (int i = 0; i < 100; i++) {
            Mutation m = new Mutation(new Text(String.valueOf(i)));
            for (int j = 1000; j < 1100; j++) {
                for (int k = 10000; k < 10100; k++) {
                    m.put(new Text(String.valueOf(j)), new Text(String.valueOf(k)), new Value());
                }
            }
            bw.addMutation(m);
        }
        
        bw.flush();
        bw.close();
    }
    
    @Before
    public void setup() throws Exception {
        resourceQueue = new ResourceQueue(100, connector);
    }
    
    @Test
    public void testScannerSessionLowMaxResults() throws TableNotFoundException {
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations());
        ScannerSession ss = new ScannerSession("testTable", auths, resourceQueue, 5, null);
        
        validate(ss);
    }
    
    @Test
    public void testScannerSessionHighMaxResults() throws TableNotFoundException {
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations());
        ScannerSession ss = new ScannerSession("testTable", auths, resourceQueue, 5000000, null);
        
        validate(ss);
    }
    
    @Test
    public void testScannerSessionWithBatchResourceLowMaxResults() throws TableNotFoundException {
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations());
        ScannerSession ss = new ScannerSession("testTable", auths, resourceQueue, 5, null);
        ss.setResourceClass(BatchResource.class);
        
        validate(ss);
    }
    
    @Test
    public void testScannerSessionWithBatchResourceHighMaxResults() throws TableNotFoundException {
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations());
        ScannerSession ss = new ScannerSession("testTable", auths, resourceQueue, 5000000, null);
        ss.setResourceClass(BatchResource.class);
        
        validate(ss);
    }
    
    @Test(expected = RuntimeException.class)
    public void testScannerSessionWithRuntimeExceptionResource() throws TableNotFoundException {
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations());
        ScannerSession ss = new ScannerSession("testTable", auths, resourceQueue, 5000000, null);
        ss.setResourceClass(StubbedRuntimeExceptionResource.class);
        
        validate(ss);
    }
    
    private void validate(ScannerSession ss) throws TableNotFoundException {
        List<Range> ranges = Arrays.asList(new Range(new Text(String.valueOf(25)), true, new Text(String.valueOf(27)), false),
                        new Range(new Text(String.valueOf(1)), true, new Text(String.valueOf(2)), false), new Range(new Text(String.valueOf(98)), true,
                                        new Text(String.valueOf(99)), false));
        
        ss.setRanges(ranges);
        
        int count = 0;
        Map<Integer,Integer> results = new HashMap<>();
        while (ss.hasNext()) {
            Map.Entry<Key,Value> entry = ss.next();
            int row = Integer.parseInt(entry.getKey().getRow().toString());
            Integer rowCount = results.get(row);
            if (rowCount == null) {
                rowCount = new Integer(0);
            }
            
            rowCount = rowCount.intValue() + 1;
            results.put(row, rowCount);
            count++;
        }
        
        int batchScannerCount = 0;
        BatchScanner scanner = connector.createBatchScanner("testTable", new Authorizations(), 12);
        scanner.setRanges(Arrays.asList(new Range(new Text(String.valueOf(25)), true, new Text(String.valueOf(27)), false),
                        new Range(new Text(String.valueOf(1)), true, new Text(String.valueOf(2)), false), new Range(new Text(String.valueOf(98)), true,
                                        new Text(String.valueOf(99)), false)));
        for (Map.Entry<Key,Value> entry : scanner) {
            batchScannerCount++;
        }
        
        scanner.close();
        
        // direct batch scanner count should equal count
        Assert.assertEquals(batchScannerCount, count);
        
        // 14 total rows covered at 10000 per row (100 CF * 100 CQ per row)
        // 1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 25, 26, 98
        Assert.assertEquals(140000, count);
        Assert.assertEquals(14, results.keySet().size());
        for (Integer row : results.keySet()) {
            Assert.assertEquals(new Integer(10000), results.get(row));
        }
        
        ss.close();
    }
    
    private static class StubbedRuntimeExceptionResource extends AccumuloResource {
        
        public StubbedRuntimeExceptionResource(Connector cxn) {
            super(cxn);
        }
        
        public StubbedRuntimeExceptionResource(AccumuloResource other) {
            super(other);
        }
        
        @Override
        public Iterator<Map.Entry<Key,Value>> iterator() {
            return new Iterator<Map.Entry<Key,Value>>() {
                @Override
                public boolean hasNext() {
                    return true;
                }
                
                @Override
                public Map.Entry<Key,Value> next() {
                    throw new RuntimeException("test exception");
                }
            };
        }
    }
}
