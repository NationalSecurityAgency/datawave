package datawave.experimental.fi;

import datawave.experimental.util.AccumuloUtil;
import datawave.util.TableName;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ParallelFiScannerTest extends FiScannerTest {
    
    private final ExecutorService fieldIndexPool = Executors.newFixedThreadPool(5);
    
    @BeforeClass
    public static void setup() throws Exception {
        util = new AccumuloUtil();
        util.create(ParallelFiScannerTest.class.getSimpleName());
        util.loadData();
    }
    
    protected void test(String query, Map<String,Set<String>> expected) {
        ParallelFiScanner scan = new ParallelFiScanner(fieldIndexPool, "scanId", util.getConnector(), TableName.SHARD, util.getAuths(),
                        util.getMetadataHelper());
        test(scan, query, expected);
    }
}
