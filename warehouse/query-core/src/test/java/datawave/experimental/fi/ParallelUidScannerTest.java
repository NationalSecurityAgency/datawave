package datawave.experimental.fi;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.BeforeClass;

import datawave.experimental.util.AccumuloUtil;
import datawave.util.TableName;

public class ParallelUidScannerTest extends UidScannerTest {

    private final ExecutorService fieldIndexPool = Executors.newFixedThreadPool(5);

    @BeforeClass
    public static void setup() throws Exception {
        util = new AccumuloUtil();
        util.create(ParallelUidScannerTest.class.getSimpleName());
        util.loadData();
    }

    @Override
    protected void test(String query, Set<String> expected) {
        ParallelUidScanner scan = new ParallelUidScanner(fieldIndexPool, util.getClient(), util.getAuths(), TableName.SHARD, "scanId");
        test(scan, query, expected);
    }
}
