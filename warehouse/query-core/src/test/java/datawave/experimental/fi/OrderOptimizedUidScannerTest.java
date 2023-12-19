package datawave.experimental.fi;

import java.util.Set;

import org.junit.BeforeClass;

import datawave.experimental.util.AccumuloUtil;
import datawave.util.TableName;

public class OrderOptimizedUidScannerTest extends UidScannerTest {

    @BeforeClass
    public static void setup() throws Exception {
        util = new AccumuloUtil();
        util.create(OrderOptimizedUidScannerTest.class.getSimpleName());
        util.loadData();
    }

    @Override
    protected void test(String query, Set<String> expected) {
        OrderOptimizedUidScanner scanner = new OrderOptimizedUidScanner(util.getClient(), util.getAuths(), TableName.SHARD, "scanId");
        test(scanner, query, expected);
    }

}
