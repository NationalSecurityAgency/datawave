package datawave.query.tld;

import datawave.ingest.json.util.JsonObjectFlattener.FlattenMode;
import datawave.query.NormalFlattenQueryTest;
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.TLDQueryLogic;
import datawave.query.testframework.AccumuloSetupHelper;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FileLoaderFactory;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Test cases for flatten mode {@link FlattenMode@NORMAL}.
 */
public class TLDNormalFlattenQueryTest extends NormalFlattenQueryTest {
    
    private static final Logger log = Logger.getLogger(TLDNormalFlattenQueryTest.class);
    
    protected ShardQueryLogic createQueryLogic() {
        return new TLDQueryLogic();
    }
    
    public TLDNormalFlattenQueryTest() {}
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        dataTypes.add(flatten);
        
        final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes, FileLoaderFactory.FileType.JSON);
        connector = helper.loadTables(log, RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER);
    }
    
}
