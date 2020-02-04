package datawave.query.tld;

import datawave.ingest.json.util.JsonObjectFlattener.FlattenMode;
import datawave.query.NormalFlattenQueryTest;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.TLDQueryLogic;
import org.apache.log4j.Logger;

/**
 * Test cases for flatten mode {@link FlattenMode@NORMAL}.
 */
public class TLDNormalFlattenQueryTest extends NormalFlattenQueryTest {
    
    private static final Logger log = Logger.getLogger(TLDNormalFlattenQueryTest.class);
    
    protected ShardQueryLogic createQueryLogic() {
        return new TLDQueryLogic();
    }
    
    public TLDNormalFlattenQueryTest() {}
}
