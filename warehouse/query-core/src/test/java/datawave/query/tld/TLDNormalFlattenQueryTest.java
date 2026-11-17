package datawave.query.tld;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.NormalFlattenQueryTest;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.TLDQueryLogic;

/**
 * Test cases for flatten mode {@link FlattenMode@NORMAL}.
 */
public class TLDNormalFlattenQueryTest extends NormalFlattenQueryTest {

    private static final Logger log = LoggerFactory.getLogger(TLDNormalFlattenQueryTest.class);

    protected ShardQueryLogic createShardQueryLogic() {
        return new TLDQueryLogic();
    }

    public TLDNormalFlattenQueryTest() {}
}
