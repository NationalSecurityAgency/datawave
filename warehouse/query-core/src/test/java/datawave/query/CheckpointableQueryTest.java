package datawave.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import datawave.query.tables.ShardQueryLogic;

public class CheckpointableQueryTest extends AnyFieldQueryTest {

    private static final Logger log = LogManager.getLogger(CheckpointableQueryTest.class);

    @Override
    protected ShardQueryLogic createShardQueryLogic() {
        ShardQueryLogic logic = super.createShardQueryLogic();
        logic.setCheckpointable(true);
        return logic;
    }
}
