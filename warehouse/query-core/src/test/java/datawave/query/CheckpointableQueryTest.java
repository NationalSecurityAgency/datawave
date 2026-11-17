package datawave.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.tables.ShardQueryLogic;

public class CheckpointableQueryTest extends AnyFieldQueryTest {

    private static final Logger log = LoggerFactory.getLogger(CheckpointableQueryTest.class);

    @Override
    protected ShardQueryLogic createShardQueryLogic() {
        ShardQueryLogic logic = super.createShardQueryLogic();
        logic.setCheckpointable(true);
        return logic;
    }
}
