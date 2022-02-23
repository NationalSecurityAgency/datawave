package datawave.query.sequentialscheduler;

import datawave.query.tables.ShardQueryLogic;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.RawDataManager;

/**
 * Force the use of the {@code SequentialScheduler} by overriding the {@code getQueryLogic} method of the superclass
 */
public abstract class UseSequentialScheduler extends AbstractFunctionalQuery {
    
    protected UseSequentialScheduler(final RawDataManager mgr) {
        super(mgr);
    }
    
    @Override
    protected ShardQueryLogic createQueryLogic() {
        ShardQueryLogic queryLogic = super.createQueryLogic();
        queryLogic.getConfig().setSequentialScheduler(true);
        return queryLogic;
    }
}
