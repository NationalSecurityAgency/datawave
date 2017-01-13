package nsa.datawave.query.rewrite.tables;

import nsa.datawave.core.iterators.ResultCountingIterator;
import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.rewrite.scheduler.PushdownScheduler;
import nsa.datawave.query.rewrite.scheduler.Scheduler;
import nsa.datawave.query.tables.ScannerFactory;
import nsa.datawave.query.tables.shard.CountAggregatingIterator;
import nsa.datawave.query.transformer.ShardQueryCountTableTransformer;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.logic.QueryLogicTransformer;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.commons.collections.iterators.TransformIterator;
import org.apache.log4j.Logger;

/**
 * A simple extension of the basic ShardQueryTable which applies a counting iterator on top of the "normal" iterator stack.
 * 
 * 
 * 
 */
public class RefactoredCountingShardQueryLogic extends RefactoredShardQueryLogic {
    private static final Logger log = Logger.getLogger(RefactoredCountingShardQueryLogic.class);
    
    public RefactoredCountingShardQueryLogic() {
        super();
    }
    
    public RefactoredCountingShardQueryLogic(RefactoredCountingShardQueryLogic other) {
        super(other);
    }
    
    @Override
    public RefactoredCountingShardQueryLogic clone() {
        return new RefactoredCountingShardQueryLogic(this);
    }
    
    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return new ShardQueryCountTableTransformer(settings, this.markingFunctions, this.responseObjectFactory);
    }
    
    @Override
    public TransformIterator getTransformIterator(Query settings) {
        return new CountAggregatingIterator(this.iterator(), getTransformer(settings));
    }
    
    @Override
    public Scheduler getScheduler(RefactoredShardQueryConfiguration config, ScannerFactory scannerFactory) {
        PushdownScheduler scheduler = new PushdownScheduler(config, scannerFactory, this.metadataHelperFactory);
        scheduler.addSetting(new IteratorSetting(config.getBaseIteratorPriority() + 50, "counter", ResultCountingIterator.class.getName()));
        return scheduler;
    }
    
}
