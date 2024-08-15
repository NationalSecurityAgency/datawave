package datawave.query.tables;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;

import datawave.core.iterators.ResultCountingIterator;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.core.query.logic.ResultPostprocessor;
import datawave.microservice.query.Query;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.scheduler.PushdownScheduler;
import datawave.query.scheduler.Scheduler;
import datawave.query.tables.shard.CountAggregatingIterator;
import datawave.query.tables.shard.CountResultPostprocessor;
import datawave.query.transformer.ShardQueryCountTableTransformer;

/**
 * A simple extension of the basic ShardQueryTable which applies a counting iterator on top of the "normal" iterator stack.
 *
 *
 *
 */
public class CountingShardQueryLogic extends ShardQueryLogic {
    private static final Logger log = Logger.getLogger(CountingShardQueryLogic.class);

    public CountingShardQueryLogic() {
        super();
    }

    public CountingShardQueryLogic(CountingShardQueryLogic other) {
        super(other);
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
        GenericQueryConfiguration config = super.initialize(client, settings, runtimeQueryAuthorizations);
        config.setReduceResults(true);
        return config;
    }

    @Override
    public CountingShardQueryLogic clone() {
        return new CountingShardQueryLogic(this);
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return new ShardQueryCountTableTransformer(settings, this.markingFunctions, this.responseObjectFactory);
    }

    @Override
    public TransformIterator getTransformIterator(Query settings) {
        return new CountAggregatingIterator(this.iterator(), getTransformer(settings), this.markingFunctions);
    }

    @Override
    public ResultPostprocessor getResultPostprocessor(GenericQueryConfiguration config) {
        return new CountResultPostprocessor(markingFunctions);
    }

    @Override
    public Scheduler getScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory) {
        PushdownScheduler scheduler = new PushdownScheduler(config, scannerFactory, this.metadataHelperFactory);
        scheduler.addSetting(new IteratorSetting(config.getBaseIteratorPriority() + 50, "counter", ResultCountingIterator.class.getName()));
        return scheduler;
    }

}
