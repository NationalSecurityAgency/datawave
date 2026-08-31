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
import datawave.next.CountScheduler;
import datawave.next.SimpleQueryVisitor;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.planner.QueryPlanner;
import datawave.query.scheduler.PushdownScheduler;
import datawave.query.scheduler.Scheduler;
import datawave.query.tables.shard.CountAggregatingIterator;
import datawave.query.tables.shard.CountResultPostprocessor;
import datawave.query.transformer.ShardQueryCountTableTransformer;

/**
 * A simple extension of the basic ShardQueryTable which applies a counting iterator on top of the "normal" iterator stack.
 * <p>
 * TODO: optimize this so a specialized query iterator returns single counts that are then aggregated
 */
public class CountingShardQueryLogic extends ShardQueryLogic {
    private static final Logger log = Logger.getLogger(CountingShardQueryLogic.class);

    // the time to wait before returning an intermediate result. Zero would return an intermediate result
    // on every call to next(), before the aggregating thread has had any chance to produce the count.
    private long pageWaitTimeMillis = CountAggregatingIterator.DEFAULT_PAGE_WAIT_TIME_MILLIS;

    // retained so the aggregation thread can be released when this logic is closed. Volatile because the thread that starts the query is not necessarily the
    // thread that tears it down, and a reset publishes this reference after the query is already visible to the thread that expires it.
    private volatile CountAggregatingIterator countAggregatingIterator;

    public CountingShardQueryLogic() {
        super();
    }

    /**
     * Copy constructor. The aggregating iterator is deliberately not copied, so that a copy does not close the thread belonging to the logic it was copied
     * from.
     *
     * @param other
     *            the logic to copy
     */
    public CountingShardQueryLogic(CountingShardQueryLogic other) {
        super(other);
        this.setPageWaitTimeMillis(other.getPageWaitTimeMillis());
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
        countAggregatingIterator = new CountAggregatingIterator(this.iterator(), getTransformer(settings), this.markingFunctions, getPageWaitTimeMillis());
        return countAggregatingIterator;
    }

    /**
     * Closes the aggregating iterator in addition to the usual query resources. Aggregation runs on its own thread, which is not released until the iterator is
     * closed.
     */
    @Override
    public void close() {
        if (countAggregatingIterator != null) {
            countAggregatingIterator.close();
        }
        super.close();
    }

    @Override
    public ResultPostprocessor getResultPostprocessor(GenericQueryConfiguration config) {
        return new CountResultPostprocessor(markingFunctions);
    }

    @Override
    public Scheduler getScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory) {
        // planner should already have run
        QueryPlanner planner = getQueryPlanner();
        if (planner instanceof DefaultQueryPlanner && config.getDocumentScannerConfig() != null && config.isUseDocumentScheduler()) {
            DefaultQueryPlanner dqp = (DefaultQueryPlanner) planner;
            boolean simple = SimpleQueryVisitor.validate(config.getQueryTree(), dqp.getIndexedFields(), dqp.getIndexOnlyFields());
            if (simple) {
                CountScheduler countScheduler = new CountScheduler(config);
                countScheduler.setVisitorFunction(getVisitorFunction(dqp.getMetadataHelper()));
                return countScheduler;
            }
        }

        PushdownScheduler scheduler = new PushdownScheduler(config, this.metadataHelperFactory);
        scheduler.addSetting(new IteratorSetting(config.getBaseIteratorPriority() + 50, "counter", ResultCountingIterator.class.getName()));
        return scheduler;
    }

    /**
     * This query logic always supports intermediate results
     *
     * @return true
     */
    @Override
    public boolean isLongRunningQuery() {
        return true;
    }

    public long getPageWaitTimeMillis() {
        return pageWaitTimeMillis;
    }

    public void setPageWaitTimeMillis(long pageWaitTimeMillis) {
        this.pageWaitTimeMillis = pageWaitTimeMillis;
    }
}
