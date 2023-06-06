package datawave.query.tables;

import com.google.common.collect.Lists;
import datawave.marking.MarkingFunctions;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.QueryOptions;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.planner.MetadataHelperQueryModelProvider;
import datawave.query.planner.QueryModelProvider;
import datawave.query.planner.QueryPlanner;
import datawave.query.scheduler.PushdownScheduler;
import datawave.query.scheduler.Scheduler;
import datawave.query.scheduler.SequentialScheduler;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.query.transformer.DocumentTransform;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.transformer.EventQueryDataDecoratorTransformer;
import datawave.query.transformer.GroupingTransform;
import datawave.query.transformer.UniqueTransform;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.util.time.TraceStopwatch;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.configuration.QueryData;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Purpose: Runs the traditional even query logic. encompasses the same logic.
 *
 * Design: Extends ShardedBaseQueryLogic to support emitting an entry of key/value.
 *
 *
 **/
public class ShardQueryLogic extends ShardedBaseQueryLogic<Entry<Key,Value>, ShardQueryConfiguration> {
    /**
     * Basic constructor
     */
    public ShardQueryLogic() {
        super();
        if (log.isTraceEnabled())
            log.trace("Creating ShardQueryLogic: " + System.identityHashCode(this));
    }

    /**
     * Copy constructor
     *
     * @param other
     *            - another ShardQueryLogic object
     */
    public ShardQueryLogic(ShardQueryLogic other) {
        super(other);

        if (log.isTraceEnabled())
            log.trace("Creating Cloned ShardQueryLogic: " + System.identityHashCode(this) + " from " + System.identityHashCode(other));

        // Set ShardQueryConfiguration variables
        this.config = ShardQueryConfiguration.create(other);
    }

    /**
     * Validate that the configuration is in a consistent state
     *
     * @throws IllegalArgumentException
     *             when config constraints are violated
     */
    @Override
    protected void validateConfiguration(ShardQueryConfiguration config) {
        // do not allow disabling track sizes unless page size is no more than 1
        if (!config.isTrackSizes() && this.getMaxPageSize() > 1) {
            throw new IllegalArgumentException("trackSizes cannot be disabled with a page size greater than 1");
        }
    }

    public static BatchScanner createBatchScanner(ShardQueryConfiguration config, ScannerFactory scannerFactory, QueryData qd) throws TableNotFoundException {
        final BatchScanner bs = scannerFactory.newScanner(config.getShardTableName(), config.getAuthorizations(), config.getNumQueryThreads(),
                config.getQuery(),false);

        if (log.isTraceEnabled()) {
            log.trace("Running with " + config.getAuthorizations() + " and " + config.getNumQueryThreads() + " threads: " + qd);
        }

        if (log.isTraceEnabled()) {
            log.trace("Running with " + config.getAuthorizations() + " and " + config.getNumQueryThreads() + " threads: " + qd.getRanges().size());
        }
        bs.setRanges(qd.getRanges());

        for (IteratorSetting cfg : qd.getSettings()) {
            bs.addScanIterator(cfg);
        }

        return bs;
    }


    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {

        this.config = ShardQueryConfiguration.create(this, settings);
        if (log.isTraceEnabled())
            log.trace("Initializing ShardQueryLogic: " + System.identityHashCode(this) + '('
                    + (this.getSettings() == null ? "empty" : this.getSettings().getId()) + ')');
        this.config.setExpandFields(true);
        this.config.setExpandValues(true);
        initialize(config, client, settings, auths);
        return config;
    }

    @Override
    public String getPlan(AccumuloClient client, Query settings, Set<Authorizations> auths, boolean expandFields, boolean expandValues) throws Exception {

        this.config = ShardQueryConfiguration.create(this, settings);
        if (log.isTraceEnabled())
            log.trace("Initializing ShardQueryLogic for plan: " + System.identityHashCode(this) + '('
                    + (this.getSettings() == null ? "empty" : this.getSettings().getId()) + ')');
        this.config.setExpandFields(expandFields);
        this.config.setExpandValues(expandValues);
        // if we are not generating the full plan, then set the flag such that we avoid checking for final executability/full table scan
        if (!expandFields || !expandValues) {
            this.config.setGeneratePlanOnly(true);
        }
        initialize(config, client, settings, auths);
        return config.getQueryString();
    }


    @Override
    protected Iterator<Entry<Key, Value>> getDedupedIterator() {
        return new DedupingIterator(this.iterator);
    }

    protected String getStopwatchHeader(ShardQueryConfiguration config) {
        return "ShardQueryLogic: " + config.getQueryString() + ", [" + config.getBeginDate() + ", " + config.getEndDate() + "]";
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        if (this.transformerInstance != null) {
            addConfigBasedTransformers();
            return this.transformerInstance;
        }

        MarkingFunctions markingFunctions = this.getMarkingFunctions();
        ResponseObjectFactory responseObjectFactory = this.getResponseObjectFactory();

        boolean reducedInSettings = false;
        String reducedResponseStr = settings.findParameter(QueryOptions.REDUCED_RESPONSE).getParameterValue().trim();
        if (org.apache.commons.lang.StringUtils.isNotBlank(reducedResponseStr)) {
            reducedInSettings = Boolean.parseBoolean(reducedResponseStr);
        }
        boolean reduced = (this.isReducedResponse() || reducedInSettings);
        DocumentTransformer transformer = new DocumentTransformer(this, settings, markingFunctions, responseObjectFactory, reduced);
        transformer.setEventQueryDataDecoratorTransformer(eventQueryDataDecoratorTransformer);
        transformer.setContentFieldNames(getConfig().getContentFieldNames());
        transformer.setLogTimingDetails(this.getLogTimingDetails());
        transformer.setCardinalityConfiguration(cardinalityConfiguration);
        transformer.setPrimaryToSecondaryFieldMap(primaryToSecondaryFieldMap);
        transformer.setQm(queryModel);
        this.transformerInstance = transformer;
        addConfigBasedTransformers();
        return this.transformerInstance;
    }

    public boolean isLongRunningQuery() {
        return !getConfig().getGroupFields().isEmpty();
    }

    /**
     * If the configuration didn't exist, OR IT CHANGED, we need to create or update the transformers that have been added.
     */
    private void addConfigBasedTransformers() {
        if (getConfig() != null) {
            ((DocumentTransformer) this.transformerInstance).setProjectFields(getConfig().getProjectFields());
            ((DocumentTransformer) this.transformerInstance).setBlacklistedFields(getConfig().getBlacklistedFields());

            if (getConfig().getUniqueFields() != null && !getConfig().getUniqueFields().isEmpty()) {
                DocumentTransform alreadyExists = ((DocumentTransformer) this.transformerInstance).containsTransform(UniqueTransform.class);
                if (alreadyExists != null) {
                    ((UniqueTransform) alreadyExists).updateConfig(getConfig().getUniqueFields(), getQueryModel());
                } else {
                    ((DocumentTransformer) this.transformerInstance).addTransform(new UniqueTransform(this, getConfig().getUniqueFields()));
                }
            }

            if (getConfig().getGroupFields() != null && !getConfig().getGroupFields().isEmpty()) {
                DocumentTransform alreadyExists = ((DocumentTransformer) this.transformerInstance).containsTransform(GroupingTransform.class);
                if (alreadyExists != null) {
                    ((GroupingTransform) alreadyExists).updateConfig(getConfig().getGroupFields(), getQueryModel());
                } else {
                    ((DocumentTransformer) this.transformerInstance).addTransform(new GroupingTransform(getQueryModel(), getConfig().getGroupFields(),
                            this.markingFunctions, this.getQueryExecutionForPageTimeout()));
                }
            }
        }
        if (getQueryModel() != null) {
            ((DocumentTransformer) this.transformerInstance).setQm(getQueryModel());
        }
    }


    /**
     * Loads a query Model
     *
     * @param helper
     * @param config
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws TableNotFoundException
     * @throws ExecutionException
     */
    protected void loadQueryModel(MetadataHelper helper, ShardQueryConfiguration config) throws InstantiationException, IllegalAccessException,
            TableNotFoundException, ExecutionException {
        TraceStopwatch modelWatch = config.getTimers().newStartedStopwatch("ShardQueryLogic - Loading the query model");

        int cacheKeyCode = new HashCodeBuilder().append(config.getDatatypeFilter()).append(config.getModelName()).hashCode();

        if (config.getCacheModel()) {
            queryModel = queryModelMap.getIfPresent(String.valueOf(cacheKeyCode));
        }
        if (null == queryModel && (null != config.getModelName() && null != config.getModelTableName())) {

            queryModel = helper.getQueryModel(config.getModelTableName(), config.getModelName(), helper.getIndexOnlyFields(config.getDatatypeFilter()));

            if (config.getCacheModel()) {

                queryModelMap.put(String.valueOf(cacheKeyCode), queryModel);
            }

        }
        config.setQueryModel(queryModel);

        modelWatch.stop();

    }

    protected Scheduler<Entry<Key,Value>> getScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory) {
        if (config.getSequentialScheduler()) {
            return new SequentialScheduler(config, scannerFactory);
        } else {
            return new PushdownScheduler(config, scannerFactory, this.metadataHelperFactory);
        }
    }

    public EventQueryDataDecoratorTransformer getEventQueryDataDecoratorTransformer() {
        return eventQueryDataDecoratorTransformer;
    }

    public void setEventQueryDataDecoratorTransformer(EventQueryDataDecoratorTransformer eventQueryDataDecoratorTransformer) {
        this.eventQueryDataDecoratorTransformer = eventQueryDataDecoratorTransformer;
    }

    @Override
    public ShardQueryLogic clone() {
        return new ShardQueryLogic(this);
    }

    @Override
    public void close() {

        super.close();

        log.debug("Closing ShardQueryLogic: " + System.identityHashCode(this));

        if (null == scannerFactory) {
            log.debug("ScannerFactory was never initialized because, therefore there are no connections to close: " + System.identityHashCode(this));
        } else {
            log.debug("Closing ShardQueryLogic scannerFactory: " + System.identityHashCode(this));
            try {
                int nClosed = 0;
                scannerFactory.lockdown();
                for (ScannerBase bs : Lists.newArrayList(scannerFactory.currentScanners())) {
                    scannerFactory.close(bs);
                    ++nClosed;
                }
                if (log.isDebugEnabled()) {
                    log.debug("Cleaned up " + nClosed + " batch scanners associated with this query logic.");
                }

                nClosed = 0;

                for (BaseScannerSession<?> bs : Lists.newArrayList(scannerFactory.currentSessions())) {
                    scannerFactory.close(bs);
                    ++nClosed;
                }

                if (log.isDebugEnabled()) {
                    log.debug("Cleaned up " + nClosed + " scanner sessions.");
                }

            } catch (Exception e) {
                log.error("Caught exception trying to close scannerFactory", e);
            }

        }

        if (null != this.planner) {
            try {
                log.debug("Closing ShardQueryLogic planner: " + System.identityHashCode(this) + '('
                        + (this.getSettings() == null ? "empty" : this.getSettings().getId()) + ')');
                this.planner.close(getConfig(), this.getSettings());
            } catch (Exception e) {
                log.error("Caught exception trying to close QueryPlanner", e);
            }
        }

        if (null != this.queries) {
            try {
                log.debug("Closing ShardQueryLogic queries: " + System.identityHashCode(this));
                this.queries.close();
            } catch (IOException e) {
                log.error("Caught exception trying to close CloseableIterable of queries", e);
            }
        }

        if (null != this.scheduler) {
            try {
                log.debug("Closing ShardQueryLogic scheduler: " + System.identityHashCode(this));
                this.scheduler.close();

                ScanSessionStats stats = this.scheduler.getSchedulerStats();

                if (null != stats) {
                    stats.logSummary(log);
                }

            } catch (IOException e) {
                log.error("Caught exception trying to close Scheduler", e);
            }
        }

    }

    @Override
    public ShardQueryConfiguration getConfig() {
        if (config == null) {
            config = ShardQueryConfiguration.create();
        }

        return config;
    }

    public void setConfig(ShardQueryConfiguration config) {
        this.config = config;
    }


    @Override
    public void configurePlanner(MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper) {

        QueryPlanner queryPlanner = getQueryPlanner();
        if (queryPlanner instanceof DefaultQueryPlanner) {
            DefaultQueryPlanner currentQueryPlanner = (DefaultQueryPlanner) queryPlanner;
            currentQueryPlanner.setMetadataHelper(metadataHelper);
            currentQueryPlanner.setDateIndexHelper(dateIndexHelper);

            QueryModelProvider queryModelProvider = currentQueryPlanner.getQueryModelProviderFactory().createQueryModelProvider();
            if (queryModelProvider instanceof MetadataHelperQueryModelProvider) {
                ((MetadataHelperQueryModelProvider) queryModelProvider).setMetadataHelper(metadataHelper);
                ((MetadataHelperQueryModelProvider) queryModelProvider).setConfig(config);
            }

            if (null != queryModelProvider.getQueryModel()) {
                queryModel = queryModelProvider.getQueryModel();

            }
        }
    }

    @Override
    public QueryPlanner getQueryPlanner() {
        if (null == planner) {
            planner = new DefaultQueryPlanner();
        }

        return planner;

    }
}
