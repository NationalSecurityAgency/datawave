package datawave.query.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.query.configuration.QueryData;
import datawave.core.query.configuration.Result;
import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.mr.bulk.RfileResource;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.BatchScannerSession;
import datawave.query.tables.BatchScannerSessionBuilder;
import datawave.query.tables.ScanSessionManager;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.async.ScannerChunk;
import datawave.query.tables.async.event.VisitorFunction;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.scan.ExecutionHintHelper;

/**
 * Purpose: Pushes down individual queries to the Tservers. Is aware that each server may have a different query, thus bins ranges per tserver and keeps the
 * plan that corresponds with those queries
 */
public class PushdownScheduler extends Scheduler {

    private static final Logger log = ThreadConfigurableLogger.getLogger(PushdownScheduler.class);

    /**
     * Configuration reference.
     */
    protected final ShardQueryConfiguration config;

    /**
     * Count for the number of QueryPlans that we have
     */
    protected final AtomicInteger count = new AtomicInteger(0);
    /**
     * BatchScannerSession reference, so that we can close upon completion or error
     */
    protected BatchScannerSession session = null;

    protected ScanSessionManager sessionManager = new ScanSessionManager();

    protected List<Function<IteratorSetting,IteratorSetting>> customizedFunctionList;

    /**
     * Local instance of the table ID
     */
    protected TableId tableId;

    protected MetadataHelper metadataHelper;

    @Deprecated(forRemoval = true, since = "7.40.0")
    public PushdownScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelperFactory metaFactory) {
        this(config, scannerFactory, metaFactory.createMetadataHelper(config.getClient(), config.getMetadataTableName(), config.getAuthorizations()));
    }

    @Deprecated(forRemoval = true, since = "7.40.0")
    protected PushdownScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper) {
        this(config, helper);
    }

    /**
     * Constructor that accepts a {@link ShardQueryConfiguration} and a {@link MetadataHelperFactory}
     *
     * @param config
     *            the shard query config
     * @param metaFactory
     *            the metadata helper factory
     */
    public PushdownScheduler(ShardQueryConfiguration config, MetadataHelperFactory metaFactory) {
        this(config, metaFactory.createMetadataHelper(config.getClient(), config.getMetadataTableName(), config.getAuthorizations()));
    }

    /**
     * Constructor that accepts a {@link ShardQueryConfiguration} and a {@link MetadataHelper}
     *
     * @param config
     *            the shard query config
     * @param helper
     *            the metadata helper
     */
    protected PushdownScheduler(ShardQueryConfiguration config, MetadataHelper helper) {
        this.config = config;
        this.metadataHelper = helper;
        customizedFunctionList = Lists.newArrayList();
        Preconditions.checkNotNull(config.getClient());
    }

    public void addSetting(IteratorSetting customSetting) {
        settings.add(customSetting);
    }

    @Override
    public List<QueryCheckpoint> checkpoint(QueryKey queryKey) {
        // if we were not actually started, then simple return the query data checkpoints
        if (session == null) {
            Iterator<QueryData> queries = getQueryDataIterator();
            List<QueryCheckpoint> checkpoints = new ArrayList<>();
            while (queries.hasNext()) {
                checkpoints.add(new QueryCheckpoint(queryKey, Collections.singletonList(queries.next())));
            }
            return checkpoints;
        } else {
            List<QueryCheckpoint> checkpoints = session.checkpoint(queryKey);
            close();
            return checkpoints;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<Result> iterator() {
        if (null == this.config) {
            throw new IllegalArgumentException("Null configuration provided");
        }

        try {
            return concatIterators();
        } catch (AccumuloException | ParseException | TableNotFoundException | AccumuloSecurityException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Concatenate Iterators
     *
     * @return the concatenated iterators
     * @throws ParseException
     *             if there is an error parsing
     * @throws TableNotFoundException
     *             if the table is not found
     * @throws AccumuloSecurityException
     *             if there is a security issue with Accumulo
     * @throws AccumuloException
     *             if there is a general Accumulo error
     */
    protected Iterator<Result> concatIterators() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, ParseException {
        boolean hasNext = config.getQueriesIter().hasNext();
        String tableName = config.getShardTableName();

        Set<Authorizations> auths = config.getAuthorizations();

        AccumuloClient client = config.getClient();
        if (client instanceof InMemoryAccumuloClient) {
            tableId = TableId.of(config.getTableName());
        } else {
            ClientContext ctx = AccumuloConnectionFactory.getClientContext(client);
            tableId = ctx.getTableId(tableName);
        }

        Iterator<List<ScannerChunk>> chunkIter = Iterators.transform(getQueryDataIterator(), getPushdownFunction());

        try {
            //  @formatter:off
            BatchScannerSessionBuilder builder = BatchScannerSessionBuilder.create(client)
                    .setTableName(tableName)
                    .setAuthorizations(auths)
                    .setQuery(config.getQuery())
                    .setNumQueryThreads(config.getNumQueryThreads());
            //  @formatter:on

            ConsistencyLevel consistencyLevel = ExecutionHintHelper.getConsistencyLevel(tableName, config.getTableConsistencyLevels());
            if (consistencyLevel != null) {
                builder.setConsistencyLevel(consistencyLevel);
            }

            Map<String,String> executionHints = ExecutionHintHelper.getExecutionHints(tableName, config.getTableHints());
            if (executionHints != null) {
                builder.setScanType(ExecutionHintHelper.getScanType(executionHints));
                builder.setScanPriority(ExecutionHintHelper.getPriority(executionHints));
            }

            session = builder.build();
            session.setConfig(config);
            sessionManager.addScanner(session);

            if (config.getBypassAccumulo()) {
                session.setDelegatedInitializer(RfileResource.class);
            }

            if (config.getSpeculativeScanning()) {
                session.setSpeculativeScanning(true);
            }

            session.addVisitor(new VisitorFunction(config, metadataHelper));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        session.setScanLimit(config.getMaxDocScanTimeout());

        if (config.getBackoffEnabled()) {
            session.setBackoffEnabled(true);
        }

        session.setChunkIter(chunkIter);

        session.updateIdentifier(config.getQuery().getId().toString());

        return session;
    }

    protected PushdownFunction getPushdownFunction() {
        return new PushdownFunction(config, settings, tableId);
    }

    protected Iterator<QueryData> getQueryDataIterator() {
        if (config.isCheckpointable()) {
            return new SingleRangeQueryDataIterator(config.getQueriesIter());
        } else {
            return config.getQueriesIter();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() {
        sessionManager.close();
        log.debug("Ran " + count.get() + " queries for a single user query");
    }

    /*
     * (non-Javadoc)
     *
     * @see Scheduler#createBatchScanner(ShardQueryConfiguration, datawave.query.tables.ScannerFactory, datawave.webservice.query.configuration.QueryData)
     */
    @Override
    public BatchScanner createBatchScanner(ShardQueryConfiguration config, ScannerFactory scannerFactory, QueryData qd) throws TableNotFoundException {
        return scannerFactory.newScanner(config, qd, config.getShardTableName());
    }

    @Override
    public ScanSessionStats getSchedulerStats() {

        ScanSessionStats stats = null;
        if (null != session) {
            stats = session.getStatistics();
        }
        return stats;
    }

}
