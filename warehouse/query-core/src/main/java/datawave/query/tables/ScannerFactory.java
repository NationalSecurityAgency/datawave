package datawave.query.tables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.microservice.query.Query;
import datawave.mr.bulk.BulkInputFormat;
import datawave.mr.bulk.MultiRfileInputformat;
import datawave.mr.bulk.RfileScanner;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.query.util.QueryScannerHelper;
import datawave.webservice.common.connection.WrappedConnector;

public class ScannerFactory {

    private static final int DEFAULT_MAX_THREADS = 100;
    protected int maxQueue = 1000;

    protected final Set<ScannerBase> instances = Collections.synchronizedSet(new HashSet<>());
    protected final Set<ScannerSession> sessionInstances = Collections.synchronizedSet(new HashSet<>());
    protected AccumuloClient client;
    // using an AtomicBoolean to give us a separate monitor for synchronization
    protected final AtomicBoolean open = new AtomicBoolean(true);

    protected boolean accrueStats = false;
    protected Query settings = null;
    protected ResourceQueue scanQueue = null;
    protected ShardQueryConfiguration config = null;

    protected Map<String,ScannerBase.ConsistencyLevel> consistencyByTable = new HashMap<>();
    protected Map<String,Map<String,String>> hintsByTable = new HashMap<>();

    private static final Logger log = Logger.getLogger(ScannerFactory.class);

    /**
     * Preferred constructor, builds scanner factory from configs
     *
     * @param config
     *            a {@link GenericQueryConfiguration}
     */
    public ScannerFactory(GenericQueryConfiguration config) {
        updateConfigs(config);
    }

    /**
     * Constructor that accepts a prebuilt AccumuloClient
     *
     * @param client
     *            an {@link AccumuloClient}
     */
    public ScannerFactory(AccumuloClient client) {
        this(client, DEFAULT_MAX_THREADS);

    }

    /**
     * Constructor that accepts a prebuild AccumuloClient and limits the internal result queue to the provided value
     *
     * @param client
     *            an {@link AccumuloClient}
     * @param queueSize
     *            the internal result queue size
     */
    public ScannerFactory(AccumuloClient client, int queueSize) {
        try {
            this.client = client;
            this.scanQueue = new ResourceQueue(queueSize, client);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Method that allows a ScannerFactory to be updated by a config after initialization
     *
     * @param genericConfig
     *            a {@link GenericQueryConfiguration}
     */
    public void updateConfigs(GenericQueryConfiguration genericConfig) {

        this.client = genericConfig.getClient();

        Map<String,ScannerBase.ConsistencyLevel> consistencyLevels = genericConfig.getTableConsistencyLevels();
        if (consistencyLevels != null && !consistencyLevels.isEmpty()) {
            this.consistencyByTable = genericConfig.getTableConsistencyLevels();
        }

        Map<String,Map<String,String>> hints = genericConfig.getTableHints();
        if (hints != null && !hints.isEmpty()) {
            this.hintsByTable = genericConfig.getTableHints();
        }

        int numThreads = DEFAULT_MAX_THREADS;
        if (genericConfig instanceof ShardQueryConfiguration) {
            ShardQueryConfiguration config = (ShardQueryConfiguration) genericConfig;

            this.settings = config.getQuery();
            this.accrueStats = config.getAccrueStats();
            this.maxQueue = config.getMaxScannerBatchSize();
            this.config = config;

            numThreads = config.getNumQueryThreads();
        }

        try {
            this.scanQueue = new ResourceQueue(numThreads, this.client);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (log.isDebugEnabled()) {
            log.debug("Created ScannerFactory " + System.identityHashCode(this) + " is wrapped ? " + (client instanceof WrappedConnector));
        }
    }

    public Scanner newSingleScanner(String tableName, Set<Authorizations> auths, Query query) throws TableNotFoundException {
        if (open.get()) {
            Scanner bs = QueryScannerHelper.createScannerWithoutInfo(client, tableName, auths, query);
            applyConfigs(bs, tableName);

            log.debug("Created scanner " + System.identityHashCode(bs));
            if (log.isTraceEnabled()) {
                log.trace("Adding instance " + bs.hashCode());
            }

            synchronized (open) {
                if (open.get()) {
                    instances.add(bs);
                    return bs;
                } else {
                    bs.close();
                    throw new IllegalStateException("Factory has been locked. No new scanners can be created.");
                }
            }
        } else {
            throw new IllegalStateException("Factory has been locked. No new scanners can be created.");
        }
    }

    public BatchScanner newScanner(String tableName, Set<Authorizations> auths, int threads, Query query) throws TableNotFoundException {
        if (open.get()) {
            BatchScanner bs = QueryScannerHelper.createBatchScanner(client, tableName, auths, threads, query);
            applyConfigs(bs, tableName);

            log.debug("Created scanner " + System.identityHashCode(bs));
            if (log.isTraceEnabled()) {
                log.trace("Adding instance " + bs.hashCode());
            }
            synchronized (open) {
                if (open.get()) {
                    instances.add(bs);
                    return bs;
                } else {
                    bs.close();
                    throw new IllegalStateException("Factory has been locked. No new scanners can be created.");
                }
            }
        } else {
            throw new IllegalStateException("Factory has been locked. No new scanners can be created.");
        }
    }

    public BatchScanner newScanner(String tableName, Set<Authorizations> auths, int threads, Query query, boolean reportErrors) throws TableNotFoundException {
        if (open.get()) {
            BatchScanner bs = QueryScannerHelper.createBatchScanner(client, tableName, auths, threads, query, reportErrors);
            applyConfigs(bs, tableName);

            log.debug("Created scanner " + System.identityHashCode(bs));
            if (log.isTraceEnabled()) {
                log.trace("Adding instance " + bs.hashCode());
            }
            synchronized (open) {
                if (open.get()) {
                    instances.add(bs);
                    return bs;
                } else {
                    bs.close();
                    throw new IllegalStateException("Factory has been locked. No new scanners can be created.");
                }
            }
        } else {
            throw new IllegalStateException("Factory has been locked. No new scanners can be created.");
        }
    }

    public BatchScanner newScanner(String tableName, Set<Authorizations> auths, Query query) throws TableNotFoundException {
        return newScanner(tableName, auths, 1, query);
    }

    public BatchScanner newScanner(String tableName, Query query) throws TableNotFoundException {
        return newScanner(tableName, Collections.singleton(Authorizations.EMPTY), query);
    }

    /**
     * Builds a new scanner session using a finalized table name and set of authorizations using the previously defined queue. Note that the number of entries
     * is hardcoded, below, to 1000, but can be changed
     *
     * @param tableName
     *            the table string
     * @param auths
     *            a set of auths
     * @param settings
     *            query settings
     * @return a new scanner session
     * @throws Exception
     *             if there are issues
     */
    public BatchScannerSession newQueryScanner(final String tableName, final Set<Authorizations> auths, Query settings) throws Exception {
        return newLimitedScanner(BatchScannerSession.class, tableName, auths, settings).setThreads(scanQueue.getCapacity());
    }

    /**
     * Builds a new scanner session using a finalized table name and set of authorizations using the previously defined queue. Note that the number of entries
     * is hardcoded, below, to 1000, but can be changed
     *
     * @param tableName
     *            the table string
     * @param auths
     *            a set of auths
     * @param settings
     *            query settings
     * @param <T>
     *            type of the wrapper
     * @param wrapper
     *            a wrapper class
     * @return a new scanner session
     * @throws Exception
     *             if there are issues
     *
     */
    public <T extends ScannerSession> T newLimitedScanner(Class<T> wrapper, final String tableName, final Set<Authorizations> auths, final Query settings)
                    throws Exception {
        Preconditions.checkNotNull(scanQueue);
        Preconditions.checkNotNull(wrapper);
        Preconditions.checkArgument(open.get(), "Factory has been locked. No New scanners can be created");

        log.debug("Creating limited scanner whose max threads is is " + scanQueue.getCapacity() + " and max capacity is " + maxQueue);

        ScanSessionStats stats = null;
        if (accrueStats) {
            stats = new ScanSessionStats();
        }

        T session;
        if (wrapper == ScannerSession.class) {
            session = (T) new ScannerSession(tableName, auths, scanQueue, maxQueue, settings).applyStats(stats);
        } else {
            session = wrapper.getConstructor(ScannerSession.class)
                            .newInstance(new ScannerSession(tableName, auths, scanQueue, maxQueue, settings).applyStats(stats));
        }

        applyConfigs(session, tableName);

        log.debug("Created session " + System.identityHashCode(session));
        if (log.isTraceEnabled()) {
            log.trace("Adding instance " + session.hashCode());
        }
        synchronized (open) {
            if (open.get()) {
                sessionInstances.add(session);
                return session;
            } else {
                session.close();
                throw new IllegalStateException("Factory has been locked. No new scanners can be created.");
            }
        }
    }

    /**
     * Builds a new scanner session using a finalized table name and set of authorizations using the previously defined queue. Note that the number of entries
     * is hardcoded, below, to 1000, but can be changed
     *
     * @param tableName
     *            the table string
     * @param auths
     *            a set of auths
     * @param settings
     *            query settings
     * @return a new scanner session
     * @throws Exception
     *             if there are issues
     */
    public RangeStreamScanner newRangeScanner(final String tableName, final Set<Authorizations> auths, final Query settings) throws Exception {
        return newRangeScanner(tableName, auths, settings, Integer.MAX_VALUE);
    }

    public RangeStreamScanner newRangeScanner(String tableName, Set<Authorizations> auths, Query query, int shardsPerDayThreshold) throws Exception {
        return newLimitedScanner(RangeStreamScanner.class, tableName, auths, settings).setScannerFactory(this);
    }

    public boolean close(ScannerBase bs) {
        try {
            log.debug("Closed scanner " + System.identityHashCode(bs));
            if (instances.remove(bs)) {
                if (log.isTraceEnabled()) {
                    log.trace("Closing instance " + bs.hashCode());
                }
                bs.close();
                return true;
            }
        } catch (Exception e) {
            // ANY EXCEPTION HERE CAN SAFELY BE IGNORED
            log.trace("Exception closing ScannerBase, can be safely ignored: {}", e);
        }
        return false;
    }

    /**
     * Returns a NEW collection of scanner instances to the caller.
     *
     * @return a NEW collection of scanners
     */
    public Collection<ScannerBase> currentScanners() {
        return new ArrayList<>(instances);
    }

    /**
     * Returns a NEW collection of scanner session instances to the caller.
     *
     * @return a NEW collection of scanner session instances
     */
    public Collection<ScannerSession> currentSessions() {
        return new ArrayList<>(sessionInstances);
    }

    public boolean lockdown() {
        log.debug("Locked scanner factory " + System.identityHashCode(this));
        if (log.isTraceEnabled()) {
            log.trace("Locked down with following stacktrace", new Exception("stacktrace for debugging"));
        }

        synchronized (open) {
            return open.getAndSet(false);
        }
    }

    public void close(ScannerSession bs) {
        try {
            log.debug("Closed session " + System.identityHashCode(bs));
            if (sessionInstances.remove(bs)) {
                if (log.isTraceEnabled()) {
                    log.trace("Closing instance " + bs.hashCode());
                }
                bs.close();
            }
        } catch (Exception e) {
            // ANY EXCEPTION HERE CAN SAFELY BE IGNORED
            log.trace("Exception closing ScannerSession, can be safely ignored: {}", e);
        }
    }

    public void setMaxQueue(int size) {
        this.maxQueue = size;
    }

    public ScannerBase newRfileScanner(String tableName, Set<Authorizations> auths, Query setting) {
        if (open.get()) {
            Configuration conf = new Configuration();

            Properties clientProps = client.properties();
            final String instanceName = clientProps.getProperty(ClientProperty.INSTANCE_NAME.getKey());
            final String zookeepers = clientProps.getProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey());

            AccumuloHelper.setInstanceName(conf, instanceName);
            AccumuloHelper.setUsername(conf, client.whoami());

            AccumuloHelper.setZooKeepers(conf, zookeepers);
            BulkInputFormat.setZooKeeperInstance(conf, instanceName, zookeepers);

            AccumuloHelper.setPassword(conf, config.getAccumuloPassword().getBytes());
            BulkInputFormat.setMemoryInput(conf, client.whoami(), config.getAccumuloPassword().getBytes(), tableName, auths.iterator().next());

            conf.set(MultiRfileInputformat.CACHE_METADATA, "true");

            ScannerBase baseScanner = new RfileScanner(client, conf, tableName, auths, 1);

            applyConfigs(baseScanner, tableName);

            synchronized (open) {
                if (open.get()) {
                    instances.add(baseScanner);
                    return baseScanner;
                } else {
                    baseScanner.close();
                    throw new IllegalStateException("Factory has been locked. No new scanners can be created.");
                }
            }
        } else {
            throw new IllegalStateException("Factory has been locked. No new scanners can be created.");
        }
    }

    /**
     * Apply table-specific scanner configs to the provided scanner base object
     *
     * @param scannerBase
     *            a {@link ScannerBase}
     * @param tableName
     *            the table
     */
    protected void applyConfigs(ScannerBase scannerBase, String tableName) {
        if (consistencyByTable != null && consistencyByTable.containsKey(tableName)) {
            scannerBase.setConsistencyLevel(consistencyByTable.get(tableName));
        }

        if (hintsByTable != null && hintsByTable.containsKey(tableName)) {
            scannerBase.setExecutionHints(hintsByTable.get(tableName));
        }
    }

    /**
     * Apply table-specific scanner configs to the provided scanner session
     *
     * @param scannerSession
     *            the {@link ScannerSession}
     * @param tableName
     *            the table
     */
    protected void applyConfigs(ScannerSession scannerSession, String tableName) {
        SessionOptions options = scannerSession.getOptions();

        if (consistencyByTable != null && consistencyByTable.containsKey(tableName)) {
            options.setConsistencyLevel(consistencyByTable.get(tableName));
        }

        if (hintsByTable != null && hintsByTable.containsKey(tableName)) {
            options.setExecutionHints(hintsByTable.get(tableName));
        }

        scannerSession.setOptions(options);
    }
}
