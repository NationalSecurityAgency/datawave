package datawave.query.tables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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

    protected int maxQueue = 1000;
    protected final Set<ScannerBase> instances = Collections.synchronizedSet(new HashSet<>());
    protected final Set<ScannerSession> sessionInstances = Collections.synchronizedSet(new HashSet<>());
    protected final AccumuloClient cxn;
    // using an AtomicBoolean to give us a separate monitor for synchronization
    protected final AtomicBoolean open = new AtomicBoolean(true);

    protected boolean accrueStats;
    protected Query settings;
    protected ResourceQueue scanQueue;
    ShardQueryConfiguration config;

    private static final Logger log = Logger.getLogger(ScannerFactory.class);

    public ScannerFactory(GenericQueryConfiguration queryConfiguration) {

        this.cxn = queryConfiguration.getClient();
        log.debug("Created scanner factory " + System.identityHashCode(this) + " is wrapped ? " + (cxn instanceof WrappedConnector));

        if (queryConfiguration instanceof ShardQueryConfiguration) {
            this.config = ((ShardQueryConfiguration) queryConfiguration);
            this.maxQueue = this.config.getMaxScannerBatchSize();
            this.settings = this.config.getQuery();
            this.accrueStats = this.config.getAccrueStats();
            try {
                scanQueue = new ResourceQueue(this.config.getNumQueryThreads(), this.cxn);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            this.config = null;
            this.settings = null;
            this.accrueStats = false;
            this.scanQueue = null;
        }
    }

    public ScannerFactory(AccumuloClient client) {
        this(client, 100);

    }

    public ScannerFactory(AccumuloClient client, int queueSize) {
        this.config = null;
        this.settings = null;
        this.accrueStats = false;
        try {
            this.cxn = client;
            this.scanQueue = new ResourceQueue(queueSize, client);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Scanner newSingleScanner(String tableName, Set<Authorizations> auths, Query query) throws TableNotFoundException {
        if (open.get()) {
            Scanner bs = QueryScannerHelper.createScannerWithoutInfo(cxn, tableName, auths, query);
            log.debug("Created scanner " + System.identityHashCode(bs));
            if (log.isTraceEnabled()) {
                log.trace("Adding instance " + bs.hashCode());
            }

            synchronized (open) {
                if (open.get()) {
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
            BatchScanner bs = QueryScannerHelper.createBatchScanner(cxn, tableName, auths, threads, query);
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
            BatchScanner bs = QueryScannerHelper.createBatchScanner(cxn, tableName, auths, threads, query, reportErrors);
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

        T session = null;
        if (wrapper == ScannerSession.class) {
            session = (T) new ScannerSession(tableName, auths, scanQueue, maxQueue, settings).applyStats(stats);
        } else {
            session = wrapper.getConstructor(ScannerSession.class)
                            .newInstance(new ScannerSession(tableName, auths, scanQueue, maxQueue, settings).applyStats(stats));
        }

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
        return newLimitedScanner(RangeStreamScanner.class, tableName, auths, settings).setShardsPerDayThreshold(shardsPerDayThreshold).setScannerFactory(this);
    }

    public boolean close(ScannerBase bs) {
        boolean removed = false;
        try {
            log.debug("Closed scanner " + System.identityHashCode(bs));
            removed = instances.remove(bs);
            if (log.isTraceEnabled()) {
                log.trace("Closing instance " + bs.hashCode());
            }
            bs.close();
        } catch (Exception e) {
            // ANY EXCEPTION HERE CAN SAFELY BE IGNORED
            log.trace("Exception closing ScannerBase, can be safely ignored: {}", e);
        }
        return removed;
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
            sessionInstances.remove(bs);
            if (log.isTraceEnabled()) {
                log.trace("Closing instance " + bs.hashCode());
            }
            bs.close();
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

            AccumuloClient con = cxn;

            Properties clientProps = con.properties();
            final String instanceName = clientProps.getProperty(ClientProperty.INSTANCE_NAME.getKey());
            final String zookeepers = clientProps.getProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey());

            AccumuloHelper.setInstanceName(conf, instanceName);
            AccumuloHelper.setUsername(conf, con.whoami());

            AccumuloHelper.setZooKeepers(conf, zookeepers);
            BulkInputFormat.setZooKeeperInstance(conf, instanceName, zookeepers);

            AccumuloHelper.setPassword(conf, config.getAccumuloPassword().getBytes());
            BulkInputFormat.setMemoryInput(conf, con.whoami(), config.getAccumuloPassword().getBytes(), tableName, auths.iterator().next());

            conf.set(MultiRfileInputformat.CACHE_METADATA, "true");

            ScannerBase baseScanner = new RfileScanner(con, conf, tableName, auths, 1);

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
}
