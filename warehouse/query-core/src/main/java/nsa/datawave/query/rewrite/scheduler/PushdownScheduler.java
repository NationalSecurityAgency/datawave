package nsa.datawave.query.rewrite.scheduler;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.mock.impl.MockTabletLocator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import nsa.datawave.mr.bulk.RfileResource;
import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.rewrite.tables.RefactoredShardQueryLogic;
import nsa.datawave.query.tables.BatchScannerSession;
import nsa.datawave.query.tables.ScannerFactory;
import nsa.datawave.query.tables.async.ScannerChunk;
import nsa.datawave.query.tables.async.event.VisitorFunction;
import nsa.datawave.query.tables.stats.ScanSessionStats;
import nsa.datawave.query.util.MetadataHelper;
import nsa.datawave.query.util.MetadataHelperFactory;
import nsa.datawave.webservice.common.logging.ThreadConfigurableLogger;
import nsa.datawave.webservice.query.configuration.QueryData;

/**
 * Purpose: Pushes down individual queries to the Tservers. Is aware that each server may have a different query, thus bins ranges per tserver and keeps the
 * plan that corresponds with those queries
 */
public class PushdownScheduler extends Scheduler {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(PushdownScheduler.class);
    
    /**
     * Configuration reference.
     */
    protected final RefactoredShardQueryConfiguration config;
    /**
     * Scanner factory reference.
     */
    protected final ScannerFactory scannerFactory;
    /**
     * Count for the number of QueryPlans that we have
     */
    protected final AtomicInteger count = new AtomicInteger(0);
    /**
     * BatchScannerSession reference, so that we can close upon completion or error
     */
    protected BatchScannerSession session = null;
    
    protected Iterator<Entry<Key,Value>> currentIterator = null;
    
    protected List<Function<IteratorSetting,IteratorSetting>> customizedFunctionList;
    
    /**
     * Local instance of the table ID
     */
    protected String tableId;
    
    protected MetadataHelper metadataHelper;
    
    protected PushdownScheduler(RefactoredShardQueryConfiguration config, ScannerFactory scannerFactory) {
        this.config = config;
        this.scannerFactory = scannerFactory;
        customizedFunctionList = Lists.newArrayList();
        metadataHelper = MetadataHelper.getInstance(config.getConnector(), config.getMetadataTableName(), config.getAuthorizations());
        Preconditions.checkNotNull(config.getConnector());
    }
    
    public PushdownScheduler(RefactoredShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelperFactory metaFactory) {
        this(config, scannerFactory, metaFactory.createMetadataHelper());
    }
    
    protected PushdownScheduler(RefactoredShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper) {
        this.config = config;
        this.metadataHelper = helper.initialize(config.getConnector(), config.getMetadataTableName(), config.getAuthorizations());
        this.scannerFactory = scannerFactory;
        customizedFunctionList = Lists.newArrayList();
        Preconditions.checkNotNull(config.getConnector());
    }
    
    public void addSetting(IteratorSetting customSetting) {
        settings.add(customSetting);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<Entry<Key,Value>> iterator() {
        if (null == this.config) {
            throw new IllegalArgumentException("Null configuration provided");
        }
        
        try {
            Iterator<Entry<Key,Value>> iter = concatIterators();
            
            return iter;
        } catch (AccumuloException | ParseException | TableNotFoundException | AccumuloSecurityException e) {
            throw new RuntimeException(e);
        }
        
    }
    
    /**
     * @return
     * @throws ParseException
     * @throws TableNotFoundException
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     */
    protected Iterator<Entry<Key,Value>> concatIterators() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, ParseException {
        
        String tableName = config.getShardTableName();
        
        Set<Authorizations> auths = config.getAuthorizations();
        
        TabletLocator tl = null;
        
        if (config.getConnector().getInstance() instanceof MockInstance) {
            tl = new MockTabletLocator();
            tableId = config.getTableName();
        } else {
            tableId = Tables.getTableId(config.getConnector().getInstance(), tableName);
            tl = TabletLocator.getLocator(config.getConnector().getInstance(), new Text(tableId));
        }
        Iterator<List<ScannerChunk>> chunkIter = Iterators.transform(getQueryDataIterator(), new PushdownFunction(tl, config, settings, tableId));
        
        try {
            session = scannerFactory.newQueryScanner(tableName, auths, config.getQuery());
            
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
        
        session.setTabletLocator(tl);
        
        session.updateIdentifier(config.getQuery().getId().toString());
        
        return session;
    }
    
    protected Iterator<QueryData> getQueryDataIterator() {
        return config.getQueries();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        if (session != null)
            scannerFactory.close(session);
        
        log.debug("Ran " + count.get() + " queries for a single user query");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.query.rewrite.scheduler.Scheduler#createBatchScanner(nsa .datawave.query.rewrite.config.RefactoredShardQueryConfiguration,
     * nsa.datawave.query.tables.ScannerFactory, nsa.datawave.webservice.query.configuration.QueryData)
     */
    @Override
    public BatchScanner createBatchScanner(RefactoredShardQueryConfiguration config, ScannerFactory scannerFactory, QueryData qd) throws TableNotFoundException {
        return RefactoredShardQueryLogic.createBatchScanner(config, scannerFactory, qd);
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
