package datawave.query.scheduler;

import com.google.common.collect.Iterators;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.accumulo.inmemory.impl.InMemoryTabletLocator;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.async.ScannerChunk;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.webservice.query.configuration.QueryData;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultServiceUnavailableRetryStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link Scheduler} that delegates scan execution and management to an intermediate scan service.
 */
public class RemoteScanScheduler extends Scheduler {
    
    private final static Logger log = LoggerFactory.getLogger(RemoteScanScheduler.class);
    
    private final AtomicBoolean isSubmitting = new AtomicBoolean(false);
    private final AtomicBoolean isExecuting = new AtomicBoolean(false);
    
    private final ExecutorService submitThread = Executors.newSingleThreadExecutor();
    private final ExecutorService resultThread = Executors.newSingleThreadExecutor();
    
    public static final String SERVICE_URL = "https://localhost:8443/scan/v1/";
    
    protected BasicCookieStore cookieStore;
    protected CloseableHttpClient httpClient;
    
    protected ShardQueryConfiguration config;
    protected RemoteScanSchedulerIterator iterator;
    protected final LinkedBlockingQueue<Map.Entry<Key,Value>> results;
    
    /**
     * This scheduler requires knowledge of the query's ShardQueryConfiguration
     *
     * @param config
     *            an instance of {@link ShardQueryConfiguration}
     */
    public RemoteScanScheduler(ShardQueryConfiguration config) {
        this.config = config;
        this.results = new LinkedBlockingQueue<>();
        
        this.cookieStore = new BasicCookieStore();
        this.httpClient = setupHttpClient();
        
        execute();
    }
    
    /**
     * TODO -- extract this to it's own class
     *
     * @return a fully formed http client
     */
    protected CloseableHttpClient setupHttpClient() {
        
        Registry r = RegistryBuilder
                        .create()
                        .register("https",
                                        new SSLConnectionSocketFactory((SSLSocketFactory) SSLSocketFactory.getDefault(), null, null,
                                                        NoopHostnameVerifier.INSTANCE)).build();
        PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager(r);
        
        //  @formatter:off
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(15 * 60 * 1000)  //  15 minutes
                .setConnectionRequestTimeout(60 * 1000)   //  1 minute
                .setSocketTimeout(63 * 60 * 1000)   //  63 minutes, longer than the 60 minute query timeout
                .build();
        //  @formatter:on
        
        // do not re-use connections
        ConnectionReuseStrategy connectionReuseStrategy = (httpResponse, httpContext) -> false;
        
        // TODO -- semi random retry strategy
        ServiceUnavailableRetryStrategy retryStrategy = new DefaultServiceUnavailableRetryStrategy();
        
        //  @formatter:off
        return HttpClientBuilder.create()
                .setConnectionManager(manager)
                .useSystemProperties()
                .setDefaultRequestConfig(requestConfig)
                .setServiceUnavailableRetryStrategy(retryStrategy)
                .setConnectionReuseStrategy(connectionReuseStrategy)
                .build();
        //  @formatter:on
    }
    
    /**
     * Spin up two threads. First thread submits scan tasks to the service, the second thread calls next on the service.
     */
    private void execute() {
        isSubmitting.set(true);
        isExecuting.set(true);
        
        // setup result thread
        log.info("building result thread");
        resultThread.execute(() -> {
            log.info("result thread started");
            Thread.currentThread().setName("result thread");
            while (isSubmitting.get() && iterator.hasNext()) {
                // call to next() is a long poll, this isn't really a 'busy loop'
                results.add(next()); // TODO -- handle null/final doc key
            }
            isExecuting.set(false);
            log.info("no more results after calling next()");
        });
        
        log.info("building submit thread");
        submitThread.execute(() -> {
            log.info("submit thread started");
            Thread.currentThread().setName("submit thread");
            
            String tableName = config.getShardTableName();
            Set<Authorizations> auths = config.getAuthorizations();
            
            Instance instance = config.getConnector().getInstance();
            TabletLocator tl;
            String tableId = null;
            if (instance instanceof InMemoryInstance) {
                tl = new InMemoryTabletLocator();
                tableId = config.getTableName();
            } else {
                try {
                    tableId = Tables.getTableId(instance, tableName);
                } catch (TableNotFoundException e) {
                    throw new IllegalStateException("Could not access tableId for table name: " + tableName);
                }
                Credentials credentials = new Credentials(config.getConnector().whoami(), new PasswordToken(config.getAccumuloPassword()));
                tl = TabletLocator.getLocator(new ClientContext(instance, credentials, AccumuloConfiguration.getDefaultConfiguration()), tableId);
            }
            
            Iterator<List<ScannerChunk>> iter = Iterators.transform(config.getQueries(), new PushdownFunction(tl, config, settings, tableId));
            List<ScannerChunk> chunks;
            while (iter.hasNext()) {
                
                chunks = iter.next();
                
                for (ScannerChunk chunk : chunks) {
                    log.info("pushing scanner chunk");
                    // TODO -- implement a min/max backoff time.
                    long backoff = 1000;
                    while (!push(chunk)) {
                        try {
                            Thread.sleep(backoff);
                        } catch (InterruptedException e) {
                            log.error("error while waiting during exponential scanner backoff");
                            e.printStackTrace();
                        }
                    }
                }
            }
            
            isSubmitting.set(false);
            log.info("done submitting scan ranges");
        });
    }
    
    /**
     * Issues a next call to the scan service.
     *
     * @return the next key value pair found, or a final document key if no such value exists
     */
    protected Map.Entry<Key,Value> next() {
        // TODO -- fill in
        return null;
    }
    
    /**
     * Push scan info to the scan service
     *
     * @param chunk
     *            a scanner chunk
     * @return
     */
    protected boolean push(ScannerChunk chunk) {
        return false;
    }
    
    /**
     * Issue close to remote scan service and shutdown executor services
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        isSubmitting.set(false);
        isExecuting.set(false);
        
    }
    
    @Override
    public Iterator<Map.Entry<Key,Value>> iterator() {
        this.iterator = new RemoteScanSchedulerIterator();
        return this.iterator;
    }
    
    // default impl
    @Override
    public BatchScanner createBatchScanner(ShardQueryConfiguration config, ScannerFactory scannerFactory, QueryData qd) throws TableNotFoundException {
        return ShardQueryLogic.createBatchScanner(config, scannerFactory, qd);
    }
    
    // no-op
    @Override
    public ScanSessionStats getSchedulerStats() {
        return null;
    }
    
    /**
     * Encapsulation of {@link Iterator} operations
     */
    public class RemoteScanSchedulerIterator implements Iterator<Map.Entry<Key,Value>> {
        
        public RemoteScanSchedulerIterator() {
            
        }
        
        @Override
        public boolean hasNext() {
            return isExecuting.get() || !results.isEmpty();
        }
        
        @Override
        public Map.Entry<Key,Value> next() {
            try {
                return results.poll(60, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                log.error("Interrupted while polling result queue: " + e.getMessage());
                e.printStackTrace();
            }
            
            log.warn("Expected another result but got nothing");
            return null;
        }
    }
}
