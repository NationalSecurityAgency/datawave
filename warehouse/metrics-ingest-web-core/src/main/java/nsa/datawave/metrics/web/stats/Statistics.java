package nsa.datawave.metrics.web.stats;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonArray;
import nsa.datawave.metrics.web.CloudContext;
import nsa.datawave.metrics.web.MetricsServlet;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.master.thrift.Compacting;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.log4j.Logger;
import org.javatuples.Quartet;

import javax.inject.Inject;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Hacked together statistics class. need to be cleaned up.
 * 
 */
@WebServlet("services/statistics")
public class Statistics extends MetricsServlet {
    
    @Inject
    @ConfigProperty(name = "dw.warehouse.accumulo.userName")
    private String warehouseUsername;
    @Inject
    @ConfigProperty(name = "dw.warehouse.accumulo.password")
    private String warehousePassword;
    @Inject
    @ConfigProperty(name = "dw.warehouse.instanceName")
    private String warehouseInstanceName;
    @Inject
    @ConfigProperty(name = "dw.warehouse.zookeepers")
    private String warehouseZookeepers;
    
    protected static final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    
    public static final LoadingCache<Quartet<String,String,String,String>,MasterMonitorInfo> masterCache = CacheBuilder.newBuilder()
                    .refreshAfterWrite(10, TimeUnit.SECONDS).maximumSize(10).concurrencyLevel(10).build(new MasterCacheReloader());
    
    private static final long serialVersionUID = -1777153867026399186L;
    
    private static final Logger log = Logger.getLogger(Statistics.class);
    
    @Override
    public void doGet(CloudContext ctx, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        
        String type = req.getParameter("type");
        
        resp.setContentType("application/json");
        
        Statistic stat = MetricsStats.newInstance(type, ctx);
        
        if (null != stat) {
            try {
                // get the stats from the cache
                Quartet<String,String,String,String> key = new Quartet<>(warehouseInstanceName, warehouseZookeepers, warehouseUsername, warehousePassword);
                MasterMonitorInfo stats = masterCache.get(key);
                
                stat.setMasterStats(stats);
            } catch (ExecutionException e) {
                throw new IOException(e);
            }
            
            com.google.gson.JsonElement element = stat.toJson(req);
            
            resp.getWriter().write(element.toString());
            
        } else {
            // can't produce the stat
            JsonArray retVal = new JsonArray();
            resp.getWriter().write(retVal.toString());
        }
        
    }
    
    public static TableInfo summarizeTableStats(TabletServerStatus status) {
        TableInfo summary = new TableInfo();
        summary.majors = new Compacting();
        summary.minors = new Compacting();
        for (TableInfo rates : status.tableMap.values()) {
            add(summary, rates);
        }
        return summary;
    }
    
    public static void add(TableInfo total, TableInfo more) {
        if (total.minors == null)
            total.minors = new Compacting();
        if (total.majors == null)
            total.majors = new Compacting();
        if (more.minors != null) {
            total.minors.running += more.minors.running;
            total.minors.queued += more.minors.queued;
        }
        if (more.majors != null) {
            total.majors.running += more.majors.running;
            total.majors.queued += more.majors.queued;
        }
        total.onlineTablets += more.onlineTablets;
        total.recs += more.recs;
        total.recsInMemory += more.recsInMemory;
        total.tablets += more.tablets;
        total.ingestRate += more.ingestRate;
        total.ingestByteRate += more.ingestByteRate;
        total.queryRate += more.queryRate;
        total.queryByteRate += more.queryByteRate;
    }
    
    public static Map<String,Double> summarizeTableStats(MasterMonitorInfo mmi) {
        Map<String,Double> compactingByTable = new HashMap<>();
        if (mmi != null && mmi.tServerInfo != null) {
            for (TabletServerStatus status : mmi.tServerInfo) {
                if (status != null && status.tableMap != null) {
                    for (String table : status.tableMap.keySet()) {
                        Double holdTime = compactingByTable.get(table);
                        compactingByTable.put(table, Math.max(holdTime == null ? 0. : holdTime, status.holdTime));
                    }
                }
            }
        }
        return compactingByTable;
    }
    
    /**
     * Returns the master client.
     * 
     * @return
     */
    
    public static class MasterCacheReloader extends CacheLoader<Quartet<String,String,String,String>,MasterMonitorInfo> {
        
        /*
         * (non-Javadoc)
         * 
         * @see com.google.common.cache.CacheLoader#load(java.lang.Object)
         */
        @Override
        public MasterMonitorInfo load(Quartet<String,String,String,String> key) throws Exception {
            
            MasterCallable callable = new MasterCallable(key);
            
            return callable.call();
            
        }
        
        public ListenableFuture<MasterMonitorInfo> reload(final Quartet<String,String,String,String> key, MasterMonitorInfo oldValue) throws Exception {
            
            if (log.isTraceEnabled()) {
                log.trace("Calling re-load");
            }
            
            ListenableFutureTask<MasterMonitorInfo> task = ListenableFutureTask.create(new MasterCallable(key));
            
            executorService.execute(task);
            
            return task;
        }
        
    }
    
    /**
     * Callable that reloads the master client and returns the MasterMonitorInfo
     */
    public static class MasterCallable implements Callable<MasterMonitorInfo> {
        
        private String warehouseInstance;
        private String warehouseZookeepers;
        private String warehouseUsername;
        private String warehousePassword;
        
        public MasterCallable(Quartet<String,String,String,String> key) {
            this.warehouseInstance = key.getValue0();
            this.warehouseZookeepers = key.getValue1();
            this.warehouseUsername = key.getValue2();
            this.warehousePassword = key.getValue3();
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see java.util.concurrent.Callable#call()
         */
        @Override
        public MasterMonitorInfo call() throws Exception {
            if (log.isTraceEnabled()) {
                log.trace("GetMasterStats");
            }
            return getMasterStats();
        }
        
        public MasterMonitorInfo getMasterStats() {
            MasterMonitorInfo mmi;
            MasterClientService.Iface client = null;
            
            ZooKeeperInstance zk = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(warehouseInstance).withZkHosts(warehouseZookeepers));
            try {
                client = MasterClient.getConnection(zk);
                
                TCredentials credentials = new Credentials(warehouseUsername, new PasswordToken(warehousePassword)).toThrift(zk);
                mmi = client.getMasterStats(null, credentials);
                
                return mmi;
            } catch (Exception e) {
                e.printStackTrace();
                log.info("Error fetching stats during reload: " + e);
            } finally {
                if (client != null) {
                    MasterClient.close(client);
                }
            }
            
            return null;
        }
        
    }
    
}
