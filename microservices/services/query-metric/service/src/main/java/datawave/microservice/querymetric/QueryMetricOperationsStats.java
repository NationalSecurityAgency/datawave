package datawave.microservice.querymetric;

import static java.util.concurrent.TimeUnit.MINUTES;

import static datawave.microservice.querymetric.config.HazelcastMetricCacheConfiguration.INCOMING_METRICS;

import java.net.InetAddress;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;

import datawave.microservice.querymetric.config.TimelyProperties;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetric.persistence.AccumuloMapStore;
import datawave.util.timely.TcpClient;
import datawave.util.timely.UdpClient;

public class QueryMetricOperationsStats {
    
    private Logger log = LoggerFactory.getLogger(getClass());
    private static final String RatePerSec_1_Min_Avg = ".RatePerSec_1_Min_Avg";
    private static final String RatePerSec_5_Min_Avg = ".RatePerSec_5_Min_Avg";
    private static final String RatePerSec_15_Min_Avg = ".RatePerSec_15_Min_Avg";
    private static final String Latency_Mean = ".Latency_Mean";
    private static final String Latency_Median = ".Latency_Median";
    private static final String Latency_Max = ".Latency_Max";
    private static final String Latency_Min = ".Latency_Min";
    private static final String Latency_75 = ".Latency_75";
    private static final String Latency_95 = ".Latency_95";
    private static final String Latency_99 = ".Latency_99";
    private static final String Latency_999 = ".Latency_999";
    private Map<TIMERS,Timer> timerMap = new HashMap<>();
    private Map<METERS,Meter> meterMap = new HashMap<>();
    private TcpClient timelyTcpClient;
    private UdpClient timelyUdpClient;
    
    protected TimelyProperties timelyProperties;
    protected ShardTableQueryMetricHandler handler;
    protected AccumuloMapStore mapStore;
    protected CacheManager cacheManager;
    protected Cache lastWrittenCache;
    protected Map<String,Long> hostCountMap = new HashMap<>();
    protected Map<String,Long> userCountMap = new HashMap<>();
    protected Map<String,Long> logicCountMap = new HashMap<>();
    protected List<String> queryStatsToWriteToTimely = Collections.synchronizedList(new ArrayList<>());
    protected Map<String,String> staticTags = new LinkedHashMap<>();
    
    public enum TIMERS {
        REST, STORE
    }
    
    public enum METERS {
        MESSAGE_RECEIVE
    }
    
    /*
     * Timer Hierarchy
     *
     * REST - receive request and send it out to the message queue MESSAGE - receive message and do all of the following CORRELATE COMBINE STORE ENTRY
     */
    
    public QueryMetricOperationsStats(TimelyProperties timelyProperties, ShardTableQueryMetricHandler handler, CacheManager cacheManager,
                    @Qualifier("lastWrittenQueryMetrics") Cache lastWrittenCache, AccumuloMapStore mapStore) {
        this.timelyProperties = timelyProperties;
        this.handler = handler;
        this.mapStore = mapStore;
        this.cacheManager = cacheManager;
        this.lastWrittenCache = lastWrittenCache;
        for (TIMERS name : TIMERS.values()) {
            this.timerMap.put(name, new Timer(new SlidingTimeWindowArrayReservoir(1, MINUTES)));
        }
        for (METERS name : METERS.values()) {
            this.meterMap.put(name, new Meter());
        }
        if (this.timelyProperties.isEnabled()) {
            try {
                if (timelyProperties.getProtocol().equals(TimelyProperties.Protocol.TCP)) {
                    this.timelyTcpClient = new TcpClient(timelyProperties.getHost(), timelyProperties.getPort());
                    this.timelyTcpClient.open();
                } else {
                    this.timelyUdpClient = new UdpClient(timelyProperties.getHost(), timelyProperties.getPort());
                    this.timelyUdpClient.open();
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        try {
            staticTags.put("host", InetAddress.getLocalHost().getCanonicalHostName());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    public Timer getTimer(TIMERS name) {
        return this.timerMap.get(name);
    }
    
    public Meter getMeter(METERS name) {
        return this.meterMap.get(name);
    }
    
    protected void addCacheStats(Map<String,Double> serviceStats) {
        Cache incomingCache = this.cacheManager.getCache(INCOMING_METRICS);
        if (incomingCache != null) {
            IMap<Object,Object> incomingQueryMetricsCache = ((IMap<Object,Object>) incomingCache.getNativeCache());
            serviceStats.put("incomingQueryMetrics.dirtyEntryCount", Double.valueOf(incomingQueryMetricsCache.getLocalMapStats().getDirtyEntryCount()));
            serviceStats.put("incomingQueryMetrics.ownedEntryCount", Double.valueOf(incomingQueryMetricsCache.getLocalMapStats().getOwnedEntryCount()));
        }
        if (this.lastWrittenCache != null) {
            long estimatedSize = ((com.github.benmanes.caffeine.cache.Cache) this.lastWrittenCache.getNativeCache()).estimatedSize();
            serviceStats.put("lastWrittenQueryMetric.estimatedSize", Double.valueOf(estimatedSize));
        }
    }
    
    public void writeServiceStatsToTimely() {
        if (this.timelyProperties.isEnabled()) {
            List<String> serviceStatsToWriteToTimely = new ArrayList<>();
            
            long timestamp = System.currentTimeMillis();
            Map<String,Double> serviceStatsDouble = getServiceStats();
            addCacheStats(serviceStatsDouble);
            Map<String,String> serviceStats = formatStats(serviceStatsDouble, false);
            serviceStats.entrySet().forEach(entry -> {
                serviceStatsToWriteToTimely
                                .add("put microservice.querymetric." + entry.getKey() + " " + timestamp + " " + entry.getValue() + getCommonTags() + "\n");
            });
            try {
                for (String metric : serviceStatsToWriteToTimely) {
                    if (this.timelyProperties.getProtocol().equals(TimelyProperties.Protocol.TCP)) {
                        this.timelyTcpClient.write(metric);
                    } else {
                        this.timelyUdpClient.write(metric);
                    }
                }
            } catch (Exception e) {
                log.error("Exception writing metrics to Timely: " + e.getMessage());
            } finally {
                if (this.timelyProperties.getProtocol().equals(TimelyProperties.Protocol.TCP)) {
                    this.timelyTcpClient.flush();
                }
            }
        }
        
    }
    
    public void writeQueryStatsToTimely() {
        if (this.timelyProperties.isEnabled()) {
            List<String> tempMetricsToWrite = new ArrayList<>();
            // add metrics to a new list so that issues with Timely don't indirectly
            // prevent the metric service from handling incoming metric updates
            synchronized (this.queryStatsToWriteToTimely) {
                tempMetricsToWrite.addAll(this.queryStatsToWriteToTimely);
                this.queryStatsToWriteToTimely.clear();
            }
            try {
                Iterator<String> itr = tempMetricsToWrite.iterator();
                while (itr.hasNext()) {
                    String metric = itr.next();
                    if (this.timelyProperties.getProtocol().equals(TimelyProperties.Protocol.TCP)) {
                        this.timelyTcpClient.write(metric);
                    } else {
                        this.timelyUdpClient.write(metric);
                    }
                    // remove metric if write is successful
                    itr.remove();
                }
            } catch (Exception e) {
                log.error("Exception writing metrics to Timely: " + e.getMessage());
                // add back metrics so that we keep trying to write them
                this.queryStatsToWriteToTimely.addAll(tempMetricsToWrite);
                if (this.queryStatsToWriteToTimely.size() > 10000) {
                    this.queryStatsToWriteToTimely.clear();
                }
            } finally {
                if (this.timelyProperties.getProtocol().equals(TimelyProperties.Protocol.TCP)) {
                    this.timelyTcpClient.flush();
                }
            }
        }
    }
    
    public Map<String,String> getLocalMapStats(LocalMapStats localMapStats) {
        Map<String,String> stats = new LinkedHashMap<>();
        DecimalFormat nFormat = new DecimalFormat("#,##0");
        stats.put("creationTime", formatDate(localMapStats.getCreationTime()));
        stats.put("lastUpdateTime", formatDate(localMapStats.getLastUpdateTime()));
        stats.put("lastAccessTime", formatDate(localMapStats.getLastAccessTime()));
        stats.put("putOperationCount", nFormat.format(localMapStats.getPutOperationCount()));
        stats.put("eventOperationCount", nFormat.format(localMapStats.getEventOperationCount()));
        stats.put("otherOperationCount", nFormat.format(localMapStats.getOtherOperationCount()));
        stats.put("getOperationCount", nFormat.format(localMapStats.getGetOperationCount()));
        stats.put("ownedEntryCount", nFormat.format(localMapStats.getOwnedEntryCount()));
        stats.put("backupCount", nFormat.format(localMapStats.getBackupCount()));
        stats.put("backupEntryCount", nFormat.format(localMapStats.getBackupEntryCount()));
        stats.put("dirtyEntryCount", nFormat.format(localMapStats.getDirtyEntryCount()));
        return stats;
    }
    
    private String formatDate(long milliseconds) {
        if (milliseconds <= 0) {
            return "";
        } else {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
            return sdf.format(new Date(milliseconds));
        }
    }
    
    public Map<String,String> formatStats(Map<String,Double> stats, boolean useSeparators) {
        DecimalFormat dFormat = useSeparators ? new DecimalFormat("#,##0.00") : new DecimalFormat("#0.00");
        DecimalFormat nFormat = useSeparators ? new DecimalFormat("#,##0") : new DecimalFormat("#0");
        Map<String,String> formattedStats = new LinkedHashMap<>();
        stats.entrySet().forEach(e -> {
            if (e.getKey().contains("Latency")) {
                formattedStats.put(e.getKey(), nFormat.format(e.getValue()));
            } else {
                formattedStats.put(e.getKey(), dFormat.format(e.getValue()));
            }
        });
        return formattedStats;
    }
    
    public Map<String,Double> getServiceStats() {
        Map<String,Double> stats = new LinkedHashMap<>();
        addTimerStats("store", getTimer(TIMERS.STORE), stats);
        addTimerStats("accumulo.write", this.mapStore.getWriteTimer(), stats);
        addTimerStats("accumulo.read", this.mapStore.getReadTimer(), stats);
        addTimerStats("rest", getTimer(TIMERS.REST), stats);
        addMeterStats("message.receive", getMeter(METERS.MESSAGE_RECEIVE), stats);
        return stats;
    }
    
    public void queueTimelyMetrics(QueryMetricUpdate update) {
        queueTimelyMetrics(update.getMetric());
    }
    
    public void queueTimelyMetrics(BaseQueryMetric queryMetric) {
        if (this.timelyProperties.isEnabled()) {
            String queryType = queryMetric.getQueryType();
            if (queryType != null && queryType.equalsIgnoreCase("RunningQuery")) {
                BaseQueryMetric.Lifecycle lifecycle = queryMetric.getLifecycle();
                String host = queryMetric.getHost();
                String user = queryMetric.getUser();
                String logic = queryMetric.getQueryLogic();
                if (lifecycle.equals(BaseQueryMetric.Lifecycle.CLOSED) || lifecycle.equals(BaseQueryMetric.Lifecycle.CANCELLED)) {
                    long createDate = queryMetric.getCreateDate().getTime();
                    // write ELAPSED_TIME
                    this.queryStatsToWriteToTimely.add("put dw.query.metrics.ELAPSED_TIME " + createDate + " " + queryMetric.getElapsedTime() + " HOST=" + host
                                    + getCommonTags() + "\n");
                    this.queryStatsToWriteToTimely.add("put dw.query.metrics.ELAPSED_TIME " + createDate + " " + queryMetric.getElapsedTime() + " USER=" + user
                                    + getCommonTags() + "\n");
                    this.queryStatsToWriteToTimely.add("put dw.query.metrics.ELAPSED_TIME " + createDate + " " + queryMetric.getElapsedTime() + " QUERY_LOGIC="
                                    + logic + getCommonTags() + "\n");
                    
                    // write NUM_RESULTS
                    this.queryStatsToWriteToTimely.add("put dw.query.metrics.NUM_RESULTS " + createDate + " " + queryMetric.getNumResults() + " HOST=" + host
                                    + getCommonTags() + "\n");
                    this.queryStatsToWriteToTimely.add("put dw.query.metrics.NUM_RESULTS " + createDate + " " + queryMetric.getNumResults() + " USER=" + user
                                    + getCommonTags() + "\n");
                    this.queryStatsToWriteToTimely.add("put dw.query.metrics.NUM_RESULTS " + createDate + " " + queryMetric.getNumResults() + " QUERY_LOGIC="
                                    + logic + getCommonTags() + "\n");
                } else if (lifecycle.equals(BaseQueryMetric.Lifecycle.INITIALIZED)) {
                    // aggregate these metrics for later writing to timely
                    synchronized (this.hostCountMap) {
                        Long hostCount = this.hostCountMap.get(host);
                        this.hostCountMap.put(host, hostCount == null ? 1l : hostCount + 1);
                        Long userCount = this.userCountMap.get(user);
                        this.userCountMap.put(user, userCount == null ? 1l : userCount + 1);
                        Long logicCount = this.logicCountMap.get(logic);
                        this.logicCountMap.put(logic, logicCount == null ? 1l : logicCount + 1);
                    }
                }
            }
        }
    }
    
    protected String getCommonTags() {
        Map<String,String> tags = new LinkedHashMap<>();
        tags.putAll(this.timelyProperties.getTags());
        tags.putAll(this.staticTags);
        if (tags != null && !tags.isEmpty()) {
            return " " + tags.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(" "));
        } else {
            return "";
        }
    }
    
    public void logServiceStats() {
        Map<String,Double> stats = getServiceStats();
        Map<String,String> serviceStats = formatStats(stats, true);
        String storeRate1 = serviceStats.get("store" + RatePerSec_1_Min_Avg);
        String storeRate5 = serviceStats.get("store" + RatePerSec_5_Min_Avg);
        String storeRate15 = serviceStats.get("store" + RatePerSec_15_Min_Avg);
        String storeLat = serviceStats.get("store" + Latency_Mean);
        log.info("storeMetric rates/sec 1m={} 5m={} 15m={} opLatMs={}", storeRate1, storeRate5, storeRate15, storeLat);
        String accumuloRate1 = serviceStats.get("accumulo.write" + RatePerSec_1_Min_Avg);
        String accumuloRate5 = serviceStats.get("accumulo.write" + RatePerSec_5_Min_Avg);
        String accumuloRate15 = serviceStats.get("accumulo.write" + RatePerSec_15_Min_Avg);
        String accumuloLat = serviceStats.get("accumulo.write" + Latency_Mean);
        log.info("accumulo rates/sec 1m={} 5m={} 15m={} opLatMs={}", accumuloRate1, accumuloRate5, accumuloRate15, accumuloLat);
    }
    
    public void queueAggregatedQueryStatsForTimely() {
        if (this.timelyProperties.isEnabled()) {
            long now = System.currentTimeMillis();
            synchronized (hostCountMap) {
                this.hostCountMap.entrySet().forEach(entry -> {
                    this.queryStatsToWriteToTimely
                                    .add("put dw.query.metrics.COUNT " + now + " " + entry.getValue() + " HOST=" + entry.getKey() + getCommonTags() + "\n");
                });
                this.hostCountMap.clear();
                this.userCountMap.entrySet().forEach(entry -> {
                    this.queryStatsToWriteToTimely
                                    .add("put dw.query.metrics.COUNT " + now + " " + entry.getValue() + " USER=" + entry.getKey() + getCommonTags() + "\n");
                });
                this.userCountMap.clear();
                this.logicCountMap.entrySet().forEach(entry -> {
                    this.queryStatsToWriteToTimely.add(
                                    "put dw.query.metrics.COUNT " + now + " " + entry.getValue() + " QUERY_LOGIC=" + entry.getKey() + getCommonTags() + "\n");
                });
                this.logicCountMap.clear();
            }
        }
    }
    
    private void addTimerStats(String baseName, Timer timer, Map<String,Double> stats) {
        Snapshot snapshot = timer.getSnapshot();
        stats.put(baseName + Latency_Mean, snapshot.getMean() / 1000000);
        stats.put(baseName + Latency_Median, snapshot.getMedian() / 1000000);
        stats.put(baseName + Latency_Max, ((Number) snapshot.getMax()).doubleValue() / 1000000);
        stats.put(baseName + Latency_Min, ((Number) snapshot.getMin()).doubleValue() / 1000000);
        stats.put(baseName + Latency_75, snapshot.get75thPercentile() / 1000000);
        stats.put(baseName + Latency_95, snapshot.get95thPercentile() / 1000000);
        stats.put(baseName + Latency_99, snapshot.get99thPercentile() / 1000000);
        stats.put(baseName + Latency_999, snapshot.get999thPercentile() / 1000000);
        stats.put(baseName + RatePerSec_1_Min_Avg, timer.getOneMinuteRate());
        stats.put(baseName + RatePerSec_5_Min_Avg, timer.getFiveMinuteRate());
        stats.put(baseName + RatePerSec_15_Min_Avg, timer.getFifteenMinuteRate());
    }
    
    private void addMeterStats(String baseName, Metered meter, Map<String,Double> stats) {
        stats.put(baseName + RatePerSec_1_Min_Avg, meter.getOneMinuteRate());
        stats.put(baseName + RatePerSec_5_Min_Avg, meter.getFiveMinuteRate());
        stats.put(baseName + RatePerSec_15_Min_Avg, meter.getFifteenMinuteRate());
    }
}
