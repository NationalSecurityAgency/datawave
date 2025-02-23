package datawave.microservice.querymetric.persistence;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.Cache;
import org.springframework.stereotype.Component;

import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapStore;
import com.hazelcast.map.MapStoreFactory;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricType;
import datawave.microservice.querymetric.QueryMetricUpdate;
import datawave.microservice.querymetric.QueryMetricUpdateHolder;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;

@Component("store")
@ConditionalOnProperty(name = "hazelcast.server.enabled")
public class AccumuloMapStore<T extends BaseQueryMetric> extends AccumuloMapLoader<T> implements MapStore<String,QueryMetricUpdateHolder<T>> {
    // A list of fields that won't change once written. There is no need to retrieve these fields from Accumulo
    // Any change here must be accounted for in ContentQueryMetricIngestHelper.
    public static final List<String> ignoreFieldsOnQuery = Arrays.asList("POSITIVE_SELECTORS", "NEGATIVE_SELECTORS", "AUTHORIZATIONS", "BEGIN_DATE", "END_DATE",
                    "PARAMETERS", "PROXY_SERVERS", "PREDICTION", "QUERY", "QUERY_LOGIC", "QUERY_NAME", "QUERY_TYPE", "USER", "USER_DN", "VERSION",
                    "PAGE_METRICS");
    // Exclude PREDICTION, PAGE_METRICS which we don't want to pull from Accumulo but which can change after query creation
    public static final List<String> ignoreFieldsOnWrite = new ArrayList<>();
    
    static {
        AccumuloMapStore.ignoreFieldsOnWrite.addAll(ignoreFieldsOnQuery);
        AccumuloMapStore.ignoreFieldsOnWrite.removeAll(Arrays.asList("PREDICTION", "PAGE_METRICS"));
    }
    
    private static AccumuloMapStore instance;
    private final Logger log = LoggerFactory.getLogger(AccumuloMapStore.class);
    private Cache lastWrittenQueryMetricCache;
    private final com.github.benmanes.caffeine.cache.Cache failures;
    private final Timer writeTimer = new Timer(new SlidingTimeWindowArrayReservoir(1, MINUTES));
    private final Timer readTimer = new Timer(new SlidingTimeWindowArrayReservoir(1, MINUTES));
    private boolean shuttingDown = false;
    
    public static class Factory implements MapStoreFactory<String,BaseQueryMetric> {
        @Override
        public MapLoader<String,BaseQueryMetric> newMapStore(String mapName, Properties properties) {
            return AccumuloMapStore.instance;
        }
    }
    
    @Autowired
    public AccumuloMapStore(ShardTableQueryMetricHandler handler) {
        this.handler = handler;
        // @formatter:off
        this.failures = Caffeine.newBuilder()
                .expireAfterWrite(60, TimeUnit.MINUTES)
                .build();
        // @formatter:on
        AccumuloMapStore.instance = this;
    }
    
    @PreDestroy
    public void shutdown() {
        this.shuttingDown = true;
        // ensure that queued updates written to the handler's
        // MultiTabletBatchWriter are flushed to Accumulo on shutdown
        try {
            this.handler.shutdown();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    public void setLastWrittenQueryMetricCache(Cache lastWrittenQueryMetricCache) {
        this.lastWrittenQueryMetricCache = lastWrittenQueryMetricCache;
    }
    
    @Override
    public void store(String queryId, QueryMetricUpdateHolder<T> queryMetricUpdate) {
        Timer.Context writeTimerContext = writeTimer.time();
        try {
            storeWithRetry(queryMetricUpdate);
        } finally {
            writeTimerContext.stop();
        }
    }
    
    public void storeWithRetry(QueryMetricUpdateHolder<T> queryMetricUpdate) {
        boolean retry = true;
        boolean success = false;
        while (!this.shuttingDown && !success && retry) {
            try {
                store(queryMetricUpdate);
                success = true;
            } catch (Exception e) {
                if (!this.shuttingDown) {
                    retry = AccumuloMapStore.this.retryOnException(queryMetricUpdate, e);
                    if (retry) {
                        log.info("retrying store of {}", queryMetricUpdate.getMetric().getQueryId());
                    }
                }
            }
        }
    }
    
    public void store(QueryMetricUpdateHolder<T> queryMetricUpdate) throws Exception {
        
        String queryId = queryMetricUpdate.getMetric().getQueryId();
        T updatedMetric = null;
        try {
            updatedMetric = (T) queryMetricUpdate.getMetric().duplicate();
            QueryMetricType metricType = queryMetricUpdate.getMetricType();
            QueryMetricUpdateHolder<T> lastQueryMetricUpdate = null;
            
            final List<String> ignoredFields = new ArrayList<>();
            if (!queryMetricUpdate.isNewMetric()) {
                lastQueryMetricUpdate = lastWrittenQueryMetricCache.get(queryId, QueryMetricUpdateHolder.class);
                if (lastQueryMetricUpdate == null) {
                    log.debug("getting metric {} from accumulo", queryId);
                    Timer.Context readTimerContext = readTimer.time();
                    try {
                        T m = handler.getQueryMetric(queryId, ignoreFieldsOnQuery);
                        if (m != null) {
                            // these fields will not be populated in the returned metric,
                            // so we should not compare them later for writing mutations
                            ignoredFields.addAll(ignoreFieldsOnWrite);
                            lastQueryMetricUpdate = new QueryMetricUpdateHolder<>(m, metricType);
                        }
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    } finally {
                        readTimerContext.stop();
                    }
                }
            }
            
            if (lastQueryMetricUpdate != null) {
                T lastQueryMetric = lastQueryMetricUpdate.getMetric();
                if (metricType.equals(QueryMetricType.DISTRIBUTED)) {
                    // these values are added incrementally in a distributed update. Because we can not be sure
                    // exactly when the incomingQueryMetricCache value is stored, it would otherwise be possible
                    // for updates to be included twice.
                    updatedMetric.setSourceCount(queryMetricUpdate.getValue("sourceCount"));
                    updatedMetric.setNextCount(queryMetricUpdate.getValue("nextCount"));
                    updatedMetric.setSeekCount(queryMetricUpdate.getValue("seekCount"));
                    updatedMetric.setYieldCount(queryMetricUpdate.getValue("yieldCount"));
                    updatedMetric.setDocSize(queryMetricUpdate.getValue("docSize"));
                    updatedMetric.setDocRanges(queryMetricUpdate.getValue("docRanges"));
                    updatedMetric.setFiRanges(queryMetricUpdate.getValue("fiRanges"));
                }
                
                updatedMetric = handler.combineMetrics(updatedMetric, lastQueryMetric, metricType);
                long numUpdates = updatedMetric.getNumUpdates();
                // The createDate shouldn't change once it is set, so this is just insurance
                // We use the higher timestamp to ensure that the deletes and successive writes persist
                // As long as this timestamp is greater than when it was written, then the delete will be effective
                long deleteTimestamp;
                if (lastQueryMetric.getCreateDate().after(updatedMetric.getCreateDate())) {
                    deleteTimestamp = updatedMetric.getCreateDate().getTime() + numUpdates;
                } else {
                    deleteTimestamp = lastQueryMetric.getCreateDate().getTime() + numUpdates;
                }
                long writeTimestamp = deleteTimestamp + 1;
                updatedMetric.setNumUpdates(numUpdates + 1);
                
                if (lastQueryMetric.getLastUpdated() != null) {
                    handler.writeMetric(updatedMetric, Collections.singletonList(lastQueryMetric), deleteTimestamp, true, ignoredFields);
                }
                handler.writeMetric(updatedMetric, Collections.singletonList(lastQueryMetric), writeTimestamp, false, ignoredFields);
            } else {
                handler.writeMetric(updatedMetric, Collections.emptyList(), updatedMetric.getCreateDate().getTime(), false, ignoredFields);
            }
            if (log.isTraceEnabled()) {
                log.trace("writing metric to accumulo: {} - {}", queryId, queryMetricUpdate.getMetric());
            } else {
                log.debug("writing metric to accumulo: {}", queryId);
            }
            
            lastWrittenQueryMetricCache.put(queryId, new QueryMetricUpdateHolder<>(updatedMetric));
            queryMetricUpdate.setPersisted();
            failures.invalidate(queryId);
        } finally {
            if (queryMetricUpdate.getMetricType().equals(QueryMetricType.DISTRIBUTED)) {
                // we've added the accumulated updates, so they can be reset
                if (updatedMetric != null) {
                    // this ensures that the incomingQueryMetricsCache has the latest values
                    queryMetricUpdate.getMetric().setSourceCount(updatedMetric.getSourceCount());
                    queryMetricUpdate.getMetric().setNextCount(updatedMetric.getNextCount());
                    queryMetricUpdate.getMetric().setSeekCount(updatedMetric.getSeekCount());
                    queryMetricUpdate.getMetric().setYieldCount(updatedMetric.getYieldCount());
                    queryMetricUpdate.getMetric().setDocSize(updatedMetric.getDocSize());
                    queryMetricUpdate.getMetric().setDocRanges(updatedMetric.getDocRanges());
                    queryMetricUpdate.getMetric().setFiRanges(updatedMetric.getFiRanges());
                }
            }
        }
    }
    
    private boolean retryOnException(QueryMetricUpdate update, Exception e) {
        String queryId = update.getMetric().getQueryId();
        int numFailures = 1;
        try {
            numFailures = (Integer) this.failures.get(queryId, o -> 0) + 1;
        } catch (Exception e1) {
            log.error(e1.getMessage(), e1);
        }
        if (numFailures < 3) {
            // track the number of failures and throw an exception
            // so that we will continue trying to write the failed entry
            this.failures.put(queryId, numFailures);
            return true;
        } else {
            // stop trying by not propagating the exception
            log.error("writing metric to accumulo: {} failed 3 times, will stop trying: {}", queryId, update.getMetric(), e);
            this.failures.invalidate(queryId);
            return false;
        }
    }
    
    @Override
    public void storeAll(Map<String,QueryMetricUpdateHolder<T>> map) {
        Iterator<Map.Entry<String,QueryMetricUpdateHolder<T>>> itr = map.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String,QueryMetricUpdateHolder<T>> entry = itr.next();
            try {
                this.store(entry.getKey(), entry.getValue());
                // remove entries that succeeded so that a potential
                // subsequent failure will know which updates remain
                itr.remove();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
    }
    
    @Override
    public QueryMetricUpdateHolder load(String s) {
        return null;
    }
    
    @Override
    public Map<String,QueryMetricUpdateHolder<T>> loadAll(Collection<String> keys) {
        return null;
    }
    
    @Override
    public void delete(String key) {
        // not implemented
    }
    
    @Override
    public void deleteAll(Collection<String> keys) {
        // not implemented
    }
    
    public Timer getWriteTimer() {
        return writeTimer;
    }
    
    public Timer getReadTimer() {
        return readTimer;
    }
}
