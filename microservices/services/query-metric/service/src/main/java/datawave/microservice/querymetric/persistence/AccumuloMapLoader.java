package datawave.microservice.querymetric.persistence;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapStoreFactory;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricUpdate;
import datawave.microservice.querymetric.QueryMetricUpdateHolder;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;

@Component("loader")
@ConditionalOnProperty(name = "hazelcast.server.enabled", havingValue = "true")
public class AccumuloMapLoader<T extends BaseQueryMetric> implements MapLoader<String,QueryMetricUpdateHolder<T>> {
    
    private Logger log = LoggerFactory.getLogger(getClass());
    private static AccumuloMapLoader instance;
    protected ShardTableQueryMetricHandler<T> handler;
    
    public static class Factory implements MapStoreFactory<String,QueryMetricUpdate> {
        @Override
        public MapLoader<String,QueryMetricUpdate> newMapStore(String mapName, Properties properties) {
            return AccumuloMapLoader.instance;
        }
    }
    
    protected AccumuloMapLoader() {
        
    }
    
    @Autowired
    public AccumuloMapLoader(ShardTableQueryMetricHandler<T> handler) {
        this.handler = handler;
        AccumuloMapLoader.instance = this;
    }
    
    public static AccumuloMapLoader getInstance() {
        return instance;
    }
    
    @Override
    public QueryMetricUpdateHolder load(String s) {
        T metric = null;
        try {
            metric = this.handler.getQueryMetric(s);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return metric == null ? null : new QueryMetricUpdateHolder(metric);
    }
    
    @Override
    public Map<String,QueryMetricUpdateHolder<T>> loadAll(Collection<String> keys) {
        Map<String,QueryMetricUpdateHolder<T>> metrics = new LinkedHashMap<>();
        keys.forEach(id -> {
            BaseQueryMetric queryMetric;
            try {
                queryMetric = this.handler.getQueryMetric(id);
                if (queryMetric != null) {
                    metrics.put(id, new QueryMetricUpdateHolder(queryMetric));
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });
        return metrics;
    }
    
    @Override
    public Iterable<String> loadAllKeys() {
        // not implemented
        return null;
    }
}
