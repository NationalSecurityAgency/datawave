package datawave.microservice.querymetric.persistence;

import java.text.SimpleDateFormat;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricUpdate;

public class MetricCacheListener implements RemovalListener {
    
    private Logger log = LoggerFactory.getLogger(MetricCacheListener.class);
    
    private final String cacheName;
    
    public MetricCacheListener(String cacheName) {
        this.cacheName = cacheName;
    }
    
    @Override
    public void onRemoval(@Nullable Object key, @Nullable Object value, @NonNull RemovalCause cause) {
        if (!cause.equals(RemovalCause.REPLACED)) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
            String queryId = (String) key;
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("removalCause=%s, key=%s", cause, queryId));
            BaseQueryMetric metric = null;
            if (value != null && value instanceof QueryMetricUpdate) {
                metric = ((QueryMetricUpdate) value).getMetric();
                sb.append(String.format(" metric[createDate=%s, host=%s, lifecycle=%s, numPages=%s, numUpdates=%s]", sdf.format(metric.getCreateDate()),
                                metric.getHost(), metric.getLifecycle(), metric.getPageTimes().size(), metric.getNumUpdates()));
            }
            if (cause.equals(RemovalCause.SIZE) && metric != null && !metric.isLifecycleFinal()) {
                // reaching max cache size and evicting metrics that are not done writing
                log.info(cacheName + " " + sb);
            } else {
                // more routine - evicting due to expired time
                log.debug(cacheName + " " + sb);
            }
        }
    }
}
