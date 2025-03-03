package datawave.microservice.querymetric.persistence;

import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.MapEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.map.listener.EntryLoadedListener;
import com.hazelcast.map.listener.EntryMergedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.map.listener.MapEvictedListener;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricUpdate;

/*
 * @see MapClearedListener
 * @see MapEvictedListener
 * @see EntryAddedListener
 * @see EntryEvictedListener
 * @see EntryRemovedListener
 * @see EntryMergedListener
 * @see EntryUpdatedListener
 * @see EntryLoadedListener
 */

public class MetricMapListener implements EntryAddedListener, EntryUpdatedListener, EntryLoadedListener, MapEvictedListener, EntryEvictedListener,
                EntryRemovedListener, EntryMergedListener {
    
    private Logger log = LoggerFactory.getLogger(MetricMapListener.class);
    private String mapName;
    
    public MetricMapListener(String mapName) {
        this.mapName = mapName;
    }
    
    private String printEvent(EntryEvent event) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
        Object value = event.getValue();
        Object oldValue = event.getOldValue();
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("entryEventType=%s, member=%s, name='%s', key=%s", event.getEventType(), event.getMember(), event.getName(), event.getKey()));
        if (value != null && value instanceof QueryMetricUpdate) {
            BaseQueryMetric valueMetric = ((QueryMetricUpdate) value).getMetric();
            sb.append(String.format(" metric[createDate=%s, host=%s, lifecycle=%s, numPages=%s, numUpdates=%s]", sdf.format(valueMetric.getCreateDate()),
                            valueMetric.getHost(), valueMetric.getLifecycle(), valueMetric.getPageTimes().size(), valueMetric.getNumUpdates()));
        }
        if (oldValue != null && oldValue instanceof QueryMetricUpdate) {
            BaseQueryMetric oldValueMetric = ((QueryMetricUpdate) oldValue).getMetric();
            sb.append(String.format(" oldMetric[createDate=%s, host=%s, lifecycle=%s, numPages=%d, numUpdates=%d]", sdf.format(oldValueMetric.getCreateDate()),
                            oldValueMetric.getHost(), oldValueMetric.getLifecycle(), oldValueMetric.getPageTimes().size(), oldValueMetric.getNumUpdates()));
        }
        return sb.toString();
    }
    
    @Override
    public void entryAdded(EntryEvent event) {
        if (event.getMember().localMember()) {
            log.debug(mapName + " " + printEvent(event));
        }
    }
    
    @Override
    public void entryUpdated(EntryEvent event) {
        if (event.getMember().localMember()) {
            log.debug(mapName + " " + printEvent(event));
        }
    }
    
    @Override
    public void entryLoaded(EntryEvent event) {
        log.debug(mapName + " " + printEvent(event));
    }
    
    @Override
    public void entryEvicted(EntryEvent event) {
        if (event.getMember().localMember()) {
            log.debug(mapName + " " + printEvent(event));
        }
    }
    
    @Override
    public void entryMerged(EntryEvent event) {
        if (event.getMember().localMember()) {
            log.debug(mapName + " " + printEvent(event));
        }
    }
    
    @Override
    public void entryRemoved(EntryEvent event) {
        if (event.getMember().localMember()) {
            log.debug(mapName + " " + printEvent(event));
        }
    }
    
    @Override
    public void mapEvicted(MapEvent event) {
        log.debug(mapName + " : " + event.toString());
    }
}
