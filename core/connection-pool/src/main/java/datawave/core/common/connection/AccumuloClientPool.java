package datawave.core.common.connection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloClientPool extends GenericObjectPool<AccumuloClient> {

    private static final Logger log = LoggerFactory.getLogger(AccumuloClientPool.class);
    private final Map<Long,Map<String,String>> threadToTrackingMapMap = Collections.synchronizedMap(new HashMap<>());
    private final Map<AccumuloClient,Map<String,String>> connectorToTrackingMapMap = Collections.synchronizedMap(new HashMap<>());
    private AccumuloClientPoolFactory factory;

    public AccumuloClientPool(AccumuloClientPoolFactory factory) {
        super(factory);
        this.factory = factory;
    }

    @Override
    public String toString() {
        return " NumIdle: " + getNumIdle() + " NumActive: " + getNumActive() + " MaxIdle: " + getMaxIdle() + " MaxTotal: " + getMaxTotal();
    }

    public AccumuloClient borrowObject(Map<String,String> trackingMap) throws Exception {

        Long threadId = Thread.currentThread().getId();
        AccumuloClient o;
        try {
            trackingMap.put(AccumuloConnectionFactory.START_TIME, Long.valueOf(System.currentTimeMillis()).toString());
            trackingMap.put(AccumuloConnectionFactory.STATE, AccumuloConnectionFactory.State.WAITING.toString());
            trackingMap.put(AccumuloConnectionFactory.THREAD_NAME, Thread.currentThread().getName());
            threadToTrackingMapMap.put(threadId, trackingMap);
            o = super.borrowObject();
            log.debug("{} thread: {} borrowed connector: {}", System.currentTimeMillis(), threadId, o);
            if (log.isTraceEnabled()) {
                log.trace("{} {}", System.currentTimeMillis(), Arrays.toString(Thread.currentThread().getStackTrace()));
            }
            // hopefully insignificant gap in synchronization where an object could be returned (and numActive incremented) without the
            // connection being moved from the threadToTrackingMapMap to the connectorToTrackingMapMap

            if (o != null) {
                trackingMap.put(AccumuloConnectionFactory.START_TIME, Long.valueOf(System.currentTimeMillis()).toString());
                trackingMap.put(AccumuloConnectionFactory.STATE, AccumuloConnectionFactory.State.CONNECTED.toString());
                connectorToTrackingMapMap.put(o, trackingMap);
            }

        } finally {
            threadToTrackingMapMap.remove(threadId);
        }
        return o;
    }

    @Override
    public AccumuloClient borrowObject() throws Exception {
        throw new UnsupportedOperationException("you can not call AccumuloConnectionFactory.borrowObject without a trackingMap argument");
    }

    public void returnObject(AccumuloClient client) {
        if (client != null) {
            synchronized (connectorToTrackingMapMap) {
                connectorToTrackingMapMap.remove(client);
                long threadId = Thread.currentThread().getId();
                log.debug("{} thread: {} returned client: {}", System.currentTimeMillis(), threadId, client);
                if (log.isTraceEnabled()) {
                    log.trace("{} {}", System.currentTimeMillis(), Arrays.toString(Thread.currentThread().getStackTrace()));
                }
            }

            super.returnObject(client);
        }
    }

    public List<Map<String,String>> getConnectionPoolStats(MutableInt maxTotal, MutableInt numActive, MutableInt maxIdle, MutableInt numIdle,
                    MutableInt numWaiting) {

        ArrayList<Map<String,String>> t = new ArrayList<>();
        // no changes to underlying values while collecting metrics
        synchronized (connectorToTrackingMapMap) {
            // no changes to underlying values while collecting metrics
            synchronized (threadToTrackingMapMap) {
                // synchronize this last to prevent race condition for this lock underlying super type
                synchronized (this) {
                    if (!threadToTrackingMapMap.isEmpty()) {
                        t.addAll(Collections.unmodifiableCollection(threadToTrackingMapMap.values()));
                    }
                    if (!connectorToTrackingMapMap.isEmpty()) {
                        t.addAll(Collections.unmodifiableCollection(connectorToTrackingMapMap.values()));
                    }
                    maxTotal.setValue(getMaxTotal());
                    numActive.setValue(getNumActive());
                    maxIdle.setValue(getMaxIdle());
                    numIdle.setValue(getNumIdle());
                    numWaiting.setValue(getNumWaiters());
                }
            }
        }
        return Collections.unmodifiableList(t);
    }

    public boolean connectorCameFromHere(AccumuloClient client) {
        return this.connectorToTrackingMapMap.containsKey(client);
    }

    @SuppressWarnings("UnusedDeclaration")
    public int getNumActiveEntriesBeingTracked() {
        return connectorToTrackingMapMap.size();
    }

    public AccumuloClientPoolFactory getFactory() {
        return factory;
    }
}
