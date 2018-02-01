package datawave.webservice.common.connection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.Connector;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.log4j.Logger;

public class AccumuloConnectionPool extends GenericObjectPool<Connector> {
    
    private static final Logger log = Logger.getLogger(AccumuloConnectionPool.class);
    private final Map<Long,Map<String,String>> threadToTrackingMapMap = Collections.synchronizedMap(new HashMap<Long,Map<String,String>>());
    private final Map<Connector,Map<String,String>> connectorToTrackingMapMap = Collections.synchronizedMap(new HashMap<Connector,Map<String,String>>());
    private AccumuloConnectionPoolFactory factory = null;
    
    public AccumuloConnectionPool(AccumuloConnectionPoolFactory factory) {
        super(factory);
        this.factory = factory;
    }
    
    @Override
    public String toString() {
        return " NumIdle: " + getNumIdle() + " NumActive: " + getNumActive() + " MaxIdle: " + getMaxIdle() + " MaxTotal: " + getMaxTotal();
    }
    
    public Connector borrowObject(Map<String,String> trackingMap) throws Exception {
        
        Long threadId = Thread.currentThread().getId();
        Connector o = null;
        try {
            trackingMap.put("connection.state.start", Long.valueOf(System.currentTimeMillis()).toString());
            trackingMap.put("state", AccumuloConnectionFactory.State.WAITING.toString());
            trackingMap.put("thread.name", Thread.currentThread().getName());
            threadToTrackingMapMap.put(threadId, trackingMap);
            o = super.borrowObject();
            log.debug(System.currentTimeMillis() + " thread: " + threadId + " borrowed connector: " + o);
            if (log.isTraceEnabled()) {
                log.trace(System.currentTimeMillis() + " " + Arrays.toString(Thread.currentThread().getStackTrace()));
            }
            // hopefully insignificant gap in synchronization where an object could be returned (and numActive incremented) without the
            // connection being moved from the threadToTrackingMapMap to the connectorToTrackingMapMap
            
            if (o != null) {
                trackingMap.put("connection.state.start", Long.valueOf(System.currentTimeMillis()).toString());
                trackingMap.put("state", AccumuloConnectionFactory.State.CONNECTED.toString());
                connectorToTrackingMapMap.put(o, trackingMap);
            }
            
        } finally {
            threadToTrackingMapMap.remove(threadId);
        }
        return o;
    }
    
    @Override
    public Connector borrowObject() throws Exception {
        throw new UnsupportedOperationException("you can not call AccumuloConnectionFactory.borrowObject without a trackingMap argument");
    }
    
    public void returnObject(Connector connector) {
        if (connector != null) {
            synchronized (connectorToTrackingMapMap) {
                connectorToTrackingMapMap.remove(connector);
                Long threadId = Thread.currentThread().getId();
                log.debug(System.currentTimeMillis() + " thread: " + threadId + " returned connector: " + connector);
                if (log.isTraceEnabled()) {
                    log.trace(System.currentTimeMillis() + " " + Arrays.toString(Thread.currentThread().getStackTrace()));
                }
            }
            
            super.returnObject(connector);
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
                    if (threadToTrackingMapMap.size() > 0) {
                        t.addAll(Collections.unmodifiableCollection(threadToTrackingMapMap.values()));
                    }
                    if (connectorToTrackingMapMap.size() > 0) {
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
    
    public boolean connectorCameFromHere(Connector c) {
        return this.connectorToTrackingMapMap.containsKey(c);
    }
    
    @SuppressWarnings("UnusedDeclaration")
    public int getNumActiveEntriesBeingTracked() {
        return connectorToTrackingMapMap.size();
    }
    
    public AccumuloConnectionPoolFactory getFactory() {
        return factory;
    }
}
