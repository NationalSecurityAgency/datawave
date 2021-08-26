package datawave.webservice.query.runner;

import datawave.webservice.common.connection.AccumuloConnectionFactory;
import org.apache.accumulo.core.util.Pair;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * For storing a map of queryId to Thread that is requesting an AccumuloConnection
 */
public class AccumuloConnectionRequestMap {
    
    private static Logger log = Logger.getLogger(AccumuloConnectionRequestMap.class);
    
    /**
     * This maps the query-id to a pair containing the tracking map (see the AccumuloConnectionFactory) and the thread handling the request
     */
    private Map<String,Pair<Map<String,String>,Thread>> getConnectionThreadMap = new ConcurrentHashMap<>();
    
    public boolean cancelConnectionRequest(String id, String userDn) {
        // this call checks that the Principal used for the connection request and th connection cancel are the same
        // if query is waiting for an accumulo connection in create or reset, then interrupt it
        boolean connectionRequestCanceled = false;
        try {
            Pair<Map<String,String>,Thread> connectionRequestPair = getConnectionThreadMap.get(id);
            if (connectionRequestPair != null && connectionRequestPair.getFirst() != null) {
                String connectionRequestPrincipalName = connectionRequestPair.getFirst().get(AccumuloConnectionFactory.USER_DN);
                String connectionCancelPrincipalName = userDn;
                if (connectionRequestPrincipalName.equals(connectionCancelPrincipalName)) {
                    connectionRequestPair.getSecond().interrupt();
                    connectionRequestCanceled = true;
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return connectionRequestCanceled;
    }
    
    public boolean adminCancelConnectionRequest(String id) {
        // it is assumed that admin status is already checked, so this call does not check the calling Principals
        // if query is waiting for an accumulo connection in create or reset, then interrupt it
        boolean connectionRequestCanceled = false;
        try {
            Pair<Map<String,String>,Thread> connectionRequestPair = getConnectionThreadMap.get(id);
            if (connectionRequestPair != null) {
                connectionRequestPair.getSecond().interrupt();
                connectionRequestCanceled = true;
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return connectionRequestCanceled;
    }
    
    public void requestBegin(String id, String userDN, Map<String,String> trackingMap) {
        Pair<Map<String,String>,Thread> connectionRequestPair = new Pair<>(trackingMap, Thread.currentThread());
        if (userDN != null && trackingMap != null)
            trackingMap.put(AccumuloConnectionFactory.USER_DN, userDN);
        getConnectionThreadMap.put(id, connectionRequestPair);
    }
    
    public void requestEnd(String id) {
        getConnectionThreadMap.remove(id);
    }
}
