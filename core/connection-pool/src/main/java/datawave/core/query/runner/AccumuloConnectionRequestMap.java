package datawave.core.query.runner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.common.connection.AccumuloConnectionFactory;

/**
 * For storing a map of queryId to Thread that is requesting an AccumuloConnection
 */
public class AccumuloConnectionRequestMap {

    private static Logger log = LoggerFactory.getLogger(AccumuloConnectionRequestMap.class);

    /**
     * This maps the query-id to a pair containing the tracking map (see the AccumuloConnectionFactory) and the thread handling the request
     */
    private Map<String,List<Pair<Map<String,String>,Thread>>> connectionThreadMap = new HashMap<>();

    public boolean cancelConnectionRequest(String id, String userDn) {
        // this call checks that the Principal used for the connection request and the connection cancel are the same
        // if query is waiting for an accumulo connection in create or reset, then interrupt it
        boolean connectionRequestCanceled = false;
        synchronized (connectionThreadMap) {
            List<Pair<Map<String,String>,Thread>> connectionRequestPairs = connectionThreadMap.get(id);
            if (connectionRequestPairs != null) {
                for (Pair<Map<String,String>,Thread> connectionRequestPair : connectionRequestPairs) {
                    try {
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
                }
            }
        }
        return connectionRequestCanceled;
    }

    public boolean adminCancelConnectionRequest(String id) {
        // it is assumed that admin status is already checked, so this call does not check the calling Principals
        // if query is waiting for an accumulo connection in create or reset, then interrupt it
        boolean connectionRequestCanceled = false;
        List<Pair<Map<String,String>,Thread>> connectionRequestPairs = connectionThreadMap.get(id);
        if (connectionRequestPairs != null) {
            for (Pair<Map<String,String>,Thread> connectionRequestPair : connectionRequestPairs) {
                try {
                    if (connectionRequestPair != null && connectionRequestPair.getFirst() != null) {
                        connectionRequestPair.getSecond().interrupt();
                        connectionRequestCanceled = true;
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }

        return connectionRequestCanceled;
    }

    public void requestBegin(String id, String userDN, Map<String,String> trackingMap) {
        synchronized (connectionThreadMap) {
            List<Pair<Map<String,String>,Thread>> connectionRequestPairs = connectionThreadMap.get(id);
            if (connectionRequestPairs == null) {
                connectionRequestPairs = new ArrayList<>();
                connectionThreadMap.put(id, connectionRequestPairs);
            }
            Pair<Map<String,String>,Thread> connectionRequestPair = new Pair<>(trackingMap, Thread.currentThread());
            if (userDN != null && trackingMap != null)
                trackingMap.put(AccumuloConnectionFactory.USER_DN, userDN);
            connectionRequestPairs.add(connectionRequestPair);
        }
    }

    public void requestEnd(String id) {
        synchronized (connectionThreadMap) {
            List<Pair<Map<String,String>,Thread>> connectionRequestPairs = connectionThreadMap.get(id);
            Thread t = Thread.currentThread();
            Iterator<Pair<Map<String,String>,Thread>> it = connectionRequestPairs.iterator();
            boolean found = false;
            while (!found && it.hasNext()) {
                Pair<Map<String,String>,Thread> connectionRequestPair = it.next();
                if (connectionRequestPair.getSecond().equals(t)) {
                    it.remove();
                    found = true;
                }
            }
            if (connectionRequestPairs.isEmpty()) {
                connectionThreadMap.remove(id);
            }
        }
    }
}
