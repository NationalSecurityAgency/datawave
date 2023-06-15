package datawave.webservice.query.runner;

import org.apache.accumulo.core.util.Pair;
import org.apache.log4j.Logger;

import javax.annotation.Resource;
import javax.ejb.EJBContext;
import javax.inject.Singleton;
import java.security.Principal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * For storing a map of queryId to Thread that is requesting an AccumuloConnection
 */
@Singleton
// CDI singleton
public class AccumuloConnectionRequestBean {

    private static Logger log = Logger.getLogger(AccumuloConnectionRequestBean.class);

    @Resource
    private EJBContext ctx;

    private Map<String,Pair<Principal,Thread>> getConnectionThreadMap = new ConcurrentHashMap<>();

    public boolean cancelConnectionRequest(String id) {
        return cancelConnectionRequest(id, ctx.getCallerPrincipal());
    }

    public boolean cancelConnectionRequest(String id, Principal principal) {
        // this call checks that the Principal used for the connection request and th connection cancel are the same
        // if query is waiting for an accumulo connection in create or reset, then interrupt it
        boolean connectionRequestCanceled = false;
        try {
            Pair<Principal,Thread> connectionRequestPair = getConnectionThreadMap.get(id);
            if (connectionRequestPair != null) {
                String connectionRequestPrincipalName = principal.getName();
                String connectionCancelPrincipalName = connectionRequestPair.getFirst().getName();
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
            Pair<Principal,Thread> connectionRequestPair = getConnectionThreadMap.get(id);
            if (connectionRequestPair != null) {
                connectionRequestPair.getSecond().interrupt();
                connectionRequestCanceled = true;
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return connectionRequestCanceled;
    }

    public void requestBegin(String id) {
        Pair<Principal,Thread> connectionRequestPair = new Pair<>(ctx.getCallerPrincipal(), Thread.currentThread());
        getConnectionThreadMap.put(id, connectionRequestPair);
    }

    public void requestEnd(String id) {
        getConnectionThreadMap.remove(id);
    }
}
