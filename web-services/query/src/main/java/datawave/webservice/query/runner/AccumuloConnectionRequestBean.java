package datawave.webservice.query.runner;

import org.apache.log4j.Logger;

import datawave.core.query.runner.AccumuloConnectionRequestMap;
import jakarta.annotation.Resource;
import jakarta.ejb.EJBContext;
import jakarta.inject.Singleton;

/**
 * For storing a map of queryId to Thread that is requesting an AccumuloConnection
 */
@Singleton
// CDI singleton
public class AccumuloConnectionRequestBean extends AccumuloConnectionRequestMap {

    private static Logger log = Logger.getLogger(AccumuloConnectionRequestBean.class);

    @Resource
    private EJBContext ctx;

    private AccumuloConnectionRequestMap getConnectionThreadMap = new AccumuloConnectionRequestMap();

    public boolean cancelConnectionRequest(String id) {
        return cancelConnectionRequest(id, ctx.getCallerPrincipal().getName());
    }

}
