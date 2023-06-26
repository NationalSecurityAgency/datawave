package datawave.webservice.query.runner;

import datawave.core.query.runner.AccumuloConnectionRequestMap;
import org.apache.log4j.Logger;

import javax.annotation.Resource;
import javax.ejb.EJBContext;
import javax.inject.Singleton;

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
