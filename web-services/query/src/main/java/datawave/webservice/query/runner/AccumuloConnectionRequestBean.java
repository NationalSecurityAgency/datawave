package datawave.webservice.query.runner;

import javax.annotation.Resource;
import javax.ejb.EJBContext;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.query.runner.AccumuloConnectionRequestMap;

/**
 * For storing a map of queryId to Thread that is requesting an AccumuloConnection
 */
@Singleton
// CDI singleton
public class AccumuloConnectionRequestBean extends AccumuloConnectionRequestMap {

    private static Logger log = LoggerFactory.getLogger(AccumuloConnectionRequestBean.class);

    @Resource
    private EJBContext ctx;

    private AccumuloConnectionRequestMap getConnectionThreadMap = new AccumuloConnectionRequestMap();

    public boolean cancelConnectionRequest(String id) {
        return cancelConnectionRequest(id, ctx.getCallerPrincipal().getName());
    }

}
