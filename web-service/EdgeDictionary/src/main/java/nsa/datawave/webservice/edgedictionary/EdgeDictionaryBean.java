package nsa.datawave.webservice.edgedictionary;

import nsa.datawave.configuration.spring.SpringBean;
import nsa.datawave.interceptor.ResponseInterceptor;
import nsa.datawave.security.authorization.DatawavePrincipal;
import nsa.datawave.security.util.AuthorizationsUtil;
import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import nsa.datawave.webservice.results.edgedictionary.EdgeDictionaryBase;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;

import javax.annotation.Resource;
import javax.annotation.security.PermitAll;
import javax.ejb.EJBContext;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import java.security.Principal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Path("/EdgeDictionary")
@GZIP
@Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
        "application/x-protostuff", "text/html"})
@LocalBean
@Stateless
@PermitAll
public class EdgeDictionaryBean {
    
    private static final Logger log = Logger.getLogger(EdgeDictionaryBean.class);
    
    @Resource
    private EJBContext ctx;
    
    @Inject
    private AccumuloConnectionFactory connectionFactory;
    
    @Inject
    private DatawaveEdgeDictionary datawaveEdgeDictionary;
    
    @Inject
    @SpringBean(refreshable = true)
    private EdgeDictionaryConfiguration edgeDictionaryConfiguration;
    
    private Connector getConnector() throws Exception {
        Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
        return connectionFactory.getConnection(AccumuloConnectionFactory.Priority.NORMAL, trackingMap);
    }
    
    private void returnConnector(Connector connector) throws Exception {
        connectionFactory.returnConnection(connector);
    }
    
    /**
     * Returns the EdgeDictionary given a metadata table and authorizations
     *
     * @param metadataTableName
     *            Name of metadata table (Optional)
     * @param queryAuthorizations
     *            Authorizations to use
     * @return
     * @throws Exception
     */
    @GET
    @Path("/")
    @Interceptors({ResponseInterceptor.class})
    public EdgeDictionaryBase getEdgeDictionary(@QueryParam("metadataTableName") String metadataTableName, @QueryParam("auths") String queryAuthorizations)
                    throws Exception {
        
        log.info("EDGEDICTIONARY: entered rest endpoint");
        if (null == metadataTableName || StringUtils.isBlank(metadataTableName)) {
            metadataTableName = this.edgeDictionaryConfiguration.getMetadataTableName();
        }
        
        Connector connector = getConnector();
        
        // If the user provides authorizations, intersect it with their actual authorizations
        Set<Authorizations> auths = AuthorizationsUtil.getDowngradedAuthorizations(queryAuthorizations, ctx.getCallerPrincipal());
        
        EdgeDictionaryBase edgeDict = this.datawaveEdgeDictionary.getEdgeDictionary(metadataTableName, connector, auths,
                        this.edgeDictionaryConfiguration.getNumThreads());
        
        returnConnector(connector);
        
        log.info("EDGEDICTIONARY: returning edge dictionary");
        return edgeDict;
    }
    
}
