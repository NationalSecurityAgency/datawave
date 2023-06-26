package datawave.webservice.modification.cache;

import datawave.configuration.spring.SpringBean;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.interceptor.RequiredInterceptor;
import datawave.interceptor.ResponseInterceptor;
import datawave.modification.cache.ModificationCache;
import datawave.modification.configuration.ModificationConfiguration;
import datawave.webservice.result.VoidResponse;
import datawave.webservice.results.modification.MutableFieldListResponse;
import org.apache.deltaspike.core.api.jmx.JmxManaged;
import org.apache.deltaspike.core.api.jmx.MBean;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;

import javax.annotation.PostConstruct;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.LocalBean;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@Path("/Modification")
@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
@Startup
// tells the container to initialize on startup
@Singleton
// this is a singleton bean in the container
@LocalBean
@Lock(LockType.READ)
// by default all methods are non-blocking
@MBean
public class ModificationCacheBean {

    private static final Text MODIFICATION_COLUMN = new Text("m");

    private Logger log = Logger.getLogger(this.getClass());

    private ModificationCache cache;

    @Inject
    private AccumuloConnectionFactory connectionFactory;

    @Inject
    @SpringBean(refreshable = true)
    private ModificationConfiguration modificationConfiguration;

    @PostConstruct
    public void init() {
        cache = new ModificationCache(connectionFactory, modificationConfiguration);
    }

    /**
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @HTTP 200 success
     * @HTTP 202 if asynch is true - see Location response header for the job URI location
     * @HTTP 400 invalid or missing parameter
     * @HTTP 500 internal server error
     */
    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/reloadCache")
    @GZIP
    @JmxManaged
    public VoidResponse reloadMutableFieldCache() {
        this.cache.reloadMutableFieldCache();
        return new VoidResponse();
    }

    @JmxManaged
    public String listMutableFields() {
        return cache.listMutableFields();
    }

    /**
     * Check to see if field for specified datatype is mutable
     *
     * @param datatype
     *            a datatype
     * @param field
     *            name of field
     * @return true if field is mutable for the given datatype
     */
    public boolean isFieldMutable(String datatype, String field) {
        return cache.isFieldMutable(datatype, field);
    }

    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/getMutableFieldList")
    @GZIP
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public List<MutableFieldListResponse> getMutableFieldList() {
        List<MutableFieldListResponse> lists = new ArrayList<>();
        for (Entry<String,Set<String>> entry : this.cache.getCachedMutableFieldList().entrySet()) {
            MutableFieldListResponse r = new MutableFieldListResponse();
            r.setDatatype(entry.getKey());
            r.setMutableFields(entry.getValue());
            lists.add(r);
        }
        return lists;
    }

    public Map<String,Set<String>> getCachedMutableFieldList() {
        return cache.getCachedMutableFieldList();
    }

    public ModificationConfiguration getModificationConfiguration() {
        return modificationConfiguration;
    }

    public ModificationCache getCache() {
        return cache;
    }

}
