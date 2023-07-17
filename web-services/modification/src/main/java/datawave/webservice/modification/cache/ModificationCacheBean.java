package datawave.webservice.modification.cache;

import static datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.EJBException;
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

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.deltaspike.core.api.jmx.JmxManaged;
import org.apache.deltaspike.core.api.jmx.MBean;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;

import datawave.configuration.spring.SpringBean;
import datawave.interceptor.RequiredInterceptor;
import datawave.interceptor.ResponseInterceptor;
import datawave.security.util.ScannerHelper;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.modification.configuration.ModificationConfiguration;
import datawave.webservice.result.VoidResponse;
import datawave.webservice.results.modification.MutableFieldListResponse;

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

    private Map<String,Set<String>> cache = new HashMap<>();

    @Inject
    private AccumuloConnectionFactory connectionFactory;

    @Inject
    @SpringBean(refreshable = true)
    private ModificationConfiguration modificationConfiguration;

    @PostConstruct
    public void init() {
        if (modificationConfiguration != null) {
            reloadMutableFieldCache();
        } else {
            log.error("modificationConfiguration was null");
        }
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
        this.clearCache();
        log.trace("cleared cache");
        final VoidResponse resp = new VoidResponse();
        AccumuloClient client = null;
        BatchScanner s = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            log.trace("getting mutable list from table " + this.modificationConfiguration.getTableName());
            log.trace("modificationConfiguration.getPoolName() = " + modificationConfiguration.getPoolName());
            client = connectionFactory.getClient(modificationConfiguration.getPoolName(), Priority.ADMIN, trackingMap);
            log.trace("got connection");
            s = ScannerHelper.createBatchScanner(client, this.modificationConfiguration.getTableName(),
                            Collections.singleton(client.securityOperations().getUserAuthorizations(client.whoami())), 8);
            s.setRanges(Collections.singleton(new Range()));
            s.fetchColumnFamily(MODIFICATION_COLUMN);
            for (Entry<Key,Value> e : s) {
                // Field name is in the row and datatype is in the colq.
                String datatype = e.getKey().getColumnQualifier().toString();
                log.trace("datatype = " + datatype);
                String fieldName = e.getKey().getRow().toString();
                log.trace("fieldname = " + fieldName);
                if (null == cache.get(datatype))
                    cache.put(datatype, new HashSet<>());
                cache.get(datatype).add(fieldName);
            }
            log.trace("cache size = " + cache.size());
            for (Entry<String,Set<String>> e : cache.entrySet()) {
                log.trace("datatype = " + e.getKey() + ", fieldcount = " + e.getValue().size());
            }
        } catch (Exception e) {
            log.error("Error during initialization of ModificationCacheBean", e);
            throw new EJBException("Error during initialization of ModificationCacheBean", e);
        } finally {
            if (null != s)
                s.close();
            try {
                connectionFactory.returnClient(client);
            } catch (Exception e) {
                log.error("Error returning connection to pool", e);
            }
        }
        return resp;
    }

    @JmxManaged
    public String listMutableFields() {
        return cache.toString();
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
        log.trace("datatype = " + datatype + ", field = " + field);
        return cache.get(datatype).contains(field);
    }

    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/getMutableFieldList")
    @GZIP
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public List<MutableFieldListResponse> getMutableFieldList() {
        List<MutableFieldListResponse> lists = new ArrayList<>();
        for (Entry<String,Set<String>> entry : this.cache.entrySet()) {
            MutableFieldListResponse r = new MutableFieldListResponse();
            r.setDatatype(entry.getKey());
            r.setMutableFields(entry.getValue());
            lists.add(r);
        }
        return lists;
    }

    public Map<String,Set<String>> getCachedMutableFieldList() {
        log.trace("cache = " + cache);
        return Collections.unmodifiableMap(cache);
    }

    public ModificationConfiguration getModificationConfiguration() {
        return modificationConfiguration;
    }

    protected void clearCache() {
        log.trace("cleared the cache");
        this.cache.clear();
    }

}
