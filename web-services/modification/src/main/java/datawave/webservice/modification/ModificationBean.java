package datawave.webservice.modification;

import java.util.List;

import jakarta.annotation.Resource;
import jakarta.annotation.security.DeclareRoles;
import jakarta.annotation.security.RolesAllowed;
import jakarta.ejb.EJBContext;
import jakarta.ejb.LocalBean;
import jakarta.ejb.Stateless;
import jakarta.ejb.TransactionAttribute;
import jakarta.ejb.TransactionAttributeType;
import jakarta.ejb.TransactionManagement;
import jakarta.ejb.TransactionManagementType;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptors;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;

import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;

import datawave.annotation.Required;
import datawave.configuration.spring.SpringBean;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.interceptor.RequiredInterceptor;
import datawave.interceptor.ResponseInterceptor;
import datawave.modification.DatawaveModificationException;
import datawave.modification.ModificationService;
import datawave.modification.configuration.ModificationConfiguration;
import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.modification.cache.ModificationCacheBean;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.runner.QueryExecutorBean;
import datawave.webservice.result.VoidResponse;
import datawave.webservice.results.modification.ModificationConfigurationResponse;

@Path("/Modification")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@Stateless
@LocalBean
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class ModificationBean {

    private Logger log = Logger.getLogger(this.getClass());

    @Resource
    private EJBContext ctx;

    @Inject
    private AccumuloConnectionFactory connectionFactory;

    @Inject
    private ModificationCacheBean cache;

    @Inject
    private QueryExecutorBean queryService;

    @Inject
    @SpringBean(refreshable = true)
    private ModificationConfiguration modificationConfiguration;

    private ModificationService service;

    private ModificationService getService() {
        if (service == null) {
            service = new ModificationService(modificationConfiguration, cache.getCache(), connectionFactory,
                            new QueryExecutorBeanService(queryService).getFactory());
        }
        return service;
    }

    /**
     * Returns a list of the Modification service names and their configurations
     *
     * @return datawave.webservice.results.modification.ModificationConfigurationResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     */
    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/listConfigurations")
    @GZIP
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public List<ModificationConfigurationResponse> listConfigurations() {
        return getService().listConfigurations();
    }

    /**
     * Execute a Modification service with the given name and runtime parameters
     *
     * @param modificationServiceName
     *            Name of the modification service configuration
     * @param request
     *            object type specified in listConfigurations response.
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @HTTP 200 success
     * @HTTP 400 if jobName is invalid
     * @HTTP 401 if user does not have correct roles
     * @HTTP 500 error starting the job
     */
    @PUT
    @Consumes({"application/xml", "text/xml", "application/json"})
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/{serviceName}/submit")
    @GZIP
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public VoidResponse submit(@Required("modificationServiceName") @PathParam("serviceName") String modificationServiceName,
                    @Required("request") ModificationRequestBase request) {
        try {
            DatawavePrincipal p = (DatawavePrincipal) ctx.getCallerPrincipal();
            return getService().submit(p, modificationServiceName, request);
        } catch (DatawaveModificationException dme) {
            VoidResponse response = new VoidResponse();
            for (QueryException qe : dme.getExceptions()) {
                response.addException(qe);
            }
            throw new DatawaveWebApplicationException(dme, response);
        }
    }

}
