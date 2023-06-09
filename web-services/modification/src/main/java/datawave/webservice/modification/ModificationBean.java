package datawave.webservice.modification;

import datawave.annotation.Required;
import datawave.configuration.spring.SpringBean;
import datawave.interceptor.RequiredInterceptor;
import datawave.interceptor.ResponseInterceptor;
import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.common.audit.AuditParameterBuilder;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.exception.BadRequestException;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.common.exception.UnauthorizedException;
import datawave.webservice.modification.cache.ModificationCacheBean;
import datawave.webservice.modification.configuration.ModificationConfiguration;
import datawave.webservice.modification.configuration.ModificationServiceConfiguration;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.UnauthorizedQueryException;
import datawave.webservice.query.runner.QueryExecutorBean;
import datawave.webservice.result.VoidResponse;
import datawave.webservice.results.modification.ModificationConfigurationResponse;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;

import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJBContext;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MultivaluedMap;
import java.security.Principal;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Map.Entry;

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

    @Inject
    private AuditParameterBuilder auditParameterBuilder;

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
        List<ModificationConfigurationResponse> configs = new ArrayList<>();
        for (Entry<String,ModificationServiceConfiguration> entry : this.modificationConfiguration.getConfigurations().entrySet()) {
            ModificationConfigurationResponse r = new ModificationConfigurationResponse();
            r.setName(entry.getKey());
            r.setRequestClass(entry.getValue().getRequestClass().getName());
            r.setDescription(entry.getValue().getDescription());
            r.setAuthorizedRoles(entry.getValue().getAuthorizedRoles());
            configs.add(r);
        }
        return configs;
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
        VoidResponse response = new VoidResponse();

        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String user;
        Set<Authorizations> cbAuths = new HashSet<>();
        Collection<String> userRoles = Collections.emptySet();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            user = dp.getShortName();
            userRoles = dp.getPrimaryUser().getRoles();
            for (Collection<String> c : dp.getAuthorizations())
                cbAuths.add(new Authorizations(c.toArray(new String[c.size()])));
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_PRINCIPAL_ERROR, MessageFormat.format("Class: {0}", p.getClass().getName()));
            response.addException(qe);
            throw new DatawaveWebApplicationException(qe, response);
        }

        AccumuloClient client = null;
        AccumuloConnectionFactory.Priority priority;
        try {
            // Get the Modification Service from the configuration
            ModificationServiceConfiguration service = modificationConfiguration.getConfiguration(modificationServiceName);
            if (!request.getClass().equals(service.getRequestClass())) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_REQUEST_CLASS,
                                MessageFormat.format("Requires: {0}", service.getRequestClass().getName()));
                response.addException(qe);
                throw new BadRequestException(qe, response);
            }

            priority = service.getPriority();

            // Ensure that the user is in the list of authorized roles
            if (null != service.getAuthorizedRoles()) {
                boolean authorized = !Collections.disjoint(userRoles, service.getAuthorizedRoles());
                if (!authorized) {
                    // Then the user does not have any of the authorized roles
                    UnauthorizedQueryException qe = new UnauthorizedQueryException(DatawaveErrorCode.JOB_EXECUTION_UNAUTHORIZED,
                                    MessageFormat.format("Requires one of: {0}", service.getAuthorizedRoles()));
                    response.addException(qe);
                    throw new UnauthorizedException(qe, response);
                }
            }

            if (service.getRequiresAudit()) {
                try {
                    MultivaluedMap<String,String> requestMap = new MultivaluedMapImpl<>();
                    requestMap.putAll(request.toMap());
                    auditParameterBuilder.convertAndValidate(requestMap);
                } catch (Exception e) {
                    QueryException qe = new QueryException(DatawaveErrorCode.QUERY_AUDITING_ERROR, e);
                    log.error(qe);
                    response.addException(qe.getBottomQueryException());
                }
            }

            // Process the modification
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            client = connectionFactory.getClient(modificationConfiguration.getPoolName(), priority, trackingMap);
            service.setQueryService(queryService);
            log.info("Processing modification request from user=" + user + ": \n" + request);
            service.process(client, request, cache.getCachedMutableFieldList(), cbAuths, user);
            return response;
        } catch (DatawaveWebApplicationException e) {
            throw e;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.MODIFICATION_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            throw new DatawaveWebApplicationException(e, response);
        } finally {
            if (null != client)
                try {
                    connectionFactory.returnClient(client);
                } catch (Exception e) {
                    log.error("Error returning connection", e);
                }
        }
    }

}
