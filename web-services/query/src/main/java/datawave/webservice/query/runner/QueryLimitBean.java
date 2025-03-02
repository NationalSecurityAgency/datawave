package datawave.webservice.query.runner;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.apache.deltaspike.core.api.exclude.Exclude;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.interceptor.RequiredInterceptor;
import datawave.interceptor.ResponseInterceptor;
import datawave.webservice.query.cache.limits.QueryLimitStore;

@Path("/Query")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@Stateless
@LocalBean
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class QueryLimitBean {

    @Inject
    private QueryLimitStore queryLimitStore;

    /**
     * Sets the DN limit.
     *
     * @param dn
     *            - DN for the user running the query.
     * @param limit
     *            - Maximum number of concurrent queries for the DN(default 100)
     * @return String representation of the limit -
     *
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/limits/concurrent/set/{dn}/{limit}")
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public String setConcurrentLimits(@PathParam("dn") String dn, @QueryParam("limit") @DefaultValue("100") int limit) {

        return Integer.toString(queryLimitStore.setQueryLimit(dn, limit));

    }

    /**
     * Gets the DN limit.
     *
     * @param dn
     *            - DN for the user running the query.
     * @return String representation of the limit -
     *
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/limits/concurrent//set/{dn}/{limit}")
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public String getConcurrentLimits(@PathParam("dn") String dn, @QueryParam("limit") @DefaultValue("100") int limit) {
        return Integer.toString(queryLimitStore.getQueryLimit(dn, Integer.MAX_VALUE));
    }

}
