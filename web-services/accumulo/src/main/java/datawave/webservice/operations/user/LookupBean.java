package datawave.webservice.operations.user;

import datawave.annotation.Required;
import datawave.interceptor.RequiredInterceptor;
import datawave.interceptor.ResponseInterceptor;
import datawave.webservice.operations.remote.RemoteLookupService;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.response.LookupResponse;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
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
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

@Path("/Accumulo")
@RolesAllowed({"InternalUser", "Administrator"})
@DeclareRoles({"InternalUser", "Administrator"})
@LocalBean
@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
@Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
public class LookupBean {

    @Inject
    private RemoteLookupService remoteLookupService;

    @Path("/Lookup/{table}/{row}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @GET
    public LookupResponse lookupGet(@Required("table") @PathParam("table") String table, @Required("row") @PathParam("row") String row, @Context UriInfo ui)
                    throws QueryException {
        
        return lookup(table, row, ui.getQueryParameters(true));
    }

    @Path("/Lookup/{table}/{row}")
    @Consumes("application/x-www-form-urlencoded")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public LookupResponse lookupPost(@Required("table") @PathParam("table") String table, @Required("row") @PathParam("row") String row,
                    MultivaluedMap<String,String> formParameters) throws QueryException {
        
        return lookup(table, row, formParameters);
    }
    
    @PermitAll
    public LookupResponse lookup(String table, String row, MultivaluedMap<String,String> queryParameters) throws QueryException {
        return remoteLookupService.lookup(table, row, queryParameters);
    }
}
