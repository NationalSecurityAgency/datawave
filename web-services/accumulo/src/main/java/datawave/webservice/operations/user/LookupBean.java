package datawave.webservice.operations.user;

import jakarta.annotation.security.DeclareRoles;
import jakarta.annotation.security.PermitAll;
import jakarta.annotation.security.RolesAllowed;
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
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.UriInfo;

import datawave.annotation.Required;
import datawave.interceptor.RequiredInterceptor;
import datawave.interceptor.ResponseInterceptor;
import datawave.webservice.operations.remote.RemoteLookupService;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.response.LookupResponse;

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
