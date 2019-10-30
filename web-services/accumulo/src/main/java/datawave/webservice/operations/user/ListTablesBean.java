package datawave.webservice.operations.user;

import datawave.webservice.operations.remote.RemoteAdminService;
import datawave.webservice.response.ListTablesResponse;

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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("/Accumulo")
@RolesAllowed({"InternalUser", "Administrator"})
@DeclareRoles({"InternalUser", "Administrator"})
@LocalBean
@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class ListTablesBean {
    
    @Inject
    private RemoteAdminService remoteAdminService;
    
    @Path("/ListTables")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @GET
    public ListTablesResponse listAccumuloTables() {
        return listTables();
    }
    
    @PermitAll
    public ListTablesResponse listTables() {
        return remoteAdminService.listTables();
    }
    
}
