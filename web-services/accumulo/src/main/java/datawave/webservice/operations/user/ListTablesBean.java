package datawave.webservice.operations.user;

import datawave.webservice.operations.remote.RemoteAdminService;
import datawave.webservice.response.ListTablesResponse;
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
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;

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
