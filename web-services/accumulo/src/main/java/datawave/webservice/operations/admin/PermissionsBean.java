package datawave.webservice.operations.admin;

import jakarta.annotation.security.DeclareRoles;
import jakarta.annotation.security.RolesAllowed;
import jakarta.ejb.LocalBean;
import jakarta.ejb.Stateless;
import jakarta.ejb.TransactionAttribute;
import jakarta.ejb.TransactionAttributeType;
import jakarta.ejb.TransactionManagement;
import jakarta.ejb.TransactionManagementType;
import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;

import datawave.webservice.operations.remote.RemoteAdminService;
import datawave.webservice.result.VoidResponse;

@Path("/Accumulo")
@RolesAllowed({"InternalUser", "Administrator"})
@DeclareRoles({"InternalUser", "Administrator"})
@LocalBean
@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class PermissionsBean {

    @Inject
    private RemoteAdminService remoteAdminService;

    @Path("/GrantSystemPermission/{userName}/{permission}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public VoidResponse grantSystemPermission(@PathParam("userName") String userName, @PathParam("permission") String permission) {

        return remoteAdminService.grantSystemPermission(userName, permission);
    }

    @Path("/GrantTablePermission/{userName}/{tableName}/{permission}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public VoidResponse grantTablePermission(@PathParam("userName") String userName, @PathParam("tableName") String tableName,
                    @PathParam("permission") String permission) {
        return remoteAdminService.grantTablePermission(userName, tableName, permission);
    }

    @Path("/RevokeSystemPermission/{userName}/{permission}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public VoidResponse revokeSystemPermission(@PathParam("userName") String userName, @PathParam("permission") String permission) {
        return remoteAdminService.revokeSystemPermission(userName, permission);
    }

    @Path("/RevokeTablePermission/{userName}/{tableName}/{permission}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public VoidResponse revokeTablePermission(@PathParam("userName") String userName, @PathParam("tableName") String tableName,
                    @PathParam("permission") String permission) {
        return remoteAdminService.revokeTablePermission(userName, tableName, permission);
    }
}
