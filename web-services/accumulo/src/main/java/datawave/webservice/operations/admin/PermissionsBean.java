package datawave.webservice.operations.admin;

import datawave.webservice.operations.remote.RemoteAdminService;
import datawave.webservice.result.VoidResponse;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

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
