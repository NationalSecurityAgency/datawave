package datawave.webservice.operations.admin;

import datawave.annotation.Required;
import datawave.webservice.operations.remote.RemoteAdminService;
import datawave.webservice.result.VoidResponse;
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

@Path("/Accumulo")
@RolesAllowed({"InternalUser", "Administrator"})
@DeclareRoles({"InternalUser", "Administrator"})
@LocalBean
@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class TableAdminBean {

    @Inject
    private RemoteAdminService remoteAdminService;

    @Path("/CreateTable/{tableName}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public VoidResponse createTable(@Required("tableName") @PathParam("tableName") String tableName) {
        return remoteAdminService.createTable(tableName);
    }

    @Path("/FlushTable/{tableName}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public VoidResponse flushTable(@PathParam("tableName") String tableName) {
        return remoteAdminService.flushTable(tableName);
    }

    @Path("/SetTableProperty/{tableName}/{propertyName}/{propertyValue}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public VoidResponse setTableProperty(@PathParam("tableName") String tableName, @PathParam("propertyName") String propertyName,
                    @PathParam("propertyValue") String propertyValue) {
        return remoteAdminService.setTableProperty(tableName, propertyName, propertyValue);
    }

    @Path("/RemoveTableProperty/{tableName}/{propertyName}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public VoidResponse removeTableProperty(@PathParam("tableName") String tableName, @PathParam("propertyName") String propertyName) {
        return remoteAdminService.removeTableProperty(tableName, propertyName);
    }
}
