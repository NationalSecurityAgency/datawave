package datawave.webservice.operations.admin;

import datawave.annotation.Required;
import datawave.webservice.operations.remote.RemoteAdminService;
import datawave.webservice.result.VoidResponse;
import org.apache.log4j.Logger;

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
