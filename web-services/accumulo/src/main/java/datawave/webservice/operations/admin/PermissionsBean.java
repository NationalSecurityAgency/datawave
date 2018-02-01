package datawave.webservice.operations.admin;

import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.exception.AccumuloWebApplicationException;
import datawave.webservice.exception.BadRequestException;
import datawave.webservice.exception.UnauthorizedException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.log4j.Logger;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import java.util.Map;

@Path("/Accumulo")
@RolesAllowed({"InternalUser", "Administrator"})
@DeclareRoles({"InternalUser", "Administrator"})
@LocalBean
@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class PermissionsBean {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @EJB
    private AccumuloConnectionFactory connectionFactory;
    
    public PermissionsBean() {}
    
    /**
     * <strong>Administrator credentials required.</strong> Grant a system permission to an Accumulo user
     * 
     * @param userName
     *            Accumulo user name
     * @param permission
     *            Accumulo system permission to grant
     * @HTTP 200 Success
     * @HTTP 400 Missing required parameter(s) or invalid parameter(s)
     * @HTTP 401 AccumuloSecurityException
     * @HTTP 500 AccumuloException
     * @return datawave.webservice.result.VoidResponse
     */
    @Path("/GrantSystemPermission/{userName}/{permission}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public VoidResponse grantSystemPermission(@PathParam("userName") String userName, @PathParam("permission") String permission) {
        VoidResponse response = new VoidResponse();
        Connector connection = null;
        AccumuloConnectionFactory.Priority priority = AccumuloConnectionFactory.Priority.ADMIN;
        
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            connection = connectionFactory.getConnection(priority, trackingMap);
            SecurityOperations ops = connection.securityOperations();
            ops.grantSystemPermission(userName, SystemPermission.valueOf(permission));
        } catch (AccumuloSecurityException e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new UnauthorizedException(e, response);
        } catch (IllegalArgumentException e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new BadRequestException(e, response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new AccumuloWebApplicationException(e, response);
        } finally {
            try {
                connectionFactory.returnConnection(connection);
            } catch (Exception e) {
                log.error("Error returning connection", e);
            }
        }
        
        return response;
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Grant a table permission to an Accumulo user
     * 
     * @param userName
     *            Accumulo user name
     * @param tableName
     *            Accumulo table name
     * @param permission
     *            Accumulo table permission to grant
     * @HTTP 200 Success
     * @HTTP 400 Missing required parameter(s) or invalid parameter(s)
     * @HTTP 401 AccumuloSecurityException
     * @HTTP 500 AccumuloException
     * @return datawave.webservice.result.VoidResponse
     */
    @Path("/GrantTablePermission/{userName}/{tableName}/{permission}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public VoidResponse grantTablePermission(@PathParam("userName") String userName, @PathParam("tableName") String tableName,
                    @PathParam("permission") String permission) {
        VoidResponse response = new VoidResponse();
        Connector connection = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            connection = connectionFactory.getConnection(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
            SecurityOperations ops = connection.securityOperations();
            ops.grantTablePermission(userName, tableName, TablePermission.valueOf(permission));
        } catch (AccumuloSecurityException e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new UnauthorizedException(e, response);
        } catch (IllegalArgumentException e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new BadRequestException(e, response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new AccumuloWebApplicationException(e, response);
        } finally {
            try {
                connectionFactory.returnConnection(connection);
            } catch (Exception e) {
                log.error("Error returning connection", e);
            }
        }
        return response;
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Revoke a system permission from an Accumulo user
     * 
     * @param userName
     *            Accumulo user name
     * @param permission
     *            Accumulo system permission to revoke
     * @HTTP 200 Success
     * @HTTP 400 Missing required parameter(s) or invalid parameter(s)
     * @HTTP 401 AccumuloSecurityException
     * @HTTP 500 AccumuloException
     * @return datawave.webservice.result.VoidResponse
     */
    @Path("/RevokeSystemPermission/{userName}/{permission}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public VoidResponse revokeSystemPermission(@PathParam("userName") String userName, @PathParam("permission") String permission) {
        VoidResponse response = new VoidResponse();
        Connector connection = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            connection = connectionFactory.getConnection(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
            SecurityOperations ops = connection.securityOperations();
            ops.revokeSystemPermission(userName, SystemPermission.valueOf(permission));
        } catch (AccumuloSecurityException e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new UnauthorizedException(e, response);
        } catch (IllegalArgumentException e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new BadRequestException(e, response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new AccumuloWebApplicationException(e, response);
        } finally {
            try {
                connectionFactory.returnConnection(connection);
            } catch (Exception e) {
                log.error("Error returning connection", e);
            }
        }
        return response;
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Revoke a table permission from an Accumulo user
     * 
     * @param userName
     *            Accumulo user name
     * @param tableName
     *            Accumulo table name
     * @param permission
     *            Accumulo table permission to revoke
     * @HTTP 200 Success
     * @HTTP 400 Missing required parameter(s) or invalid parameter(s)
     * @HTTP 401 AccumuloSecurityException
     * @HTTP 500 AccumuloException
     * @return datawave.webservice.result.VoidResponse
     */
    @Path("/RevokeTablePermission/{userName}/{tableName}/{permission}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public VoidResponse revokeTablePermission(@PathParam("userName") String userName, @PathParam("tableName") String tableName,
                    @PathParam("permission") String permission) {
        VoidResponse response = new VoidResponse();
        Connector connection = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            connection = connectionFactory.getConnection(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
            SecurityOperations ops = connection.securityOperations();
            ops.revokeTablePermission(userName, tableName, TablePermission.valueOf(permission));
        } catch (AccumuloSecurityException e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new UnauthorizedException(e, response);
        } catch (IllegalArgumentException e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new BadRequestException(e, response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new AccumuloWebApplicationException(e, response);
        } finally {
            try {
                connectionFactory.returnConnection(connection);
            } catch (Exception e) {
                log.error("Error returning connection", e);
            }
        }
        return response;
    }
}
