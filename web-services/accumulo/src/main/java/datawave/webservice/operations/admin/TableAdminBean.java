package datawave.webservice.operations.admin;

import java.util.Map;

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

import datawave.annotation.Required;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.exception.AccumuloWebApplicationException;
import datawave.webservice.exception.ConflictException;
import datawave.webservice.exception.UnauthorizedException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.log4j.Logger;

@Path("/Accumulo")
@RolesAllowed({"InternalUser", "Administrator"})
@DeclareRoles({"InternalUser", "Administrator"})
@LocalBean
@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class TableAdminBean {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @EJB
    private AccumuloConnectionFactory connectionFactory;
    
    /**
     * <strong>Administrator credentials required.</strong> Create a Accumulo table
     * 
     * @param tableName
     *            Accumulo table name
     * @HTTP 200 Success
     * @HTTP 400 Missing required parameter(s) or invalid parameter(s)
     * @HTTP 401 AccumuloSecurityException
     * @HTTP 409 Table already exists
     * @HTTP 500 AccumuloException
     * @return datawave.webservice.result.VoidResponse
     */
    @Path("/CreateTable/{tableName}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public VoidResponse createTable(@Required("tableName") @PathParam("tableName") String tableName) {
        VoidResponse response = new VoidResponse();
        Connector connection = null;
        
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            connection = connectionFactory.getConnection(AccumuloConnectionFactory.Priority.NORMAL, trackingMap);
            TableOperations ops = connection.tableOperations();
            ops.create(tableName);
        } catch (TableExistsException e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new ConflictException(e, response);
        } catch (AccumuloSecurityException e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new UnauthorizedException(e, response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new AccumuloWebApplicationException(e, response);
        } finally {
            try {
                connectionFactory.returnConnection(connection);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        return response;
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Flush the memory buffer of a named table to disk (minor compaction)
     * 
     * @param tableName
     *            Accumulo table name
     * @HTTP 200 Success
     * @HTTP 400 Missing required parameter(s) or invalid parameter(s)
     * @HTTP 401 AccumuloSecurityException
     * @HTTP 500 AccumuloException
     * @returnWrapped datawave.webservice.result.VoidResponse
     */
    @Path("/FlushTable/{tableName}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public VoidResponse flushTable(@PathParam("tableName") String tableName) {
        VoidResponse response = new VoidResponse();
        Connector connection = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            connection = connectionFactory.getConnection(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
            TableOperations ops = connection.tableOperations();
            ops.flush(tableName, null, null, false);
        } catch (AccumuloSecurityException e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new UnauthorizedException(e, response);
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
     * <strong>Administrator credentials required.</strong> Set a TableProperty on a Accumulo table
     * 
     * @param tableName
     *            Accumulo table name
     * @param propertyName
     *            table property to remove
     * @param propertyValue
     *            value of table property
     * @HTTP 200 Success
     * @HTTP 401 AccumuloSecurityException
     * @HTTP 500 AccumuloException
     * @returnWrapped datawave.webservice.result.VoidResponse
     */
    @Path("/SetTableProperty/{tableName}/{propertyName}/{propertyValue}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public VoidResponse setTableProperty(@PathParam("tableName") String tableName, @PathParam("propertyName") String propertyName,
                    @PathParam("propertyValue") String propertyValue) {
        VoidResponse response = new VoidResponse();
        Connector connection = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            connection = connectionFactory.getConnection(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
            TableOperations ops = connection.tableOperations();
            ops.setProperty(tableName, propertyName, propertyValue);
        } catch (AccumuloSecurityException e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new UnauthorizedException(e, response);
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
     * <strong>Administrator credentials required.</strong> Remove a TableProperty from a Accumulo table
     * 
     * @param tableName
     *            Accumulo table name
     * @param propertyName
     *            Accumulo table property to remove
     * @HTTP 200 Success
     * @HTTP 401 AccumuloSecurityException
     * @HTTP 500 AccumuloException
     * @returnWrapped datawave.webservice.result.VoidResponse
     */
    @Path("/RemoveTableProperty/{tableName}/{propertyName}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public VoidResponse removeTableProperty(@PathParam("tableName") String tableName, @PathParam("propertyName") String propertyName) {
        VoidResponse response = new VoidResponse();
        Connector connection = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            connection = connectionFactory.getConnection(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
            TableOperations ops = connection.tableOperations();
            ops.removeProperty(tableName, propertyName);
        } catch (AccumuloSecurityException e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e));
            throw new UnauthorizedException(e, response);
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
