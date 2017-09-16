package datawave.webservice.operations.user;

import javax.annotation.security.PermitAll;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.exception.AccumuloWebApplicationException;
import datawave.webservice.exception.UnauthorizedException;
import datawave.webservice.response.ListTablesResponse;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

@Path("/Accumulo")
@RolesAllowed({"InternalUser", "Administrator"})
@DeclareRoles({"InternalUser", "Administrator"})
@LocalBean
@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class ListTablesBean {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @EJB
    private AccumuloConnectionFactory connectionFactory;
    
    /**
     * List of tables that exist in Accumulo (Requires Administrator role)
     *
     * @RequestHeader X-ProxiedEntitiesChain (optional) for server calls on behalf of a user: &lt;subjectDN&gt;
     * @RequestHeader X-ProxiedIssuersChain (optional unless X-ProxiedEntitesChain is specified) contains one &lt;issuerDN&gt; per &lt;subjectDN&gt; in
     *                X-ProxiedEntitesChain
     * @HTTP 200 Success
     * @HTTP 401 AccumuloSecurityException
     * @HTTP 500 AccumuloException
     * @return datawave.webservice.response.ListTablesResponse
     */
    @Path("/ListTables")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @GET
    public ListTablesResponse listAccumuloTables() {
        return listTables();
    }
    
    @PermitAll
    public ListTablesResponse listTables() {
        
        ListTablesResponse response = new ListTablesResponse();
        Connector connection = null;
        AccumuloConnectionFactory.Priority priority = AccumuloConnectionFactory.Priority.ADMIN;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            connection = connectionFactory.getConnection(priority, trackingMap);
            
            TableOperations ops = connection.tableOperations();
            SortedSet<String> availableTables = ops.list();
            
            List<String> tables = new ArrayList<>();
            tables.addAll(availableTables);
            response.setTables(tables);
            
            return response;
        } catch (AccumuloSecurityException e) {
            log.error(e.getMessage(), e);
            response.addException(e);
            throw new UnauthorizedException(e, response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.addException(e);
            throw new AccumuloWebApplicationException(e, response);
        } finally {
            try {
                connectionFactory.returnConnection(connection);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
    
}
