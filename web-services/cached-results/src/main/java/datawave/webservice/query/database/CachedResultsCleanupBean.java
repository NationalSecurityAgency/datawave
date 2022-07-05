package datawave.webservice.query.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.LocalBean;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;
import javax.sql.DataSource;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import datawave.configuration.spring.SpringBean;
import datawave.webservice.results.cached.CachedResultsParameters;
import org.apache.log4j.Logger;

/**
 * Removes tables and views from the MySQL database that have been there for 24 hours so that we don't have to purge data from them.
 */
@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@Startup
// tells the container to initialize on startup
@Singleton
// this is a singleton bean in the container
@LocalBean
@Lock(LockType.WRITE)
// by default all methods are blocking
public class CachedResultsCleanupBean {
    
    private static Logger log = Logger.getLogger(CachedResultsCleanupBean.class);
    
    private static final String GET_TABLES_TO_REMOVE = "select table_name from information_schema.tables where "
                    + " table_name != 'cachedResultsQuery' and table_name != 'template' and table_schema = '?' and " + " table_name like 't%' and "
                    + " create_time < date_sub(sysdate(),interval XYZ day)";
    
    private static final String GET_TABLES_AND_VIEWS = "select table_name from information_schema.tables where "
                    + " table_name != 'cachedResultsQuery' and table_name != 'template' and table_schema = '?' and "
                    + " (table_name like 't%' or table_name like 'v%')";
    
    @Resource(lookup = "java:jboss/datasources/CachedResultsDS")
    private DataSource ds;
    
    // reference "datawave/query/CachedResultsCleanup.xml"
    @Inject
    @SpringBean(refreshable = true)
    protected CachedResultsCleanupConfiguration cachedResultsCleanupConfiguration;
    
    private RowSetFactory rowSetProvider;
    
    @PostConstruct
    public void init() {
        try {
            rowSetProvider = RowSetProvider.newFactory();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Method that is invoked every 30 minutes from the Timer service. This will occur
     */
    @Schedule(hour = "*", minute = "*/30", persistent = false)
    public void cleanup() {
        try (Connection con = ds.getConnection()) {
            
            String schema = con.getCatalog();
            if (null == schema || "".equals(schema))
                throw new RuntimeException("Unable to determine schema from connection");
            try (Statement s = con.createStatement();
                            ResultSet rs = s.executeQuery(GET_TABLES_TO_REMOVE.replace("?", schema).replace("XYZ",
                                            Integer.toString(cachedResultsCleanupConfiguration.getDaysToLive())))) {
                while (rs.next()) {
                    String objectName = CachedResultsParameters.validate(rs.getString(1));
                    // Drop the table
                    String dropTable = "DROP TABLE " + objectName;
                    try (Statement statement = con.createStatement()) {
                        statement.execute(dropTable);
                    }
                    removeCrqRow(objectName);
                    
                    String viewName = CachedResultsParameters.validate(objectName.replaceFirst("t", "v"));
                    // Drop the associated view
                    String dropView = "DROP VIEW " + viewName;
                    try (Statement statement = con.createStatement()) {
                        statement.execute(dropView);
                    }
                    removeCrqRow(viewName);
                }
            }
        } catch (SQLException e) {
            log.error("Error cleaning up cached result objects: " + e.getMessage());
        }
    }
    
    private void removeCrqRow(String id) {
        
        try (Connection con = ds.getConnection();
                        PreparedStatement ps = con.prepareStatement("DELETE FROM cachedResultsQuery WHERE tableName = ? OR view = ?")) {
            ps.setString(1, id);
            ps.setString(2, id);
            ps.execute();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    public Set<String> getAllCrqTablesViews() {
        
        Set<String> tableViewSet = new HashSet<>();
        
        try (Connection connection = ds.getConnection(); CachedRowSet crs = rowSetProvider.createCachedRowSet()) {
            
            crs.setCommand("SELECT tableName, view FROM cachedResultsQuery");
            crs.execute(connection);
            
            crs.last();
            int numRows = crs.getRow();
            
            for (int row = 1; row <= numRows; row++) {
                
                crs.absolute(row);
                String table = crs.getString(1);
                if (table != null) {
                    tableViewSet.add(table);
                }
                
                String view = crs.getString(2);
                if (view != null) {
                    tableViewSet.add(view);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return tableViewSet;
    }
    
    public Set<String> getAllTables() {
        
        Set<String> tableSet = new HashSet<>();
        
        try (Connection connection = ds.getConnection(); CachedRowSet crs = rowSetProvider.createCachedRowSet()) {
            
            String schema = connection.getCatalog();
            if (null == schema || "".equals(schema))
                throw new RuntimeException("Unable to determine schema from connection");
            
            String cmd = GET_TABLES_AND_VIEWS.replace("?", schema);
            crs.setCommand(cmd);
            crs.execute(connection);
            
            crs.last();
            int numRows = crs.getRow();
            
            for (int row = 1; row <= numRows; row++) {
                crs.absolute(row);
                String table = crs.getString(1);
                
                if (table != null) {
                    
                    tableSet.add(table);
                }
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        return tableSet;
    }
}
