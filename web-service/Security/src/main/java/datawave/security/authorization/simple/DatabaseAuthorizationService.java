package datawave.security.authorization.simple;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Resource;
import javax.inject.Inject;
import javax.sql.DataSource;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.security.authorization.AuthorizationService;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;

@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class DatabaseAuthorizationService implements AuthorizationService {
    
    private static Logger log = Logger.getLogger(DatabaseAuthorizationService.class);
    
    @Resource(lookup = "java:jboss/datasources/DatabaseAuthorizationServiceDS")
    protected DataSource ds;
    
    @Inject
    @ConfigProperty(name = "dw.databaseAuthorizationService.tableName", defaultValue = "userAuthorizations")
    private String tableName;
    @Inject
    @ConfigProperty(name = "dw.databaseAuthorizationService.userNameColumn", defaultValue = "userName")
    private String userNameColumn;
    @Inject
    @ConfigProperty(name = "dw.databaseAuthorizationService.userAuthColumn", defaultValue = "userAuths")
    private String userAuthColumn;
    
    @Override
    public String[] getRoles(String projectName, String userDN, String issuerDN) {
        
        String userAuthString = null;
        try (Connection connection = ds.getConnection();
                        PreparedStatement ps = connection.prepareStatement("SELECT " + userAuthColumn + " FROM " + tableName + " WHERE " + userNameColumn
                                        + " = ?")) {
            ps.setString(1, userDN);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    userAuthString = rs.getString(1);
                }
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        
        return (userAuthString == null) ? new String[0] : userAuthString.split("\\s*,\\s*");
    }
}
