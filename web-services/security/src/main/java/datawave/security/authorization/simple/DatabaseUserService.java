package datawave.security.authorization.simple;

import com.google.common.collect.HashMultimap;
import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.util.StringUtils;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.inject.Inject;
import javax.sql.DataSource;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * A {@link DatawaveUserService} that retrieves {@link DatawaveUser} objects from a SQL database. This login module expects the supplied {@link DataSource} to
 * contain two tables: users (name is customizable by setting the dw.databaseUsersService.usersTableName property) and roleToAuthMapping (name is customizable
 * by setting the dw.databaseUsersService.mappingTableName property). The expected structure of the users table is:
 * <table border="1">
 * <caption></caption>
 * <tr>
 * <th>Column Name</th>
 * <th>Column Type</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>subjectDN</td>
 * <td>String</td>
 * <td>User's certificate subject DN</td>
 * </tr>
 * <tr>
 * <td>issuerDN</td>
 * <td>String</td>
 * <td>User's certificate issuer DN</td>
 * </tr>
 * <tr>
 * <td>userType</td>
 * <td>String</td>
 * <td>The type of user, parse-able by {@link UserType#valueOf(String)}</td>
 * </tr>
 * <tr>
 * <td>roles</td>
 * <td>String</td>
 * <td>Comma-separated list of the roles attributed to this user.</td>
 * </tr>
 * <tr>
 * <td>auths</td>
 * <td>String</td>
 * <td>Comma-separated list of the Accumulo auths attributed to this user.</td>
 * </tr>
 * </table>
 * <p>
 * The roleToAuthMapping table contains the mappings of roles seen in the roles column of the users table into Accumulo auths that appear in the auths column of
 * the users table. The expected structure of this table is:
 * <table border="1">
 * <caption></caption>
 * <tr>
 * <th>Column Name</th>
 * <th>Column Type</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>role</td>
 * <td>String</td>
 * <td>The role that is mapped to an Accumulo authorization</td>
 * </tr>
 * <tr>
 * <td>auth</td>
 * <td>String</td>
 * <td>The Accumulo auth that was mapped from the value in the role column</td>
 * </tr>
 * </table>
 */
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class DatabaseUserService implements DatawaveUserService {
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    @Resource(lookup = "java:jboss/datasources/DatabaseUserServiceDS")
    protected DataSource ds;
    
    protected final HashMultimap<String,String> roleToAuthorizationMap;
    
    private final String usersTableName;
    private final String mappingTableName;
    
    /**
     * Constructs a new DatabaseUserService.
     *
     * @param usersTableName
     *            the name of the table that contains user information (subject/issuer DN, user type, roles, and auths)
     * @param mappingTableName
     *            the name of the table that contains mapping from roles to authorization (for populating {@link DatawaveUser}s)
     */
    @Inject
    public DatabaseUserService(@ConfigProperty(name = "dw.databaseUsersService.usersTableName", defaultValue = "users") String usersTableName, @ConfigProperty(
                    name = "dw.databaseUsersService.mappingTableName", defaultValue = "roleToAuthMapping") String mappingTableName) {
        this.usersTableName = usersTableName;
        this.mappingTableName = mappingTableName;
        this.roleToAuthorizationMap = HashMultimap.create();
    }
    
    @PostConstruct
    public void setup() {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT role, auth FROM " + mappingTableName)) {
            while (rs.next()) {
                roleToAuthorizationMap.put(rs.getString("role"), rs.getString("auth"));
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException("Unable to read roleToAuthorizationMap.", e);
        }
    }
    
    @Override
    public Collection<DatawaveUser> lookup(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException {
        try (Connection c = ds.getConnection();
                        PreparedStatement ps = c.prepareStatement("SELECT * from " + usersTableName + " where subjectDN = ? and issuerDN = ?")) {
            ArrayList<DatawaveUser> users = new ArrayList<>();
            for (SubjectIssuerDNPair dn : dns) {
                users.add(lookup(ps, dn));
            }
            return users;
        } catch (SQLException e) {
            throw new AuthorizationException("Unable to lookup users " + dns + ": " + e.getMessage(), e);
        }
    }
    
    private DatawaveUser lookup(PreparedStatement ps, SubjectIssuerDNPair dn) throws AuthorizationException {
        try {
            ps.setString(1, dn.subjectDN());
            ps.setString(2, dn.issuerDN());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    UserType userType = UserType.valueOf(rs.getString("userType"));
                    Collection<String> roles = Arrays.asList(StringUtils.split(rs.getString("roles"), "\\s*,\\s*"));
                    Collection<String> auths = Arrays.asList(StringUtils.split(rs.getString("auths"), "\\s*,\\s*"));
                    HashMultimap<String,String> map = HashMultimap.create();
                    roles.forEach(r -> map.putAll(r, roleToAuthorizationMap.get(r)));
                    return new DatawaveUser(dn, userType, auths, roles, map, System.currentTimeMillis());
                } else {
                    throw new AuthorizationException("No user found for " + dn);
                }
            }
        } catch (SQLException e) {
            throw new AuthorizationException("Unable to retrieve user for " + dn, e);
        }
    }
}
