package datawave.security.authorization.simple;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.inject.Inject;
import javax.sql.DataSource;

import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;

import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.SubjectIssuerDNPair;

/**
 * A {@link DatawaveUserService} that retrieves {@link DatawaveUser} objects from a SQL database. This login module expects the supplied {@link DataSource} to
 * contain two tables: users (name is customizable by setting the dw.databaseUsersService.usersTableName property) and roleToAuthMapping (name is customizable
 * by setting the dw.databaseUsersService.mappingTableName property). The expected structure of the users table is:
 * <table border="1">
 * <caption>User data table</caption>
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
 * <caption>Role to Auth Mapping table</caption>
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
public class DatabaseUserService implements DatawaveUserService {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Resource(lookup = "java:jboss/datasources/DatabaseUserServiceDS")
    protected DataSource ds;

    protected final HashMultimap<String,String> roleToAuthorizationMap;

    private final String usersTableName;
    private final String mappingTableName;
    private final String regPattern = "\\s*,\\s*";

    /**
     * Constructs a new DatabaseUserService.
     *
     * @param usersTableName
     *            the name of the table that contains user information (subject/issuer DN, user type, roles, and auths)
     * @param mappingTableName
     *            the name of the table that contains mapping from roles to authorization (for populating {@link DatawaveUser}s)
     */
    @Inject
    public DatabaseUserService(@ConfigProperty(name = "dw.databaseUsersService.usersTableName", defaultValue = "users") String usersTableName,
                    @ConfigProperty(name = "dw.databaseUsersService.mappingTableName", defaultValue = "roleToAuthMapping") String mappingTableName) {
        this.usersTableName = usersTableName;
        this.mappingTableName = mappingTableName;
        this.roleToAuthorizationMap = HashMultimap.create();
    }

    @PostConstruct
    public void setup() {
        log.trace("enter: setup()");
        try (Connection c = ds.getConnection();
                        Statement s = c.createStatement();
                        ResultSet rs = s.executeQuery(String.format("SELECT * from %s", usersTableName))) {
            dumpTable(rs, usersTableName);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (Connection c = ds.getConnection();
                        Statement s = c.createStatement();
                        ResultSet rs = s.executeQuery(String.format("SELECT * from %s", mappingTableName))) {
            dumpTable(rs, mappingTableName);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (Connection c = ds.getConnection();
                        Statement s = c.createStatement();
                        ResultSet rs = s.executeQuery(String.format("SELECT role, auth FROM %s", mappingTableName))) {
            while (rs.next()) {
                roleToAuthorizationMap.put(rs.getString("role"), rs.getString("auth"));
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException("Unable to read roleToAuthorizationMap.", e);
        }
        log.trace("exit: setup()");
    }

    private void dumpTable(ResultSet rs, String tableName) throws SQLException {
        if (log.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            ResultSetMetaData metaData = rs.getMetaData();
            sb.append("Dumping table ").append(tableName).append("\n\n");
            int columns = metaData.getColumnCount();
            List<String> columnNames = new ArrayList<>();
            for (int i = 1; i <= columns; i++) {
                columnNames.add(metaData.getColumnName(i).toLowerCase());
            }
            appendSeparatedByPipes(sb, columnNames);

            while (rs.next()) {
                List<String> rowValues = new ArrayList<>();
                for (String columnName : columnNames) {
                    rowValues.add(rs.getString(columnName));
                }
                appendSeparatedByPipes(sb, rowValues);
            }
            sb.append("\n");
            log.trace(sb.toString());
        }
    }

    private void appendSeparatedByPipes(StringBuilder sb, List<String> items) {
        sb.append("|");
        items.forEach(i -> sb.append("  ").append(i).append("  |"));
        sb.append("\n");
    }

    @Override
    public Collection<DatawaveUser> lookup(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException {
        log.trace("enter: lookup({})", dns);

        try (Connection c = ds.getConnection();
                        PreparedStatement ps = c.prepareStatement(String.format("SELECT * from %s where subjectDN = ? and issuerDN = ?", usersTableName))) {
            ArrayList<DatawaveUser> users = new ArrayList<>();
            for (SubjectIssuerDNPair dn : dns) {
                users.add(lookup(ps, dn));
            }
            return users;
        } catch (SQLException e) {
            throw new AuthorizationException("Unable to lookup users " + dns + ": " + e.getMessage(), e);
        } finally {
            log.trace("exit: lookup(Collection<SubjectIssuerDNPair>))");
        }
    }

    private DatawaveUser lookup(PreparedStatement ps, SubjectIssuerDNPair dn) throws AuthorizationException {
        try {
            ps.setString(1, dn.subjectDN());
            ps.setString(2, dn.issuerDN());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    UserType userType = UserType.valueOf(rs.getString("userType"));
                    Collection<String> roles = Arrays.asList(rs.getString("roles").split(regPattern));
                    Collection<String> auths = Arrays.asList(rs.getString("auths").split(regPattern));
                    HashMultimap<String,String> map = HashMultimap.create();
                    log.info("Laura - Found roles {} and auths {}", roles, auths);
                    roles.forEach(r -> map.putAll(r, roleToAuthorizationMap.get(r)));
                    log.info("Laura - roles after adding auths: {}", roles);
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
