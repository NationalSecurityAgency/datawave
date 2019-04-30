package datawave.microservice.accumulo.admin;

import datawave.microservice.accumulo.TestHelper;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.webservice.query.util.OptionallyEncodedString;
import datawave.webservice.request.UpdateRequest;
import datawave.webservice.request.objects.Mutation;
import datawave.webservice.request.objects.MutationEntry;
import datawave.webservice.request.objects.TableUpdate;
import datawave.webservice.response.ListTablesResponse;
import datawave.webservice.response.ListUserAuthorizationsResponse;
import datawave.webservice.response.ListUserPermissionsResponse;
import datawave.webservice.response.ListUsersResponse;
import datawave.webservice.response.UpdateResponse;
import datawave.webservice.response.ValidateVisibilityResponse;
import datawave.webservice.response.objects.SystemPermission;
import datawave.webservice.response.objects.SystemPermission.SystemPermissionType;
import datawave.webservice.result.VoidResponse;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.RequestEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.junit.Assert.*;

/**
 * These tests exercise the endpoints defined by the AdminController, and thus the respective methods of the underlying AdminService delegate are tested as
 * well. Leverages the "mock" profile to provide an in-memory Accumulo instance
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "spring.main.allow-bean-definition-overriding=true")
@ComponentScan(basePackages = "datawave.microservice")
@ActiveProfiles({"mock", "admin-service-enabled"})
public class AdminServiceTest {
    
    @LocalServerPort
    private int webServicePort;
    
    @Autowired
    private RestTemplateBuilder restTemplateBuilder;
    
    @Autowired
    private ApplicationContext context;
    
    @Autowired
    @Qualifier("warehouse")
    private Connector warehouseConnector;
    
    private JWTRestTemplate jwtRestTemplate;
    private ProxiedUserDetails defaultUserDetails;
    private String defaultAccumuloUser;
    private TestHelper th;
    
    @Before
    public void setup() {
        // REST api user must have Administrator role
        defaultUserDetails = TestHelper.userDetails(Collections.singleton("Administrator"), null);
        jwtRestTemplate = restTemplateBuilder.build(JWTRestTemplate.class);
        defaultAccumuloUser = "root";
        
        th = new TestHelper(jwtRestTemplate, defaultUserDetails, webServicePort, "/accumulo/v1/admin");
    }
    
    @Test
    public void verifyAutoConfig() {
        assertTrue("adminService bean not found", context.containsBean("adminService"));
        assertTrue("adminController bean not found", context.containsBean("adminController"));
        
        assertFalse("auditServiceConfiguration bean should not have been found", context.containsBean("auditServiceConfiguration"));
        assertFalse("auditServiceInstanceProvider bean should not have been found", context.containsBean("auditServiceInstanceProvider"));
        assertFalse("auditLookupSecurityMarking bean should not have been found", context.containsBean("auditLookupSecurityMarking"));
        assertFalse("lookupService bean should not have been found", context.containsBean("lookupService"));
        assertFalse("lookupController bean should not have been found", context.containsBean("lookupController"));
        assertFalse("statsService bean should not have been found", context.containsBean("statsService"));
        assertFalse("statsController bean should not have been found", context.containsBean("statsController"));
    }
    
    /**
     * Tests both /grantSystemPermission/{userName}/{permission} and /listUserPermissions/{userName}
     */
    @Test
    public void testGrantAndListSystemPermissions() {
        grantSystemPermission(defaultUserDetails, defaultAccumuloUser, SystemPermissionType.GRANT.name());
        grantSystemPermission(defaultUserDetails, defaultAccumuloUser, SystemPermissionType.CREATE_TABLE.name());
        grantSystemPermission(defaultUserDetails, defaultAccumuloUser, SystemPermissionType.ALTER_NAMESPACE.name());
        grantSystemPermission(defaultUserDetails, defaultAccumuloUser, SystemPermissionType.ALTER_TABLE.name());
        grantSystemPermission(defaultUserDetails, defaultAccumuloUser, SystemPermissionType.ALTER_USER.name());
        grantSystemPermission(defaultUserDetails, defaultAccumuloUser, SystemPermissionType.CREATE_NAMESPACE.name());
        grantSystemPermission(defaultUserDetails, defaultAccumuloUser, SystemPermissionType.CREATE_USER.name());
        grantSystemPermission(defaultUserDetails, defaultAccumuloUser, SystemPermissionType.DROP_NAMESPACE.name());
        grantSystemPermission(defaultUserDetails, defaultAccumuloUser, SystemPermissionType.DROP_TABLE.name());
        
        // Verify all permissions are returned from ListUserPermissions endpoint
        
        //@formatter:off
        ListUserPermissionsResponse response = th.assert200Status(
            th.createGetRequest("/listUserPermissions/" + defaultAccumuloUser),
            ListUserPermissionsResponse.class);
        //@formatter:on
        
        assertNotNull(response);
        assertNotNull(response.getUserPermissions());
        assertNotNull(response.getUserPermissions().getSystemPermissions());
        
        List<SystemPermission> spl = response.getUserPermissions().getSystemPermissions();
        assertEquals(1, spl.stream().filter(sp -> sp.getPermission().equals(SystemPermissionType.GRANT)).count());
        assertEquals(1, spl.stream().filter(sp -> sp.getPermission().equals(SystemPermissionType.CREATE_TABLE)).count());
        assertEquals(1, spl.stream().filter(sp -> sp.getPermission().equals(SystemPermissionType.ALTER_NAMESPACE)).count());
        assertEquals(1, spl.stream().filter(sp -> sp.getPermission().equals(SystemPermissionType.ALTER_TABLE)).count());
        assertEquals(1, spl.stream().filter(sp -> sp.getPermission().equals(SystemPermissionType.ALTER_USER)).count());
        assertEquals(1, spl.stream().filter(sp -> sp.getPermission().equals(SystemPermissionType.CREATE_NAMESPACE)).count());
        assertEquals(1, spl.stream().filter(sp -> sp.getPermission().equals(SystemPermissionType.CREATE_USER)).count());
        assertEquals(1, spl.stream().filter(sp -> sp.getPermission().equals(SystemPermissionType.DROP_NAMESPACE)).count());
        assertEquals(1, spl.stream().filter(sp -> sp.getPermission().equals(SystemPermissionType.DROP_TABLE)).count());
    }
    
    private VoidResponse grantSystemPermission(ProxiedUserDetails userDetails, String accumuloUser, String permission) {
        String path = "/grantSystemPermission/" + accumuloUser + "/" + permission;
        return th.assert200Status(th.createPostRequest(userDetails, path, null), VoidResponse.class);
    }
    
    /**
     * Tests both /createTable/{tableName} and /listTables endpoints
     */
    @Test
    public void testCreateTableAndListTables() {
        
        final String newTable = "testCreateTableAndListTables";
        
        // Test /CreateTable/{tableName}
        th.assert200Status(th.createPostRequest("/createTable/" + newTable, null), VoidResponse.class);
        
        // Test /ListTables
        ListTablesResponse response = th.assert200Status(th.createGetRequest("/listTables"), ListTablesResponse.class);
        
        assertNotNull(response);
        assertNotNull(response.getTables());
        
        // Verify
        assertTrue(response.getTables().contains(newTable));
    }
    
    /**
     * Tests the /listUserAuthorizations endpoint
     */
    @Test
    public void testListUserAuthorizations() throws Exception {
        
        // Create a new Accumulo user and assign some auths
        String testUser = "testListUserAuthorizations";
        SecurityOperations so = warehouseConnector.securityOperations();
        so.createLocalUser(testUser, new PasswordToken("test"));
        so.changeUserAuthorizations(testUser, new Authorizations("A", "B", "C", "D", "E", "F"));
        
        // Retrieve the user's auths
        ListUserAuthorizationsResponse response = th.assert200Status(th.createGetRequest("/listUserAuthorizations/" + testUser),
                        ListUserAuthorizationsResponse.class);
        
        // Verify
        assertNotNull(response);
        assertNotNull(response.getUserAuthorizations());
        assertEquals(6, response.getUserAuthorizations().size());
        assertTrue(response.getUserAuthorizations().contains("A"));
        assertTrue(response.getUserAuthorizations().contains("B"));
        assertTrue(response.getUserAuthorizations().contains("C"));
        assertTrue(response.getUserAuthorizations().contains("D"));
        assertTrue(response.getUserAuthorizations().contains("E"));
        assertTrue(response.getUserAuthorizations().contains("F"));
    }
    
    /**
     * Tests the /listUsers endpoint
     */
    @Test
    public void testListUsers() {
        ListUsersResponse response = th.assert200Status(th.createGetRequest("/listUsers"), ListUsersResponse.class);
        
        assertNotNull(response);
        assertNotNull(response.getUsers());
        
        List<String> users = response.getUsers();
        
        assertTrue(users.contains("root"));
    }
    
    /**
     * Tests the /flushTable endpoint
     */
    @Test
    public void testFlushTable() {
        th.assert200Status(th.createPostRequest("/flushTable/accumulo.metadata", null), VoidResponse.class);
    }
    
    /**
     * Tests both the setTableProperty/{tableName}/{propertyName}/{propertyValue} and removeTableProperty/{tableName}/{propertyName} endpoints
     */
    @Test
    public void testSetAndRemoveTableProperties() throws Exception {
        final String testTable = "testSetTableProperty";
        warehouseConnector.tableOperations().create(testTable);
        
        final String propKey = "datawave.test.foo";
        final String propVal = "testValue";
        
        // Test SetTableProperty/{tableName}/{propertyName}/{propertyValue}
        String path = String.format("/setTableProperty/%s/%s/%s", testTable, propKey, propVal);
        th.assert200Status(th.createPostRequest(path, null), VoidResponse.class);
        
        Iterable<Map.Entry<String,String>> props = warehouseConnector.tableOperations().getProperties(testTable);
        
        //@formatter:off
        assertEquals(1, StreamSupport.stream(props.spliterator(), false).
            filter(e -> e.getKey().equals(propKey) && e.getValue().equals(propVal)).count());
        //@formatter:on
        
        // Test RemoveTableProperty/{tableName}/{propertyName}
        path = String.format("/removeTableProperty/%s/%s", testTable, propKey);
        th.assert200Status(th.createPostRequest(path, null), VoidResponse.class);
        
        props = warehouseConnector.tableOperations().getProperties(testTable);
        
        //@formatter:off
        assertEquals(0, StreamSupport.stream(props.spliterator(), false).
            filter(e -> e.getKey().equals(propKey) && e.getValue().equals(propVal)).count());
        //@formatter:on
    }
    
    /**
     * Tests the /update endpoint by writing 2 mutations to a new table
     */
    @Test
    public void testUpdate() throws Exception {
        
        // First, create a new table...
        final String testTable = "testUpdateTable";
        TableOperations tops = warehouseConnector.tableOperations();
        tops.create(testTable);
        
        assertTrue("Table wasn't created as expected", tops.exists(testTable));
        
        // Create an UpdateRequest with 2 mutations
        UpdateRequest request = createUpdateRequest(testTable);
        
        // Use AdminController to write the mutations...
        UpdateResponse response = th.assert200Status(th.createPutRequest("/update", request), UpdateResponse.class);
        
        assertNotNull("UpdateResponse should not have been NULL", response);
        
        // Verify the correct number of mutations were written..
        
        assertEquals("MutationsAccepted should have been 2", 2, response.getMutationsAccepted().intValue());
        assertEquals("MutationsDenied should have been 0", 0, response.getMutationsDenied().intValue());
        assertNull("TableNotFoundList should have been NULL", response.getTableNotFoundList());
        assertNull("AuthorizationFailures should have been NULL", response.getAuthorizationFailures());
    }
    
    /**
     * Tests /validateVisibilities endpoint
     */
    @Test
    public void testValidateVisibilities() {
        
        // 3 good ones, one bad
        final String[] visibilities = new String[] {"A|(B&C&D&E&F)", "A|B|C", "A&B&C", "THIS:^IS||N@T%VALID"};
        final LinkedMultiValueMap<String,String> requestParam = new LinkedMultiValueMap<>();
        Arrays.stream(visibilities).forEach(s -> requestParam.add("visibility", s));
        RequestEntity<?> request = th.createPostRequest("/validateVisibilities", requestParam);
        ValidateVisibilityResponse response = th.assert200Status(request, ValidateVisibilityResponse.class);
        
        //@formatter:off
        assertNotNull(response);
        assertEquals("There should have been 4 visibilities in the response",
                4, response.getVisibilityList().size());
        assertEquals("There should have been 3 valid visibilities in the response",
                3, response.getVisibilityList().stream().filter(v -> v.getValid()).count());
        //@formatter:on
    }
    
    private UpdateRequest createUpdateRequest(String tableName) {
        
        final TableUpdate tableUpdate = new TableUpdate();
        tableUpdate.setTable(tableName);
        
        final OptionallyEncodedString row = new OptionallyEncodedString();
        row.setValue("row");
        
        final Mutation mutation = new Mutation();
        mutation.setRow(row);
        
        final List<MutationEntry> mutationEntries = new ArrayList<>(2);
        
        final OptionallyEncodedString colFam1 = new OptionallyEncodedString();
        final OptionallyEncodedString colQual1 = new OptionallyEncodedString();
        final OptionallyEncodedString value1 = new OptionallyEncodedString();
        
        final OptionallyEncodedString colFam2 = new OptionallyEncodedString();
        final OptionallyEncodedString colQual2 = new OptionallyEncodedString();
        final OptionallyEncodedString value2 = new OptionallyEncodedString();
        
        colFam1.setValue("cf1");
        colQual1.setValue("cq1");
        value1.setValue("value1");
        
        colFam2.setValue("cf2");
        colQual2.setValue("cq2");
        value2.setValue("value2");
        
        MutationEntry mutationEntry = new MutationEntry();
        
        mutationEntry.setColFam(colFam1);
        mutationEntry.setColQual(colQual1);
        mutationEntry.setValue(value1);
        mutationEntry.setVisibility("A&B&C");
        
        mutationEntries.add(mutationEntry);
        
        mutationEntry = new MutationEntry();
        mutationEntry.setColFam(colFam2);
        mutationEntry.setColQual(colQual2);
        mutationEntry.setValue(value2);
        mutationEntry.setVisibility("D&E&F");
        
        mutationEntries.add(mutationEntry);
        
        mutation.setMutationEntries(mutationEntries);
        
        tableUpdate.setMutations(Collections.singletonList(mutation));
        
        final UpdateRequest request = new UpdateRequest();
        request.setTableUpdates(Collections.singletonList(tableUpdate));
        
        return request;
    }
    
    /**
     * Tests AdminController with non-existent user
     */
    @Test(expected = HttpServerErrorException.class)
    public void testUnknownAccumuloUser() {
        grantSystemPermission(defaultUserDetails, "thisuserdoesnotexist", SystemPermissionType.CREATE_TABLE.name());
    }
    
    /**
     * Tests AdminController with unauthorized user
     */
    @Test
    public void testUnauthorizedDatawaveUser() {
        // Requires Administrator role...
        ProxiedUserDetails unauthorizedUser = TestHelper.userDetails(Collections.singleton("AuthorizedUser"), null);
        try {
            grantSystemPermission(unauthorizedUser, defaultAccumuloUser, SystemPermissionType.CREATE_TABLE.name());
            fail("This test should have thrown HttpClientErrorException with 403 status");
        } catch (HttpClientErrorException hcee) {
            assertEquals("Test should have returned 403 status", 403, hcee.getStatusCode().value());
        }
    }
}
