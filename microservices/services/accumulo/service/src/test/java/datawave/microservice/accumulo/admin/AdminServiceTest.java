package datawave.microservice.accumulo.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.RequestEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import datawave.microservice.accumulo.TestHelper;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.DatawaveUserDetails;
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
import datawave.webservice.response.objects.Visibility;
import datawave.webservice.result.VoidResponse;

/**
 * These tests exercise the endpoints defined by the AdminController, and thus the respective methods of the underlying AdminService delegate are tested as
 * well. Leverages the "mock" profile to provide an in-memory Accumulo instance
 */
@ExtendWith(SpringExtension.class)
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
    private AccumuloClient warehouseAccumuloClient;
    
    private DatawaveUserDetails defaultUserDetails;
    private String defaultAccumuloUser;
    private TestHelper th;
    
    @BeforeEach
    public void setup() {
        // REST api user must have Administrator role
        defaultUserDetails = TestHelper.userDetails(Collections.singleton("Administrator"), null);
        JWTRestTemplate jwtRestTemplate = restTemplateBuilder.build(JWTRestTemplate.class);
        defaultAccumuloUser = "root";
        
        th = new TestHelper(jwtRestTemplate, defaultUserDetails, webServicePort, "/accumulo/v1/admin");
    }
    
    @Test
    public void verifyAutoConfig() {
        assertTrue(context.containsBean("adminService"), "adminService bean not found");
        assertTrue(context.containsBean("adminController"), "adminController bean not found");
        
        assertFalse(context.containsBean("auditServiceConfiguration"), "auditServiceConfiguration bean should not have been found");
        assertFalse(context.containsBean("auditServiceInstanceProvider"), "auditServiceInstanceProvider bean should not have been found");
        assertFalse(context.containsBean("auditLookupSecurityMarking"), "auditLookupSecurityMarking bean should not have been found");
        assertFalse(context.containsBean("lookupService"), "lookupService bean should not have been found");
        assertFalse(context.containsBean("lookupController"), "lookupController bean should not have been found");
        assertFalse(context.containsBean("statsService"), "statsService bean should not have been found");
        assertFalse(context.containsBean("statsController"), "statsController bean should not have been found");
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
    
    private VoidResponse grantSystemPermission(DatawaveUserDetails userDetails, String accumuloUser, String permission) {
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
        SecurityOperations so = warehouseAccumuloClient.securityOperations();
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
        warehouseAccumuloClient.tableOperations().create(testTable);
        
        final String propKey = "datawave.test.foo";
        final String propVal = "testValue";
        
        // Test SetTableProperty/{tableName}/{propertyName}/{propertyValue}
        String path = String.format("/setTableProperty/%s/%s/%s", testTable, propKey, propVal);
        th.assert200Status(th.createPostRequest(path, null), VoidResponse.class);
        
        Iterable<Map.Entry<String,String>> props = warehouseAccumuloClient.tableOperations().getProperties(testTable);
        
        //@formatter:off
        assertEquals(1, StreamSupport.stream(props.spliterator(), false).
            filter(e -> e.getKey().equals(propKey) && e.getValue().equals(propVal)).count());
        //@formatter:on
        
        // Test RemoveTableProperty/{tableName}/{propertyName}
        path = String.format("/removeTableProperty/%s/%s", testTable, propKey);
        th.assert200Status(th.createPostRequest(path, null), VoidResponse.class);
        
        props = warehouseAccumuloClient.tableOperations().getProperties(testTable);
        
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
        TableOperations tops = warehouseAccumuloClient.tableOperations();
        tops.create(testTable);
        
        assertTrue(tops.exists(testTable), "Table wasn't created as expected");
        
        // Create an UpdateRequest with 2 mutations
        UpdateRequest request = createUpdateRequest(testTable);
        
        // Use AdminController to write the mutations...
        UpdateResponse response = th.assert200Status(th.createPutRequest("/update", request), UpdateResponse.class);
        
        assertNotNull(response, "UpdateResponse should not have been NULL");
        
        // Verify the correct number of mutations were written..
        
        assertEquals(2, response.getMutationsAccepted().intValue(), "MutationsAccepted should have been 2");
        assertEquals(0, response.getMutationsDenied().intValue(), "MutationsDenied should have been 0");
        assertNull(response.getTableNotFoundList(), "TableNotFoundList should have been NULL");
        assertNull(response.getAuthorizationFailures(), "AuthorizationFailures should have been NULL");
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
        assertEquals(4, response.getVisibilityList().size(), "There should have been 4 visibilities in the response");
        assertEquals(3, response.getVisibilityList().stream().filter(Visibility::getValid).count(),
                "There should have been 3 valid visibilities in the response");
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
    @Test
    public void testUnknownAccumuloUser() {
        assertThrows(HttpServerErrorException.class,
                        () -> grantSystemPermission(defaultUserDetails, "thisuserdoesnotexist", SystemPermissionType.CREATE_TABLE.name()));
    }
    
    /**
     * Tests AdminController with unauthorized user
     */
    @Test
    public void testUnauthorizedDatawaveUser() {
        // Requires Administrator role...
        DatawaveUserDetails unauthorizedUser = TestHelper.userDetails(Collections.singleton("AuthorizedUser"), null);
        try {
            grantSystemPermission(unauthorizedUser, defaultAccumuloUser, SystemPermissionType.CREATE_TABLE.name());
            fail("This test should have thrown HttpClientErrorException with 403 status");
        } catch (HttpClientErrorException hcee) {
            assertEquals(403, hcee.getStatusCode().value(), "Test should have returned 403 status");
        }
    }
}
