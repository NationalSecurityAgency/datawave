package datawave.microservice.accumulo.lookup;

import datawave.microservice.accumulo.TestHelper;
import datawave.microservice.accumulo.mock.MockAccumuloConfiguration;
import datawave.microservice.accumulo.mock.MockAccumuloDataService;
import datawave.microservice.audit.AuditClient;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.webservice.common.audit.Auditor;
import org.apache.accumulo.core.client.Connector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.anything;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/**
 * Tests both {@link AuditClient} functionality as well as lookup-specific audit functionality ({@code accumulo.lookup.enabled=true} and
 * {@code audit-client.enabled=true})
 * <p>
 * Utilizes mocked audit server to verify that expected REST calls are made based on preconfigured audit rules for the lookup service
 * ({@code accumulo.lookup.audit.*})
 * <p>
 * Note that by activating the "mock" profile we get a properly initialized in-memory Accumulo instance with a canned dataset pre-loaded via
 * {@link MockAccumuloConfiguration}
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "spring.main.allow-bean-definition-overriding=true")
@ComponentScan(basePackages = "datawave.microservice")
@ActiveProfiles({"mock", "lookup-with-audit-enabled"})
public class LookupServiceAuditEnabledTest {
    
    public static final String BASE_PATH = "/accumulo/v1";
    
    /**
     * "Expected" base URI to be invoked on the mocked audit server
     */
    private static final String AUDIT_BASE_URI = "http://localhost:11111/audit/v1/audit";
    
    /**
     * String.format template for all expected query strings to be invoked on the mocked audit server
     */
    private static String EXPECTED_AUDIT_QUERYSTRING_FORMAT = "?%s&%s&query=lookup/%s/%s&auths=[A,%%20B,%%20C,%%20D,%%20E,%%20F,%%20G,%%20H,%%20I]&auditUserDN=userdn%%3Cissuerdn%%3E&%s&%s&logicClass=AccumuloLookup";
    
    @LocalServerPort
    private int webServicePort;
    
    @Autowired
    @Qualifier("warehouse")
    private Connector connector;
    
    @Autowired
    private MockAccumuloDataService mockDataService;
    
    private String testTableName;
    
    @Autowired
    private RestTemplateBuilder restTemplateBuilder;
    
    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private LookupService lookupService;
    
    private JWTRestTemplate jwtRestTemplate;
    private MockRestServiceServer mockServer;
    private ProxiedUserDetails defaultUserDetails;
    
    @Before
    public void setup() throws Exception {
        defaultUserDetails = TestHelper.userDetails(Collections.singleton("Administrator"), Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I"));
        jwtRestTemplate = restTemplateBuilder.build(JWTRestTemplate.class);
        testTableName = MockAccumuloDataService.WAREHOUSE_MOCK_TABLE;
        setupMockAuditServer();
    }
    
    @Test
    public void verifyAutoConfig() {
        assertTrue("auditServiceConfiguration bean not found", context.containsBean("auditServiceConfiguration"));
        assertTrue("auditServiceInstanceProvider bean not found", context.containsBean("auditServiceInstanceProvider"));
        assertTrue("auditLookupSecurityMarking bean not found", context.containsBean("auditLookupSecurityMarking"));
        assertTrue("lookupService bean not found", context.containsBean("lookupService"));
        assertTrue("lookupController bean not found", context.containsBean("lookupController"));
        
        assertFalse("statsService bean should not have been found", context.containsBean("statsService"));
        assertFalse("statsController bean should not have been found", context.containsBean("statsController"));
        assertFalse("adminService bean should not have been found", context.containsBean("adminService"));
        assertFalse("adminController bean should not have been found", context.containsBean("adminController"));
    }
    
    @Test
    public void testLookupRow1AndVerifyAuditURI() throws Exception {
        testLookupAndVerifyAuditUriWithSuccess(testTableName, "row1", Auditor.AuditType.ACTIVE);
    }
    
    @Test
    public void testLookupRow2AndVerifyAuditURI() throws Exception {
        testLookupAndVerifyAuditUriWithSuccess(testTableName, "row2", Auditor.AuditType.PASSIVE);
    }
    
    @Test
    public void testLookupRow3AndVerifyAuditURI() throws Exception {
        testLookupAndVerifyAuditUriWithSuccess(testTableName, "row3", Auditor.AuditType.ACTIVE);
    }
    
    @Test
    public void testLookupAndVerifyAuditTypeNONE() throws Exception {
        // Create new table, which will have no audit rule defined and thus default to
        // AuditType.NONE, i.e., accumulo.lookup.audit.defaultColumnVisibility = NONE.
        // AuditClient should avoid REST calls to the audit server when audit type is NONE
        
        testTableName = "tableWithNoAuditRule";
        
        mockDataService.setupMockTable(connector, testTableName);
        
        // So, the strategy here is simply to setup URI expectations for successful 'ACTIVE',
        // 'PASSIVE' and 'NONE' audit requests, and then ensure that 0 requests actually occurred
        
        // Verify zero 'ACTIVE' audit requests
        
        try {
            mockServer.expect(anything());
            testLookupAndVerifyAuditUriWithSuccess(testTableName, "row3", Auditor.AuditType.ACTIVE);
            fail("This code should never be reached");
        } catch (AssertionError ae) {
            assertTrue("Unexpected exception message", ae.getMessage().contains("\n0 request(s) executed"));
        }
        
        // Verify zero 'PASSIVE' audit requests
        
        setupMockAuditServer(); // must re-init
        try {
            mockServer.expect(anything());
            testLookupAndVerifyAuditUriWithSuccess(testTableName, "row3", Auditor.AuditType.PASSIVE);
            fail("This code should never be reached");
        } catch (AssertionError ae) {
            assertTrue("Unexpected exception message", ae.getMessage().contains("\n0 request(s) executed"));
        }
        
        // Verify zero 'NONE' audit requests
        
        setupMockAuditServer(); // must re-init
        try {
            mockServer.expect(anything());
            testLookupAndVerifyAuditUriWithSuccess(testTableName, "row3", Auditor.AuditType.NONE);
            fail("This code should never be reached");
        } catch (AssertionError ae) {
            assertTrue("Unexpected exception message", ae.getMessage().contains("\n0 request(s) executed"));
        }
    }
    
    @Test
    public void testErrorOnMissingColVizParam() {
        ProxiedUserDetails userDetails = TestHelper.userDetails(Collections.singleton("Administrator"), Arrays.asList("A"));
        String queryString = TestHelper.queryString("NotColumnVisibility=foo");
        try {
            doLookup(userDetails, path(testTableName + "/row2"), queryString);
            fail("This code should never be reached");
        } catch (HttpClientErrorException ex) {
            assertEquals("Test should have returned 400 status", 400, ex.getStatusCode().value());
        } catch (Throwable t) {
            t.printStackTrace();
            fail("Unexpected throwable type was caught");
        }
    }
    
    private void testLookupAndVerifyAuditUriWithSuccess(String targetTable, String targetRow, Auditor.AuditType expectedAuditType) throws Exception {
        
        String queryUseAuths = "useAuthorizations=A,C,E,G,I";
        String queryColumnViz = "columnVisibility=foo";
        
        String expectedAuditTypeString = "auditType=" + expectedAuditType.name();
        String expectedAuditColVizString = "auditColumnVisibility=foo";
        
        //@formatter:off
        String expectedAuditUri = String.format(
            AUDIT_BASE_URI + EXPECTED_AUDIT_QUERYSTRING_FORMAT,
                queryUseAuths,
                queryColumnViz,
                targetTable,
                targetRow,
                expectedAuditTypeString,
                expectedAuditColVizString);
        //@formatter:on
        
        mockServer.expect(requestTo(expectedAuditUri)).andRespond(withSuccess());
        
        String queryString = TestHelper.queryString(queryUseAuths, queryColumnViz);
        doLookup(defaultUserDetails, path(targetTable + "/" + targetRow), queryString);
        
        mockServer.verify();
    }
    
    private String path(String pathParams) {
        return BASE_PATH + "/lookup/" + pathParams;
    }
    
    /**
     * Mocks the AuditClient jwtRestTemplate field within the internal AuditClient employed by our LookupService instance
     */
    private void setupMockAuditServer() {
        // Here we're mocking the jwtRestTemplate field within the AuditClient instance
        // owned by our lookupService, i.e., lookupService.auditor.jwtRestTemplate
        //@formatter:off
        RestTemplate auditorRestTemplate = (RestTemplate)
            new DirectFieldAccessor(
                new DirectFieldAccessor(
                    lookupService
                ).getPropertyValue("auditor")
            ).getPropertyValue("jwtRestTemplate");
        //@formatter:on
        mockServer = MockRestServiceServer.createServer(auditorRestTemplate);
    }
    
    /**
     * Lookups here should return one or more valid Accumulo table entries. If not, an exception is thrown
     */
    private ResponseEntity<String> doLookup(ProxiedUserDetails authUser, String path, String query) throws Exception {
        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path(path).query(query).build();
        ResponseEntity<String> entity = jwtRestTemplate.exchange(authUser, HttpMethod.GET, uri, String.class);
        assertEquals("Lookup request to " + uri + " did not return 200 status", HttpStatus.OK, entity.getStatusCode());
        return entity;
    }
}
