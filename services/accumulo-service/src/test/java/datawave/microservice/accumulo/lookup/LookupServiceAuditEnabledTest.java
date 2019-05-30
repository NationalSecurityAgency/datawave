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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.text.StringContainsInOrder.stringContainsInOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.anything;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
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
    
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    
    private static final String EXPECTED_AUDIT_URI = "http://localhost:11111/audit/v1/audit";
    
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
    private MockRestServiceServer mockAuditServer;
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
    public void testLookupAndVerifyLOCALONLY() throws Exception {
        mockDataService.setupMockTable(connector, "localonlyAuditTable");
        testLookupAndVerifyAuditUriWithSuccess("localonlyAuditTable", "row1", Auditor.AuditType.LOCALONLY);
    }
    
    /**
     * Create new table, which will have no audit rule defined and thus default to AuditType.NONE (i.e., accumulo.lookup.audit.defaultAuditType = NONE)
     * AuditClient should avoid REST calls to the audit server when audit type is NONE
     */
    @Test
    public void testAuditTypeACTIVE_VerifyZeroAudits() throws Exception {
        // All mockAuditServer expectation(s) must fail
        expectedException.expect(AssertionError.class);
        expectedException.expectMessage(stringContainsInOrder("\n0 request(s) executed"));
        mockDataService.setupMockTable(connector, "tableWithNoAuditRule1");
        mockAuditServer.expect(anything());
        testLookupAndVerifyAuditUriWithSuccess("tableWithNoAuditRule1", "row3", Auditor.AuditType.ACTIVE);
    }
    
    /**
     * Create new table, which will have no audit rule defined and thus default to AuditType.NONE (i.e., accumulo.lookup.audit.defaultAuditType = NONE)
     * AuditClient should avoid REST calls to the audit server when audit type is NONE
     */
    @Test
    public void testAuditTypePASSIVE_VerifyZeroAudits() throws Exception {
        // All mockAuditServer expectation(s) must fail
        expectedException.expect(AssertionError.class);
        expectedException.expectMessage(stringContainsInOrder("\n0 request(s) executed"));
        mockDataService.setupMockTable(connector, "tableWithNoAuditRule2");
        mockAuditServer.expect(anything());
        testLookupAndVerifyAuditUriWithSuccess("tableWithNoAuditRule2", "row3", Auditor.AuditType.PASSIVE);
    }
    
    /**
     * Create new table, which will have no audit rule defined and thus default to AuditType.NONE (i.e., accumulo.lookup.audit.defaultAuditType = NONE)
     * AuditClient should avoid REST calls to the audit server when audit type is NONE
     */
    @Test
    public void testAuditTypeNONE_VerifyZeroAudits() throws Exception {
        // All mockAuditServer expectation(s) must fail
        expectedException.expect(AssertionError.class);
        expectedException.expectMessage(stringContainsInOrder("\n0 request(s) executed"));
        mockDataService.setupMockTable(connector, "tableWithNoAuditRule3");
        mockAuditServer.expect(anything());
        testLookupAndVerifyAuditUriWithSuccess("tableWithNoAuditRule3", "row3", Auditor.AuditType.NONE);
    }
    
    /**
     * Create new table, which will have no audit rule defined and thus default to AuditType.NONE (i.e., accumulo.lookup.audit.defaultAuditType = NONE)
     * AuditClient should avoid REST calls to the audit server when audit type is NONE
     */
    @Test
    public void testAuditTypeLOCALONLY_VerifyZeroAudits() throws Exception {
        // All mockAuditServer expectation(s) must fail
        expectedException.expect(AssertionError.class);
        expectedException.expectMessage(stringContainsInOrder("\n0 request(s) executed"));
        mockDataService.setupMockTable(connector, "tableWithNoAuditRule4");
        mockAuditServer.expect(anything());
        testLookupAndVerifyAuditUriWithSuccess("tableWithNoAuditRule4", "row3", Auditor.AuditType.LOCALONLY);
    }
    
    @Test
    public void testErrorOnMissingColVizParam() throws Exception {
        expectedException.expect(HttpClientErrorException.class);
        expectedException.expect(new TestHelper.StatusMatcher(400));
        ProxiedUserDetails userDetails = TestHelper.userDetails(Collections.singleton("Administrator"), Arrays.asList("A"));
        doLookup(userDetails, path(testTableName + "/row2"), "NotColumnVisibility=foo");
    }
    
    private void testLookupAndVerifyAuditUriWithSuccess(String targetTable, String targetRow, Auditor.AuditType expectedAuditType) throws Exception {
        
        String auditColViz = "foo";
        String queryAuths = "A,C,E,G,I";
        String queryUseAuths = "useAuthorizations=" + queryAuths;
        String queryColumnViz = "columnVisibility=" + auditColViz;
        
        MultiValueMap<String,String> expectedFormData = new LinkedMultiValueMap<>();
        expectedFormData.set("useAuthorizations", queryAuths);
        expectedFormData.set("columnVisibility", auditColViz);
        expectedFormData.set("query", String.join("/", "lookup", targetTable, targetRow));
        expectedFormData.set("auths", queryAuths);
        expectedFormData.set("auditUserDN", defaultUserDetails.getPrimaryUser().getDn().toString());
        expectedFormData.set("auditType", expectedAuditType.name());
        expectedFormData.set("auditColumnVisibility", auditColViz);
        expectedFormData.set("logicClass", "AccumuloLookup");
        
        //@formatter:off
        mockAuditServer.expect(requestTo(EXPECTED_AUDIT_URI))
            .andExpect(content().formData(expectedFormData))
            .andRespond(withSuccess()
        );
        //@formatter:on
        
        String queryString = String.join("&", queryUseAuths, queryColumnViz);
        doLookup(defaultUserDetails, path(targetTable + "/" + targetRow), queryString);
        
        mockAuditServer.verify();
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
        mockAuditServer = MockRestServiceServer.createServer(auditorRestTemplate);
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
