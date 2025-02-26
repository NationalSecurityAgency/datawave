package datawave.microservice;

import java.util.Collections;
import java.util.HashSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.ResponseErrorHandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;

import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;

/**
 * Provides a default setup for controller integration tests (specifically). Contains properties and setup that are common to controller integration tests. The
 * setup and dependency injections in this class are done before any in implementing classes.
 */
@ExtendWith(SpringExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class ControllerIT {
    
    @LocalServerPort
    protected int webServicePort;
    
    @Autowired
    @Qualifier("warehouse")
    protected AccumuloClient accumuloClient;
    
    protected DatawaveUserDetails adminUser;
    protected DatawaveUserDetails regularUser;
    
    @Autowired
    private RestTemplateBuilder restTemplateBuilder;
    
    protected JWTRestTemplate jwtRestTemplate;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @BeforeAll
    public void oneTimeSetup() {
        
        // Allow 403 responses through without throwing an exception so tests can assert the response appropriately.
        ResponseErrorHandler errorHandler = new DefaultResponseErrorHandler() {
            @Override
            protected boolean hasError(HttpStatus statusCode) {
                return super.hasError(statusCode) && statusCode.value() != 403;
            }
        };
        
        jwtRestTemplate = restTemplateBuilder.errorHandler(errorHandler).build(JWTRestTemplate.class);
        
        SubjectIssuerDNPair dn = SubjectIssuerDNPair.of("userDn", "issuerDn");
        HashSet<String> auths = Sets.newHashSet("PUBLIC", "PRIVATE");
        HashSet<String> roles = Sets.newHashSet("AuthorizedUser", "Administrator");
        long createTime = System.currentTimeMillis();
        adminUser = new DatawaveUserDetails(Collections.singleton(new DatawaveUser(dn, DatawaveUser.UserType.USER, auths, roles, null, createTime)),
                        createTime);
        regularUser = new DatawaveUserDetails(
                        Collections.singleton(
                                        new DatawaveUser(dn, DatawaveUser.UserType.USER, auths, Collections.singleton("AuthorizedUser"), null, createTime)),
                        createTime);
    }
    
}
