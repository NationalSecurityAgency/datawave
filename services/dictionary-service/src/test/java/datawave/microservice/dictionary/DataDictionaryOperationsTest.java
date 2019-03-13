package datawave.microservice.dictionary;

import com.google.common.collect.Sets;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.dictionary.config.DataDictionaryProperties;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.results.datadictionary.DefaultDataDictionary;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "spring.main.allow-bean-definition-overriding=true")
public class DataDictionaryOperationsTest {
    private static Instance instance = new InMemoryInstance();
    
    @LocalServerPort
    private int webServicePort;
    
    @Autowired
    private RestTemplateBuilder restTemplateBuilder;
    
    @Autowired
    @Qualifier("warehouse")
    private Connector connector;
    
    @Autowired
    private DataDictionaryProperties dataDictionaryProperties;
    
    private JWTRestTemplate jwtRestTemplate;
    private ProxiedUserDetails adminUser;
    private ProxiedUserDetails regularUser;
    
    @Before
    public void setUp() throws Exception {
        // Allow 403 responses through without throwing an exception so we can assert the response in the test.
        ResponseErrorHandler errorHandler = new DefaultResponseErrorHandler() {
            @Override
            protected boolean hasError(HttpStatus statusCode) {
                return super.hasError(statusCode) && statusCode.value() != 403;
            }
        };
        jwtRestTemplate = restTemplateBuilder.errorHandler(errorHandler).build(JWTRestTemplate.class);
        
        try {
            connector.tableOperations().create(dataDictionaryProperties.getMetadataTableName());
        } catch (TableExistsException e) {
            // ignore
        }
        
        SubjectIssuerDNPair dn = SubjectIssuerDNPair.of("userDn", "issuerDn");
        HashSet<String> auths = Sets.newHashSet("PUBLIC", "PRIVATE");
        HashSet<String> roles = Sets.newHashSet("AuthorizedUser", "Administrator");
        long createTime = System.currentTimeMillis();
        adminUser = new ProxiedUserDetails(Collections.singleton(new DatawaveUser(dn, DatawaveUser.UserType.USER, auths, roles, null, createTime)), createTime);
        regularUser = new ProxiedUserDetails(
                        Collections.singleton(
                                        new DatawaveUser(dn, DatawaveUser.UserType.USER, auths, Collections.singleton("AuthorizedUser"), null, createTime)),
                        createTime);
    }
    
    @Test
    public void testGet() {
        // @formatter:off
        UriComponents uri = UriComponentsBuilder.newInstance()
                .scheme("https").host("localhost").port(webServicePort)
                .path("/dictionary/data/v1/")
                .build();
        // @formatter:on
        
        ResponseEntity<DefaultDataDictionary> response = jwtRestTemplate.exchange(adminUser, HttpMethod.GET, uri, DefaultDataDictionary.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
    }
    
    @Test
    public void testRestrictedMethods() {
        // @formatter:off
        UriComponents uri = UriComponentsBuilder.newInstance()
                .scheme("https").host("localhost").port(webServicePort)
                .path("/dictionary/data/v1/Descriptions/{dt}/{fn}/{desc}")
                .query("columnVisibility=PUBLIC")
                .buildAndExpand("dataType", "fieldName", "desc");
        // @formatter:on
        
        ResponseEntity<String> response = jwtRestTemplate.exchange(regularUser, HttpMethod.PUT, uri, String.class);
        assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
        
        // @formatter:off
        uri = UriComponentsBuilder.newInstance()
                .scheme("https").host("localhost").port(webServicePort)
                .path("/dictionary/data/v1/Descriptions")
                .build();
        // @formatter:on
        
        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE);
        MultiValueMap<String,String> body = new LinkedMultiValueMap<>();
        body.set("datatype", "dType");
        body.set("fieldName", "fName");
        body.set("description", "desc");
        body.set("columnVisibility", "cVis");
        RequestEntity<MultiValueMap<String,String>> entity = jwtRestTemplate.createRequestEntity(regularUser, body, headers, HttpMethod.POST, uri);
        response = jwtRestTemplate.exchange(entity, String.class);
        assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
        
        // @formatter:off
        uri = UriComponentsBuilder.newInstance()
                .scheme("https").host("localhost").port(webServicePort)
                .path("/dictionary/data/v1/Descriptions/{dt}/{fn}")
                .query("columnVisibility=PUBLIC")
                .buildAndExpand("dataType", "fieldName");
        // @formatter:on
        
        response = jwtRestTemplate.exchange(regularUser, HttpMethod.DELETE, uri, String.class);
        assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
    }
    
    @ComponentScan(basePackages = "datawave.microservice")
    @Configuration
    public static class DataDictionaryImplTestConfiguration {
        @Bean
        @Qualifier("warehouse")
        public Connector warehouseConnector() throws AccumuloSecurityException, AccumuloException {
            return instance.getConnector("root", new PasswordToken(""));
        }
    }
}
