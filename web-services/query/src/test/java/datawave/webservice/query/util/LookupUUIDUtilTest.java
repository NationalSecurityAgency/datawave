package datawave.webservice.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Collections;

import javax.ejb.EJBContext;
import javax.ws.rs.core.MultivaluedMap;

import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import datawave.core.query.logic.QueryLogicFactory;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.query.lookup.LookupProperties;
import datawave.security.authorization.UserOperations;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.runner.QueryExecutor;

@ExtendWith(MockitoExtension.class)
public class LookupUUIDUtilTest {

    @Mock
    LookupProperties lookupProperties;
    @Mock
    QueryExecutor queryExecutor;
    @Mock
    EJBContext context;
    @Mock
    ResponseObjectFactory responseObjectFactory;
    @Mock
    QueryLogicFactory queryLogicFactory;
    @Mock
    UserOperations userOperations;

    @Test
    public void testCreateSettings() {
        when(lookupProperties.getContentLookupTypes()).thenReturn(Collections.emptyMap());
        when(lookupProperties.getUuidTypes()).thenReturn(null);
        when(lookupProperties.getBeginDate()).thenReturn("20230101");
        when(lookupProperties.getBatchLookupUpperLimit()).thenReturn(10);
        when(lookupProperties.getTagCloudLookupUpperLimit()).thenReturn(50);
        MultiValueMap<String,String> defaultParams = new LinkedMultiValueMap<>();
        defaultParams.put("foo", Collections.singletonList("bar"));
        defaultParams.put("foo2", Collections.singletonList("default"));
        when(lookupProperties.optionalParamsToMap()).thenReturn(defaultParams);
        when(responseObjectFactory.getQueryImpl()).thenReturn(new QueryImpl());

        LookupUUIDUtil utils = new LookupUUIDUtil(lookupProperties, queryExecutor, context, responseObjectFactory, queryLogicFactory, userOperations);

        MultivaluedMap<String,String> properties = new MultivaluedMapImpl<>();
        properties.putSingle("foo2", "bar2");
        properties.add("foo3", "bar3");
        properties.add("foo3", "bar3.1");
        Query q = utils.createSettings(properties);

        assertEquals(new QueryImpl.Parameter("foo", "bar"), q.findParameter("foo"));
        assertEquals(new QueryImpl.Parameter("foo2", "bar2"), q.findParameter("foo2"));
        assertEquals(new QueryImpl.Parameter("foo3", ""), q.findParameter("foo3"));
        assertEquals(3, q.getOptionalQueryParameters().size());
        properties.putSingle("foo", "bar");
        assertEquals(properties, q.getOptionalQueryParameters());
    }
}
