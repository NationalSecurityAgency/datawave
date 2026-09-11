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

import datawave.core.query.logic.QueryLogicFactory;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.data.UUIDType;
import datawave.security.authorization.UserOperations;
import datawave.webservice.query.configuration.LookupUUIDConfiguration;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.runner.QueryExecutor;

@ExtendWith(MockitoExtension.class)
public class LookupUUIDUtilTest {

    @Mock
    LookupUUIDConfiguration configuration;
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
        when(configuration.getContentLookupTypes()).thenReturn(Collections.emptyMap());
        when(configuration.getUuidTypes()).thenReturn(Collections.singletonList(new UUIDType("ID", "LuceneUUIDEventQuery", 28)));
        when(configuration.getBeginDate()).thenReturn("20230101");
        when(configuration.getBatchLookupUpperLimit()).thenReturn(10);
        when(configuration.getTagCloudLookupUpperLimit()).thenReturn(50);
        MultivaluedMap<String,String> defaultParams = new MultivaluedMapImpl<>();
        defaultParams.putSingle("foo", "bar");
        defaultParams.putSingle("foo2", "default");
        when(configuration.optionalParamsToMap()).thenReturn(defaultParams);
        when(responseObjectFactory.getQueryImpl()).thenReturn(new QueryImpl());

        LookupUUIDUtil utils = new LookupUUIDUtil(configuration, queryExecutor, context, responseObjectFactory, queryLogicFactory, userOperations);

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
