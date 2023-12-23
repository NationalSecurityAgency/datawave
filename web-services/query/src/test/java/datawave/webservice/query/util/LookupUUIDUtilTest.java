package datawave.webservice.query.util;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.powermock.api.easymock.PowerMock.replayAll;

import java.util.Collections;

import javax.ejb.EJBContext;
import javax.ws.rs.core.MultivaluedMap;

import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.jboss.resteasy.util.FindAnnotation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import datawave.core.query.logic.QueryLogicFactory;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.data.UUIDType;
import datawave.security.authorization.UserOperations;
import datawave.webservice.query.configuration.LookupUUIDConfiguration;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.runner.QueryExecutor;

@RunWith(PowerMockRunner.class)
@PrepareForTest(FindAnnotation.class)
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
        expect(configuration.getUuidTypes()).andReturn(Collections.singletonList(new UUIDType("ID", "LuceneUUIDEventQuery", 28)));
        expect(configuration.getBeginDate()).andReturn("20230101");
        expect(configuration.getBatchLookupUpperLimit()).andReturn(10);
        MultivaluedMap<String,String> defaultParams = new MultivaluedMapImpl<>();
        defaultParams.putSingle("foo", "bar");
        defaultParams.putSingle("foo2", "default");
        expect(configuration.optionalParamsToMap()).andReturn(defaultParams);
        expect(responseObjectFactory.getQueryImpl()).andReturn(new QueryImpl());
        replayAll();
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
