package datawave.webservice.query.logic.filtered;

import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.QueryLogic;
import org.apache.accumulo.core.security.Authorizations;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

@Disabled
@ExtendWith(EasyMockExtension.class)
public class FilteredQueryLogicTest {
    
    FilteredQueryLogic logic;
    
    @Mock
    QueryLogic delegate;
    
    @BeforeEach
    public void setup() {
        logic = new FilteredQueryLogic();
        logic.setDelegate(delegate);
        logic.setFilter(new QueryLogicFilterByAuth("FOO|BAR"));
    }
    
    @AfterEach
    public void cleanup() {
        EasyMock.reset();
    }
    
    @Test
    public void testFiltered() throws Exception {
        Query settings = new QueryImpl();
        Set<Authorizations> auths = Collections.singleton(new Authorizations("FILTERME"));
        
        EasyMock.replay();
        GenericQueryConfiguration config = logic.initialize(null, settings, auths);
        logic.setupQuery(config);
        Iterator it = logic.iterator();
        Assertions.assertFalse(it.hasNext());
        String plan = logic.getPlan(null, settings, auths, true, true);
        Assertions.assertEquals("", plan);
        EasyMock.verify();
    }
    
    @Test
    public void testNotFiltered() throws Exception {
        Query settings = new QueryImpl();
        Set<Authorizations> auths = Collections.singleton(new Authorizations("FOO"));
        GenericQueryConfiguration config = new GenericQueryConfiguration() {};
        
        EasyMock.expect(delegate.initialize(null, settings, auths)).andReturn(config);
        delegate.setupQuery(config);
        EasyMock.expect(delegate.iterator()).andReturn(Collections.singleton(new Object()).iterator());
        EasyMock.expect(delegate.getPlan(null, settings, auths, true, true)).andReturn("a plan");
        
        EasyMock.replay();
        logic.initialize(null, new QueryImpl(), Collections.singleton(new Authorizations("FOO")));
        logic.setupQuery(config);
        Iterator it = logic.iterator();
        Assertions.assertTrue(it.hasNext());
        it.next();
        Assertions.assertFalse(it.hasNext());
        String plan = logic.getPlan(null, settings, auths, true, true);
        Assertions.assertEquals("a plan", plan);
        EasyMock.verify();
    }
}
