package datawave.webservice.query.logic.filtered;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogic;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

public class FilteredQueryLogicTest {

    FilteredQueryLogic logic;
    QueryLogic delegate;

    @Before
    public void setup() {
        delegate = PowerMock.createMock(QueryLogic.class);
        logic = new FilteredQueryLogic();
        logic.setDelegate(delegate);
        logic.setFilter(new QueryLogicFilterByAuth("FOO|BAR"));
    }

    @After
    public void cleanup() {
        PowerMock.resetAll();
    }

    @Test
    public void testFiltered() throws Exception {
        Query settings = new QueryImpl();
        Set<Authorizations> auths = Collections.singleton(new Authorizations("FILTERME"));

        PowerMock.replayAll();
        GenericQueryConfiguration config = logic.initialize(null, settings, auths);
        logic.setupQuery(config);
        Iterator it = logic.iterator();
        Assert.assertFalse(it.hasNext());
        String plan = logic.getPlan(null, settings, auths, true, true);
        Assert.assertEquals("", plan);
        PowerMock.verifyAll();
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

        PowerMock.replayAll();
        logic.initialize(null, new QueryImpl(), Collections.singleton(new Authorizations("FOO")));
        logic.setupQuery(config);
        Iterator it = logic.iterator();
        Assert.assertTrue(it.hasNext());
        it.next();
        Assert.assertFalse(it.hasNext());
        String plan = logic.getPlan(null, settings, auths, true, true);
        Assert.assertEquals("a plan", plan);
        PowerMock.verifyAll();
    }
}
