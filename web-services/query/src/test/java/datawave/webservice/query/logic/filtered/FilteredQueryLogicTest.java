package datawave.webservice.query.logic.filtered;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.filtered.FilteredQueryLogic;
import datawave.core.query.logic.filtered.QueryLogicFilterByAuth;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;

public class FilteredQueryLogicTest {

    FilteredQueryLogic logic;
    QueryLogic delegate;

    @Before
    public void setup() {
        delegate = Mockito.mock(QueryLogic.class);
        logic = new FilteredQueryLogic();
        logic.setDelegate(delegate);
        logic.setFilter(new QueryLogicFilterByAuth("FOO|BAR"));
    }

    @After
    public void cleanup() {
        Mockito.reset();
    }

    @Test
    public void testFiltered() throws Exception {
        Query settings = new QueryImpl();
        Set<Authorizations> auths = Collections.singleton(new Authorizations("FILTERME"));

        GenericQueryConfiguration config = logic.initialize(null, settings, auths);
        logic.setupQuery(config);
        Iterator it = logic.iterator();
        Assert.assertFalse(it.hasNext());
        String plan = logic.getPlan(null, settings, auths, true, true);
        Assert.assertEquals("", plan);
    }

    @Test
    public void testNotFiltered() throws Exception {
        Query settings = new QueryImpl();
        Set<Authorizations> auths = Collections.singleton(new Authorizations("FOO"));
        GenericQueryConfiguration config = new GenericQueryConfiguration() {};

        Mockito.when(delegate.initialize(null, settings, auths)).thenReturn(config);
        delegate.setupQuery(config);
        Mockito.when(delegate.iterator()).thenReturn(Collections.singleton(new Object()).iterator());
        Mockito.when(delegate.getPlan(null, settings, auths, true, true)).thenReturn("a plan");

        logic.initialize(null, new QueryImpl(), Collections.singleton(new Authorizations("FOO")));
        logic.setupQuery(config);
        Iterator it = logic.iterator();
        Assert.assertTrue(it.hasNext());
        it.next();
        Assert.assertFalse(it.hasNext());
        String plan = logic.getPlan(null, settings, auths, true, true);
        Assert.assertEquals("a plan", plan);
    }
}
