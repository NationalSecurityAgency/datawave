package datawave.webservice.query.logic.filtered;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

    @BeforeEach
    public void setup() {
        delegate = Mockito.mock(QueryLogic.class);
        logic = new FilteredQueryLogic();
        logic.setDelegate(delegate);
        logic.setFilter(new QueryLogicFilterByAuth("FOO|BAR"));
    }

    @AfterEach
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
        assertFalse(it.hasNext());
        String plan = logic.getPlan(null, settings, auths, true, true);
        assertEquals("", plan);
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
        assertTrue(it.hasNext());
        it.next();
        assertFalse(it.hasNext());
        String plan = logic.getPlan(null, settings, auths, true, true);
        assertEquals("a plan", plan);
    }
}
