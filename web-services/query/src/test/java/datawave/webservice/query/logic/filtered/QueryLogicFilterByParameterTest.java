package datawave.webservice.query.logic.filtered;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import datawave.core.query.logic.filtered.QueryLogicFilterByParameter;
import datawave.microservice.query.QueryImpl;

public class QueryLogicFilterByParameterTest {
    @Test
    public void testDefaults() {
        QueryLogicFilterByParameter filter = new QueryLogicFilterByParameter();
        filter.setParameter("foo");
        QueryImpl query = new QueryImpl();
        assertFalse(filter.canRunQuery(query, null));
        query.addParameter("bar", "true");
        assertFalse(filter.canRunQuery(query, null));
        query.addParameter("foo", "false");
        assertFalse(filter.canRunQuery(query, null));
        query.addParameter("foo", "true");
        assertTrue(filter.canRunQuery(query, null));
    }

    @Test
    public void testValue() {
        QueryLogicFilterByParameter filter = new QueryLogicFilterByParameter();
        filter.setParameter("foo");
        filter.setValue("bar");
        QueryImpl query = new QueryImpl();
        assertFalse(filter.canRunQuery(query, null));
        query.addParameter("bar", "true");
        assertFalse(filter.canRunQuery(query, null));
        query.addParameter("foo", "false");
        assertFalse(filter.canRunQuery(query, null));
        query.addParameter("foo", "true");
        assertFalse(filter.canRunQuery(query, null));
        query.addParameter("foo", "bar");
        assertTrue(filter.canRunQuery(query, null));
    }

    @Test
    public void testNegates() {
        QueryLogicFilterByParameter filter = new QueryLogicFilterByParameter();
        filter.setParameter("foo");
        filter.setValue("bar");
        filter.setNegated(true);
        QueryImpl query = new QueryImpl();
        assertTrue(filter.canRunQuery(query, null));
        query.addParameter("bar", "true");
        assertTrue(filter.canRunQuery(query, null));
        query.addParameter("foo", "false");
        assertTrue(filter.canRunQuery(query, null));
        query.addParameter("foo", "true");
        assertTrue(filter.canRunQuery(query, null));
        query.addParameter("foo", "bar");
        assertFalse(filter.canRunQuery(query, null));
    }

}
