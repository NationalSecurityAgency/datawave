package datawave.webservice.query.logic.filtered;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.LinkedHashSet;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;

public class QueryLogicFilterByAuthTest {
    @Test
    public void testDefaults() {
        QueryLogicFilterByAuth filter = new QueryLogicFilterByAuth();
        filter.setVisibility("FOO|BAR");
        assertTrue(filter.canRunQuery(null, Collections.singleton(new Authorizations("FOO"))));
        assertTrue(filter.canRunQuery(null, Collections.singleton(new Authorizations("BAR"))));
        assertFalse(filter.canRunQuery(null, Collections.singleton(new Authorizations("FOOBAR"))));
        LinkedHashSet<Authorizations> set = new LinkedHashSet<>();
        set.add(new Authorizations("FOO"));
        set.add(new Authorizations("BAR"));
        assertTrue(filter.canRunQuery(null, set));
        set.add(new Authorizations("FOOBAR"));
        assertFalse(filter.canRunQuery(null, set));
    }

    @Test
    public void testFirstMatch() {
        QueryLogicFilterByAuth filter = new QueryLogicFilterByAuth();
        filter.setVisibility("FOO|BAR");
        filter.setMatchType(QueryLogicFilterByAuth.MatchType.FIRST);
        assertTrue(filter.canRunQuery(null, Collections.singleton(new Authorizations("FOO"))));
        assertTrue(filter.canRunQuery(null, Collections.singleton(new Authorizations("BAR"))));
        assertFalse(filter.canRunQuery(null, Collections.singleton(new Authorizations("FOOBAR"))));
        LinkedHashSet<Authorizations> set = new LinkedHashSet<>();
        set.add(new Authorizations("FOO"));
        set.add(new Authorizations("BAR"));
        assertTrue(filter.canRunQuery(null, set));
        set.add(new Authorizations("FOOBAR"));
        assertTrue(filter.canRunQuery(null, set));
    }

    @Test
    public void testNegated() {
        QueryLogicFilterByAuth filter = new QueryLogicFilterByAuth();
        filter.setVisibility("FOO|BAR");
        filter.setNegated(true);
        assertFalse(filter.canRunQuery(null, Collections.singleton(new Authorizations("FOO"))));
        assertFalse(filter.canRunQuery(null, Collections.singleton(new Authorizations("BAR"))));
        assertTrue(filter.canRunQuery(null, Collections.singleton(new Authorizations("FOOBAR"))));
        LinkedHashSet<Authorizations> set = new LinkedHashSet<>();
        set.add(new Authorizations("FOO"));
        set.add(new Authorizations("BAR"));
        assertFalse(filter.canRunQuery(null, set));
        set.add(new Authorizations("FOOBAR"));
        assertTrue(filter.canRunQuery(null, set));
    }
}
