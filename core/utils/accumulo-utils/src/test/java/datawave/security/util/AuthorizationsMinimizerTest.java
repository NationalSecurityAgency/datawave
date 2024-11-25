package datawave.security.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

public class AuthorizationsMinimizerTest {
    
    @Test
    public void testWithDisjointSets() {
        Collection<Authorizations> toMinimize = Lists.newArrayList(new Authorizations("A", "B", "C"), new Authorizations("D", "E", "F"),
                        new Authorizations("G", "H", "I"));
        Collection<Authorizations> actual = AuthorizationsMinimizer.minimize(toMinimize);
        assertEquals(toMinimize, actual);
    }
    
    @Test
    public void testWithDuplicateDisjointSets() {
        Collection<Authorizations> toMinimize = Lists.newArrayList(new Authorizations("A", "B", "C"), new Authorizations("D", "E", "F"),
                        new Authorizations("G", "H", "I"), new Authorizations("A", "B", "C"), new Authorizations("D", "E", "F"));
        LinkedHashSet<Authorizations> expected = new LinkedHashSet<>(toMinimize);
        Collection<Authorizations> actual = AuthorizationsMinimizer.minimize(toMinimize);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testWithMinimimSet() {
        Collection<Authorizations> toMinimize = Lists.newArrayList(new Authorizations("A", "B", "C", "D", "E", "F"),
                        new Authorizations("A", "C", "D", "E", "F"), new Authorizations("C", "E", "F"));
        LinkedHashSet<Authorizations> expected = new LinkedHashSet<>(Collections.singleton(new Authorizations("C", "E", "F")));
        Collection<Authorizations> actual = AuthorizationsMinimizer.minimize(toMinimize);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testWithDuplicateMinimimSet() {
        Collection<Authorizations> toMinimize = Lists.newArrayList(new Authorizations("C", "E", "F"), new Authorizations("A", "B", "C", "D", "E", "F"),
                        new Authorizations("C", "E", "F"), new Authorizations("A", "C", "D", "E", "F"), new Authorizations("C", "E", "F"));
        LinkedHashSet<Authorizations> expected = new LinkedHashSet<>(Collections.singleton(new Authorizations("C", "E", "F")));
        Collection<Authorizations> actual = AuthorizationsMinimizer.minimize(toMinimize);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testWithDuplicatesAndMinimimSet() {
        Collection<Authorizations> toMinimize = Lists.newArrayList(new Authorizations("A", "C", "D", "E", "F"),
                        new Authorizations("A", "B", "C", "D", "E", "F"), new Authorizations("A", "B", "C", "D", "E", "F"),
                        new Authorizations("A", "C", "D", "E", "F"), new Authorizations("C", "E", "F"));
        LinkedHashSet<Authorizations> expected = new LinkedHashSet<>(Collections.singleton(new Authorizations("C", "E", "F")));
        Collection<Authorizations> actual = AuthorizationsMinimizer.minimize(toMinimize);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testWithDisjointSupersets() {
        Collection<Authorizations> toMinimize = Lists.newArrayList(new Authorizations("A", "C", "D", "E", "F"),
                        new Authorizations("A", "B", "C", "D", "E", "F"), new Authorizations("A", "B", "C", "D", "E", "F"),
                        new Authorizations("A", "C", "D", "E", "F"), new Authorizations("C", "E", "F"), new Authorizations("G", "H", "I", "J", "K", "L"),
                        new Authorizations("H", "L"), new Authorizations("H", "I", "K", "L"));
        LinkedHashSet<Authorizations> expected = new LinkedHashSet<>(Arrays.asList(new Authorizations("C", "E", "F"), new Authorizations("H", "L")));
        Collection<Authorizations> actual = AuthorizationsMinimizer.minimize(toMinimize);
        assertEquals(expected, actual);
    }
}
