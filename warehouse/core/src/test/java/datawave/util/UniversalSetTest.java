package datawave.util;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

public class UniversalSetTest {
    
    Collection<String> universalSet = UniversalSet.instance();
    
    @Test
    public void testExpectedBehavior() {
        
        Assert.assertTrue(universalSet.isEmpty());
        
        Assert.assertTrue(universalSet.contains(new Object()));
        
        Assert.assertTrue(universalSet.containsAll(Sets.newHashSet("foo", "bar", "baz")));
        
        try {
            universalSet.remove(this);
            Assert.fail(universalSet + " should not be modifiable");
        } catch (UnsupportedOperationException ex) {
            // expected
        }
        try {
            universalSet.removeAll(Sets.newHashSet(this));
            Assert.fail(universalSet + " should not be modifiable");
        } catch (UnsupportedOperationException ex) {
            // expected
        }
        try {
            universalSet.add("trouble-maker");
            Assert.fail(universalSet + " should not be modifiable");
        } catch (UnsupportedOperationException ex) {
            // expected
        }
        try {
            universalSet.addAll(Sets.newHashSet("foo", "bar", "baz"));
            Assert.fail(universalSet + " should not be modifiable");
        } catch (UnsupportedOperationException ex) {
            // expected
        }
        try {
            universalSet.retainAll(Sets.newHashSet("foo", "bar", "baz"));
            Assert.fail(universalSet + " should not be modifiable");
        } catch (UnsupportedOperationException ex) {
            // expected
        }
        try {
            universalSet.removeIf(x -> true);
            Assert.fail(universalSet + " should not be modifiable");
        } catch (UnsupportedOperationException ex) {
            // expected
        }
    }
}
