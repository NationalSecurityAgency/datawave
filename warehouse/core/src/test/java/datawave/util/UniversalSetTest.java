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
        
        int originalSize = universalSet.size();
        universalSet.remove(this);
        Assert.assertEquals(universalSet.size(), originalSize);
        
        universalSet.removeAll(Sets.newHashSet(this));
        Assert.assertEquals(universalSet.size(), originalSize);
        
        universalSet.add("trouble-maker");
        Assert.assertEquals(universalSet.size(), originalSize);
        
        universalSet.addAll(Sets.newHashSet("foo", "bar", "baz"));
        Assert.assertEquals(universalSet.size(), originalSize);
        
        universalSet.retainAll(Sets.newHashSet("foo", "bar", "baz"));
        Assert.assertEquals(universalSet.size(), originalSize);
        
        universalSet.removeIf(x -> true);
        Assert.assertEquals(universalSet.size(), originalSize);
    }
}
