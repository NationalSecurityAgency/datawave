package datawave.data.type;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.util.Assert;

public class ListTypeTest {
    
    @Test
    public void test() {
        String str = "1,2,3;a;b;c";
        
        LcNoDiacriticsListType t = new LcNoDiacriticsListType(str);
        Assert.equals(6, t.normalizeToMany(str).size());
        List<String> expected = Arrays.asList(new String[] {"1", "2", "3", "a", "b", "c"});
        Assert.equals(expected, t.normalizeToMany(str));
    }
    
    @Test
    public void testLcNDList() {
        String str = "01,02,03;A;B;C";
        
        LcNoDiacriticsListType t = new LcNoDiacriticsListType();
        Assert.equals(6, t.normalizeToMany(str).size());
        List<String> expected = Arrays.asList(new String[] {"01", "02", "03", "a", "b", "c"});
        Assert.equals(expected, t.normalizeToMany(str));
    }
    
    @Test
    public void testNumberList() {
        String str = "1,2,3,5.5";
        List<String> expected = Arrays.asList(new String[] {"+aE1", "+aE2", "+aE3", "+aE5.5"});
        
        NumberListType nt = new NumberListType();
        Assert.equals(4, nt.normalizeToMany(str).size());
        Assert.equals(expected, nt.normalizeToMany(str));
    }
    
    @Test
    public void testBadNumberList() {
        String str = "3,2,1,banana";
        
        NumberListType nt = new NumberListType();
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            nt.normalizeToMany(str);
        });
        
    }
    
}
