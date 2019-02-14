package datawave.marking;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class MarkingFunctionsDefaultCombineTest {
    
    @Test
    public void testCombineAnds() throws MarkingFunctions.Exception {
        MarkingFunctions markingFunctions = new MarkingFunctions.Default();
        
        ColumnVisibility oneAnna = new ColumnVisibility("A&B");
        ColumnVisibility twoAnna = new ColumnVisibility("A&C");
        
        ColumnVisibility combined = markingFunctions.combine(Sets.newHashSet(oneAnna, twoAnna));
        Assert.assertEquals(new ColumnVisibility("A&B&C"), combined);
    }
    
    @Test
    public void testCombineOrs() throws MarkingFunctions.Exception {
        MarkingFunctions markingFunctions = new MarkingFunctions.Default();
        
        ColumnVisibility oneOr = new ColumnVisibility("A|B");
        ColumnVisibility twoOr = new ColumnVisibility("A|C");
        
        ColumnVisibility combined = markingFunctions.combine(Sets.newHashSet(oneOr, twoOr));
        Assert.assertEquals(new ColumnVisibility("(A|B)&(A|C)"), combined);
    }
    
    @Test
    public void testCombineMapsOfAnds() throws MarkingFunctions.Exception {
        MarkingFunctions markingFunctions = new MarkingFunctions.Default();
        
        Map<String,String> mapOne = ImmutableMap.of("FOO", "A&B");
        Map<String,String> mapTwo = ImmutableMap.of("FOO", "A&C");
        
        Map<String,String> expected = ImmutableMap.of("FOO", "(A&B)&(A&C)");
        
        Assert.assertEquals(expected, markingFunctions.combine(mapOne, mapTwo));
    }
    
    @Test
    public void testCombineMapsOfOrs() throws MarkingFunctions.Exception {
        MarkingFunctions markingFunctions = new MarkingFunctions.Default();
        
        Map<String,String> mapOne = ImmutableMap.of("FOO", "A|B");
        Map<String,String> mapTwo = ImmutableMap.of("FOO", "A|C");
        
        Map<String,String> expected = ImmutableMap.of("FOO", "(A|B)&(A|C)");
        
        Assert.assertEquals(expected, markingFunctions.combine(mapOne, mapTwo));
    }
}
