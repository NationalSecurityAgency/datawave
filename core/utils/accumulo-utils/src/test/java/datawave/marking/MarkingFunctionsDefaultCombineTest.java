package datawave.marking;

import static datawave.marking.MarkingFunctions.Default.COLUMN_VISIBILITY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class MarkingFunctionsDefaultCombineTest {
    
    @Test
    public void testCombineAnds() throws MarkingFunctions.Exception {
        MarkingFunctions markingFunctions = new MarkingFunctions.Default();
        
        ColumnVisibility oneAnna = new ColumnVisibility("A&B");
        ColumnVisibility twoAnna = new ColumnVisibility("A&C");
        
        ColumnVisibility combined = markingFunctions.combine(Sets.newHashSet(oneAnna, twoAnna));
        assertEquals(new ColumnVisibility("A&B&C"), combined);
    }
    
    @Test
    public void testCombineOrs() throws MarkingFunctions.Exception {
        MarkingFunctions markingFunctions = new MarkingFunctions.Default();
        
        ColumnVisibility oneOr = new ColumnVisibility("A|B");
        ColumnVisibility twoOr = new ColumnVisibility("A|C");
        
        ColumnVisibility combined = markingFunctions.combine(Sets.newHashSet(oneOr, twoOr));
        assertEquals(new ColumnVisibility("(A|B)&(A|C)"), combined);
    }
    
    @Test
    public void testCombineMapsOfAnds() throws MarkingFunctions.Exception {
        MarkingFunctions markingFunctions = new MarkingFunctions.Default();
        
        Map<String,String> mapOne = ImmutableMap.of(COLUMN_VISIBILITY, "A&B");
        Map<String,String> mapTwo = ImmutableMap.of(COLUMN_VISIBILITY, "A&C");
        
        Map<String,String> expected = ImmutableMap.of(COLUMN_VISIBILITY, "A&B&C");
        
        assertEquals(expected, markingFunctions.combine(mapOne, mapTwo));
    }
    
    @Test
    public void testCombineMapsOfOrs() throws MarkingFunctions.Exception {
        MarkingFunctions markingFunctions = new MarkingFunctions.Default();
        
        Map<String,String> mapOne = ImmutableMap.of(COLUMN_VISIBILITY, "A|B");
        Map<String,String> mapTwo = ImmutableMap.of(COLUMN_VISIBILITY, "A|C");
        
        Map<String,String> expected = ImmutableMap.of(COLUMN_VISIBILITY, "(A|B)&(A|C)");
        
        assertEquals(expected, markingFunctions.combine(mapOne, mapTwo));
    }
}
