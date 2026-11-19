package datawave.visibility.standard;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;

import datawave.visibility.FlattenedVisibilityCache;
import datawave.visibility.VisibilityFunctions;

public class StandardVisibilityFunctionsTest {

    private final VisibilityFunctions functions = VisibilityFunctions.getInstance();

    // -- combine(Collection) --

    @Test
    public void testCombineAnds() {
        assertEquals(new ColumnVisibility("A&B&C"), functions.combine(List.of(new ColumnVisibility("A&B"), new ColumnVisibility("A&C"))));
    }

    @Test
    public void testCombineOrs() {
        assertEquals(new ColumnVisibility("(A|B)&(A|C)"), functions.combine(List.of(new ColumnVisibility("A|B"), new ColumnVisibility("A|C"))));
    }

    @Test
    public void testCombineSkipsNulls() {
        Collection<ColumnVisibility> visibilities = new ArrayList<>(Arrays.asList(new ColumnVisibility("A&B"), null, new ColumnVisibility("A&C"), null));
        assertEquals(new ColumnVisibility("A&B&C"), functions.combine(visibilities));
    }

    @Test
    public void testCombineSkipsEmpty() {
        assertEquals(new ColumnVisibility("A&B"), functions.combine(List.of(new ColumnVisibility(), new ColumnVisibility("B&A"))));
    }

    @Test
    public void testCombineAllNullsReturnsEmpty() {
        assertEquals(new ColumnVisibility(), functions.combine(new ArrayList<>(Arrays.asList(null, null))));
    }

    @Test
    public void testCombineNullCollectionReturnsEmpty() {
        assertEquals(new ColumnVisibility(), functions.combine((Collection<ColumnVisibility>) null));
    }

    @Test
    public void testCombineSingleIsNormalized() {
        assertEquals(new ColumnVisibility("A&B"), functions.combine(List.of(new ColumnVisibility("B&A&B"))));
    }

    @Test
    public void testCombineDuplicatesCollapse() {
        assertEquals(new ColumnVisibility("A|B"), functions.combine(List.of(new ColumnVisibility("A|B"), new ColumnVisibility("A|B"))));
    }

    @Test
    public void testCombineMixedAndOr() {
        assertEquals(new ColumnVisibility("C&(A|B)"), functions.combine(List.of(new ColumnVisibility("B|A"), new ColumnVisibility("C"))));
    }

    @Test
    public void testCombineQuotedAuthorizations() {
        // quotes are kept only where required (a space requires quoting, a plain letter does not)
        assertEquals(new ColumnVisibility("\"A 1\"&B"), functions.combine(List.of(new ColumnVisibility("\"A 1\""), new ColumnVisibility("\"B\""))));
    }

    // -- combine(first, second) --

    @Test
    public void testCombinePairBothNullReturnsNull() {
        assertNull(functions.combine((ColumnVisibility) null, (ColumnVisibility) null));
    }

    @Test
    public void testCombinePairOneNullReturnsOther() {
        ColumnVisibility vis = new ColumnVisibility("B&A");
        assertSame(vis, functions.combine(null, vis));
        assertSame(vis, functions.combine(vis, null));
    }

    @Test
    public void testCombinePair() {
        assertEquals(new ColumnVisibility("A&B&C"), functions.combine(new ColumnVisibility("A&B"), new ColumnVisibility("C")));
    }

    // -- flatten --

    @Test
    public void testFlattenMatchesColumnVisibilityFlatten() {
        for (String expression : List.of("", "A", "B&A", "A&(B&C)", "(Z&Y)|(C&B)", "A|B|A", "\"ABC\"&\"XYZ\"", "(A|B)&(C|D)")) {
            ColumnVisibility vis = new ColumnVisibility(expression);
            assertArrayEquals(vis.flatten(), functions.flatten(vis), expression);
            assertArrayEquals(vis.flatten(), FlattenedVisibilityCache.flatten(new ArrayByteSequence(expression.getBytes(UTF_8))), expression);
        }
    }

    // -- discovery --

    @Test
    public void testGetInstanceIsStable() {
        assertInstanceOf(StandardVisibilityFunctions.class, functions);
        assertSame(functions, VisibilityFunctions.getInstance());
    }
}
