package datawave.marking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;

public class MarkingFunctionsDefaultCombineTest {

    @Test
    public void testCombineAnds() throws MarkingFunctions.Exception {
        MarkingFunctions<AccessExpressionMarkings> markingFunctions = new MarkingFunctions.Default();

        AccessExpressionMarkings oneAnna = AccessExpressionMarkings.builder().columnVisibility("A&B").build();
        AccessExpressionMarkings twoAnna = AccessExpressionMarkings.builder().columnVisibility("A&C").build();

        AccessExpressionMarkings combined = markingFunctions.combine(List.of(oneAnna, twoAnna));
        assertEquals(AccessExpressionMarkings.builder().columnVisibility("A&B&C").build(), combined);
    }

    @Test
    public void testCombineOrs() throws MarkingFunctions.Exception {
        MarkingFunctions<AccessExpressionMarkings> markingFunctions = new MarkingFunctions.Default();

        AccessExpressionMarkings oneOr = AccessExpressionMarkings.builder().columnVisibility("A|B").build();
        AccessExpressionMarkings twoOr = AccessExpressionMarkings.builder().columnVisibility("A|C").build();

        AccessExpressionMarkings combined = markingFunctions.combine(List.of(oneOr, twoOr));
        assertEquals(AccessExpressionMarkings.builder().columnVisibility("(A|B)&(A|C)").build(), combined);
    }

    @Test
    public void testCombineMapsOfAnds() throws MarkingFunctions.Exception {
        MarkingFunctions<AccessExpressionMarkings> markingFunctions = new MarkingFunctions.Default();

        AccessExpressionMarkings cv1 = AccessExpressionMarkings.builder().columnVisibility("A&B").build();
        AccessExpressionMarkings cv2 = AccessExpressionMarkings.builder().columnVisibility("A&C").build();
        AccessExpressionMarkings expected = AccessExpressionMarkings.builder().columnVisibility("A&B&C").build();

        assertEquals(expected, markingFunctions.combine(List.of(cv1, cv2)));
    }

    @Test
    public void testCombineMapsOfOrs() throws MarkingFunctions.Exception {
        MarkingFunctions<AccessExpressionMarkings> markingFunctions = new MarkingFunctions.Default();

        AccessExpressionMarkings cv1 = AccessExpressionMarkings.builder().columnVisibility("A|B").build();
        AccessExpressionMarkings cv2 = AccessExpressionMarkings.builder().columnVisibility("A|C").build();
        AccessExpressionMarkings expected = AccessExpressionMarkings.builder().columnVisibility("(A|B)&(A|C)").build();

        assertEquals(expected, markingFunctions.combine(List.of(cv1, cv2)));
    }

    @Test
    public void testCombineSkipsNullMarkings() throws MarkingFunctions.Exception {
        MarkingFunctions<AccessExpressionMarkings> markingFunctions = new MarkingFunctions.Default();

        AccessExpressionMarkings aem1 = AccessExpressionMarkings.builder().columnVisibility("A&B").build();
        AccessExpressionMarkings aem2 = AccessExpressionMarkings.builder().columnVisibility("A&C").build();
        AccessExpressionMarkings expected = AccessExpressionMarkings.builder().columnVisibility("A&B&C").build();

        Collection<Markings<?>> markings = new ArrayList<>(Arrays.asList(aem1, null, aem2, null));
        assertEquals(expected, markingFunctions.combine(markings));
    }

    @Test
    public void testCombineAllNullMarkingsReturnsNull() throws MarkingFunctions.Exception {
        MarkingFunctions<AccessExpressionMarkings> markingFunctions = new MarkingFunctions.Default();

        Collection<Markings<?>> markings = new ArrayList<>(Arrays.asList(null, null));
        assertNull(markingFunctions.combine(markings));
    }

    @Test
    public void testCombineVisibilitiesSkipsNullElements() throws MarkingFunctions.Exception {
        MarkingFunctions<AccessExpressionMarkings> markingFunctions = new MarkingFunctions.Default();

        ColumnVisibility vis1 = new ColumnVisibility("A&B");
        ColumnVisibility vis2 = new ColumnVisibility("A&C");

        Collection<ColumnVisibility> visibilities = new ArrayList<>(Arrays.asList(vis1, null, vis2, null));
        ColumnVisibility combined = markingFunctions.combineVisibilities(visibilities);
        assertEquals(new ColumnVisibility("A&B&C"), combined);
    }

    @Test
    public void testCombineVisibilitiesAllNullsReturnsEmpty() throws MarkingFunctions.Exception {
        MarkingFunctions<AccessExpressionMarkings> markingFunctions = new MarkingFunctions.Default();

        Collection<ColumnVisibility> visibilities = new ArrayList<>(Arrays.asList(null, null));
        assertEquals(new ColumnVisibility(), markingFunctions.combineVisibilities(visibilities));
    }
}
