package datawave.visibility;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;

import datawave.marking.MarkingFunctions;

/**
 * Verifies that {@link VisibilityFunctions.Default} behaves exactly like the {@link MarkingFunctions.Default} it replaces.
 * <p>
 * TODO remove-accumulo-utils: delete this test together with MarkingFunctions in Phase 3.
 */
public class VisibilityFunctionsParityTest {

    private static final List<List<String>> INPUTS = List.of(List.of("A&B", "A&C"), List.of("A|B", "A|C"), List.of("B&A"), List.of("A&(B&C)", "D"),
                    List.of("(Z&Y)|(C&B)", "X"), List.of("\"ABC\"&\"XYZ\"", "ABC"), List.of("A|B", "A|B"), List.of("C", "B|A", "A&C"),
                    List.of("(A|B)&(C|D)", "E|F"), List.of("", "A"), List.of("", ""));

    private final VisibilityFunctions functions = new VisibilityFunctions.Default();
    private final MarkingFunctions<?> markingFunctions = new MarkingFunctions.Default();

    @Test
    public void testCombineParity() throws MarkingFunctions.Exception {
        for (List<String> input : INPUTS) {
            Collection<ColumnVisibility> visibilities = new ArrayList<>();
            input.forEach(expression -> visibilities.add(new ColumnVisibility(expression)));
            visibilities.add(null);
            assertEquals(markingFunctions.combineVisibilities(visibilities), functions.combine(visibilities), input.toString());
        }
    }

    @Test
    public void testCombineAllNullParity() throws MarkingFunctions.Exception {
        Collection<ColumnVisibility> visibilities = new ArrayList<>(Arrays.asList(null, null));
        assertEquals(markingFunctions.combineVisibilities(visibilities), functions.combine(visibilities));
    }

    @Test
    public void testFlattenParity() {
        for (List<String> input : INPUTS) {
            for (String expression : input) {
                ColumnVisibility vis = new ColumnVisibility(expression);
                assertArrayEquals(markingFunctions.flatten(vis), functions.flatten(vis), expression);
            }
        }
    }
}
