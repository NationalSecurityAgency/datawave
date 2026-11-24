package example.visibility;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.junit.jupiter.api.Test;

import datawave.visibility.VisibilityFunctions;

class AbsorbingVisibilityFunctionsTest {
    private final VisibilityFunctions functions = VisibilityFunctions.getInstance();

    @Test
    void discoversProviderAndRemovesOnlySatisfiedAlternatives() {
        assertInstanceOf(AbsorbingVisibilityFunctions.class, functions);
        assertEquals(new ColumnVisibility("A&(C|D)"), functions.combine(List.of(new ColumnVisibility("A"), new ColumnVisibility("B|A"),
                        new ColumnVisibility("C|D"))));
    }

    @Test
    void respectsQuotedLabelsAndNestedBranches() throws Exception {
        // "A|B" is one label, distinct from the expression A|B. Only the first OR can be removed.
        List<ColumnVisibility> inputs = List.of(new ColumnVisibility("\"A|B\""), new ColumnVisibility("C|\"A|B\""), new ColumnVisibility("A|B"),
                        new ColumnVisibility("(\"A|B\"&C)|D"));
        ColumnVisibility result = functions.combine(inputs);
        String[] labels = {"A|B", "A", "B", "C", "D"};
        for (int mask = 0; mask < (1 << labels.length); mask++) {
            List<String> granted = new ArrayList<>();
            for (int i = 0; i < labels.length; i++) {
                if ((mask & (1 << i)) != 0) {
                    granted.add(labels[i]);
                }
            }
            VisibilityEvaluator evaluator = new VisibilityEvaluator(new Authorizations(granted.toArray(new String[0])));
            boolean allAllowed = true;
            for (ColumnVisibility input : inputs) {
                allAllowed &= evaluator.evaluate(input);
            }
            assertEquals(allAllowed, evaluator.evaluate(result), granted.toString());
        }
    }

    @Test
    void retainsDefaultNullEmptyAndFlattenContracts() {
        ColumnVisibility vis = new ColumnVisibility("B&A");
        assertNull(functions.combine(null, null));
        assertSame(vis, functions.combine(null, vis));
        assertEquals(new ColumnVisibility(), functions.combine(null));
        assertEquals(new ColumnVisibility(), functions.combine(Arrays.asList(null, new ColumnVisibility())));
        assertArrayEquals(vis.flatten(), functions.flatten(vis));
    }
}
