package datawave.util.keyword;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Test;

public class VisibilityFlattenTest {
    @Test
    public void testFlattenAssociative() {
        String ab = "a&b";
        String ac = "a&c";

        String concat = "(" + ab + ")&(" + ac + ")";

        ColumnVisibility cv = new ColumnVisibility(concat);
        ColumnVisibility cvf = new ColumnVisibility(cv.flatten());

        System.err.println(cvf);
    }

    @Test
    public void testFlattenDuplicate() {
        String ab = "a";
        String ac = "a";

        String concat = "(" + ab + ")&(" + ac + ")";

        ColumnVisibility cv = new ColumnVisibility(concat);
        ColumnVisibility cvf = new ColumnVisibility(cv.flatten());

        System.err.println(cvf);
    }

    @Test
    public void testFlattenUnions() {
        String ab = "a&(b|d)";
        String ac = "((a&c)|(a&e))";

        String concat = "(" + ab + ")&(" + ac + ")";

        ColumnVisibility cv = new ColumnVisibility(concat);
        ColumnVisibility cvf = new ColumnVisibility(cv.flatten());
        cvf = new ColumnVisibility(cvf.flatten());

        System.err.println(cvf);
    }
}
