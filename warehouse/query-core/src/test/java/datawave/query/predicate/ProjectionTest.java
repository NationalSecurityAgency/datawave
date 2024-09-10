package datawave.query.predicate;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.Sets;

public class ProjectionTest {

    @Deprecated
    @Test(expected = RuntimeException.class)
    public void testNoConfigurationDepricated() {
        Projection projection = new Projection();
        assertTrue(projection.apply("FIELD_A"));
    }

    @Test(expected = RuntimeException.class)
    public void testNoConfiguration() {
        Projection projection = new Projection(null, Projection.ProjectionType.INCLUDES);
        assertTrue(projection.apply("FIELD_A"));
    }

    @Deprecated
    @Test(expected = RuntimeException.class)
    public void testTooMuchConfiguration() {
        Projection projection = new Projection();
        projection.setIncludes(Sets.newHashSet("FIELD_A", "FIELD_B"));
        projection.setExcludes(Sets.newHashSet("FIELD_X", "FIELD_Y"));
        assertTrue(projection.apply("FIELD_A"));
    }

    @Deprecated
    @Test(expected = RuntimeException.class)
    public void testTooMuchOfTheSameConfiguration() {
        Projection projection = new Projection();
        projection.setIncludes(Sets.newHashSet("FIELD_A", "FIELD_B"));
        projection.setIncludes(Sets.newHashSet("FIELD_A", "FIELD_B"));
        assertTrue(projection.apply("FIELD_A"));
    }

    @Deprecated
    @Test
    public void testIncludesDepricated() {
        Projection projection = new Projection();
        projection.setIncludes(Sets.newHashSet("FIELD_A", "FIELD_B"));

        assertTrue(projection.isUseIncludes());
        assertFalse(projection.isUseExcludes());

        assertTrue(projection.apply("FIELD_A"));
        assertTrue(projection.apply("FIELD_B"));
        assertFalse(projection.apply("FIELD_C"));
        assertFalse(projection.apply("FIELD_X"));
        assertFalse(projection.apply("FIELD_Y"));
        assertFalse(projection.apply("FIELD_Z"));
    }

    @Test
    public void testIncludes() {
        Projection projection = new Projection(Sets.newHashSet("FIELD_A", "FIELD_B"), Projection.ProjectionType.INCLUDES);

        assertTrue(projection.isUseIncludes());
        assertFalse(projection.isUseExcludes());

        assertTrue(projection.apply("FIELD_A"));
        assertTrue(projection.apply("FIELD_B"));
        assertFalse(projection.apply("FIELD_C"));
        assertFalse(projection.apply("FIELD_X"));
        assertFalse(projection.apply("FIELD_Y"));
        assertFalse(projection.apply("FIELD_Z"));
    }

    @Deprecated
    @Test
    public void testIncludesWithGroupingContextDeprecated() {
        Projection projection = new Projection();
        projection.setIncludes(Sets.newHashSet("FIELD_A", "FIELD_B"));

        assertTrue(projection.isUseIncludes());
        assertFalse(projection.isUseExcludes());

        assertTrue(projection.apply("$FIELD_A"));
        assertTrue(projection.apply("FIELD_B.0"));
        assertFalse(projection.apply("FIELD_C"));
        assertFalse(projection.apply("$FIELD_X"));
        assertFalse(projection.apply("FIELD_Y.0"));
        assertFalse(projection.apply("FIELD_Z"));
    }

    @Test
    public void testIncludesWithGroupingContext() {
        Projection projection = new Projection(Sets.newHashSet("FIELD_A", "FIELD_B"), Projection.ProjectionType.INCLUDES);

        assertTrue(projection.isUseIncludes());
        assertFalse(projection.isUseExcludes());

        assertTrue(projection.apply("$FIELD_A"));
        assertTrue(projection.apply("FIELD_B.0"));
        assertFalse(projection.apply("FIELD_C"));
        assertFalse(projection.apply("$FIELD_X"));
        assertFalse(projection.apply("FIELD_Y.0"));
        assertFalse(projection.apply("FIELD_Z"));
    }

    @Deprecated
    @Test
    public void testExcludesDeprecated() {
        Projection projection = new Projection();
        projection.setExcludes(Sets.newHashSet("FIELD_X", "FIELD_Y"));

        assertFalse(projection.isUseIncludes());
        assertTrue(projection.isUseExcludes());

        assertTrue(projection.apply("FIELD_A"));
        assertTrue(projection.apply("FIELD_B"));
        assertTrue(projection.apply("FIELD_C"));
        assertFalse(projection.apply("FIELD_X"));
        assertFalse(projection.apply("FIELD_Y"));
        assertTrue(projection.apply("FIELD_Z"));
    }

    @Test
    public void testExcludes() {
        Projection projection = new Projection(Sets.newHashSet("FIELD_X", "FIELD_Y"), Projection.ProjectionType.EXCLUDES);

        assertFalse(projection.isUseIncludes());
        assertTrue(projection.isUseExcludes());

        assertTrue(projection.apply("FIELD_A"));
        assertTrue(projection.apply("FIELD_B"));
        assertTrue(projection.apply("FIELD_C"));
        assertFalse(projection.apply("FIELD_X"));
        assertFalse(projection.apply("FIELD_Y"));
        assertTrue(projection.apply("FIELD_Z"));
    }

    @Deprecated
    @Test
    public void testExcludesWithGroupingContextDeprecated() {
        Projection projection = new Projection();
        projection.setExcludes(Sets.newHashSet("FIELD_X", "FIELD_Y"));

        assertFalse(projection.isUseIncludes());
        assertTrue(projection.isUseExcludes());

        assertTrue(projection.apply("$FIELD_A"));
        assertTrue(projection.apply("FIELD_B.0"));
        assertTrue(projection.apply("FIELD_C"));
        assertFalse(projection.apply("$FIELD_X"));
        assertFalse(projection.apply("FIELD_Y.0"));
        assertTrue(projection.apply("FIELD_Z"));
    }

    @Test
    public void testExcludesWithGroupingContext() {
        Projection projection = new Projection(Sets.newHashSet("FIELD_X", "FIELD_Y"), Projection.ProjectionType.EXCLUDES);

        assertFalse(projection.isUseIncludes());
        assertTrue(projection.isUseExcludes());

        assertTrue(projection.apply("$FIELD_A"));
        assertTrue(projection.apply("FIELD_B.0"));
        assertTrue(projection.apply("FIELD_C"));
        assertFalse(projection.apply("$FIELD_X"));
        assertFalse(projection.apply("FIELD_Y.0"));
        assertTrue(projection.apply("FIELD_Z"));
    }

    @Deprecated
    @Test
    public void testTheAbsurdDeprecated() {
        Projection projection = new Projection();
        projection.setIncludes(Sets.newHashSet("PREFIX"));
        assertTrue(projection.apply("$PREFIX.SUFFIX01.SUFFIX.02"));
    }

    @Test
    public void testTheAbsurd() {
        Projection projection = new Projection(Sets.newHashSet("PREFIX"), Projection.ProjectionType.INCLUDES);
        assertTrue(projection.apply("$PREFIX.SUFFIX01.SUFFIX.02"));
    }
}
