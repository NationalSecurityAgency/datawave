package datawave.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.annotations.VisibleForTesting;

public class FullPathGlobFilterTest {
    @Test
    public void testEmptyPatternsAlwaysRejected() throws IOException {
        FullPathGlobFilter filter = new FullPathGlobFilter(Collections.emptySet());
        assertFalse(filter.accept(new Path("/")));
        assertFalse(filter.accept(new Path("/2023")));
        assertFalse(filter.accept(new Path("/2023/09")));
        assertFalse(filter.accept(new Path("/one/2023/09/")));
    }

    @Test
    public void testTopLevelOnly() throws IOException {
        FullPathGlobFilter filter = new FullPathGlobFilter(Collections.singletonList("/*"));
        assertTrue(filter.accept(new Path("/")));
        assertTrue(filter.accept(new Path("/2023")));
        assertFalse(filter.accept(new Path("/2023/09")));
        assertFalse(filter.accept(new Path("/one/2023/09/")));
    }

    @Test
    public void testSecondLevelOnly() throws IOException {
        FullPathGlobFilter filter = new FullPathGlobFilter(Collections.singletonList("/*/*"));
        assertFalse(filter.accept(new Path("/")));
        assertFalse(filter.accept(new Path("/2023")));
        assertTrue(filter.accept(new Path("/2023/09")));
        assertFalse(filter.accept(new Path("/one/2023/09/")));
    }

    @Test
    public void testThirdLevelOnly() throws IOException {
        FullPathGlobFilter filter = new FullPathGlobFilter(Collections.singletonList("/*/*/*"));
        assertFalse(filter.accept(new Path("/")));
        assertFalse(filter.accept(new Path("/2023")));
        assertFalse(filter.accept(new Path("/2023/09")));
        assertTrue(filter.accept(new Path("/one/2023/09/")));
    }

    @Test
    public void testFlagMakerConfigPattern() throws IOException {
        FullPathGlobFilter filter = new FullPathGlobFilter(Collections.singletonList("/2*/*/*/[0-9a-zA-Z]*[0-9a-zA-Z]"));

        assertFalse(filter.accept(new Path("/")));
        assertFalse(filter.accept(new Path("/2023")));
        assertFalse(filter.accept(new Path("/2023/09")));
        assertFalse(filter.accept(new Path("/one/2023/09/")));

        // baseline cases that are accepted
        assertTrue(filter.accept(new Path("/2/0/0/00")));
        assertTrue(filter.accept(new Path("/2023/09/01/0123456")));

        // verify patterns on the last level
        assertTrue("last level may contain non-alphanumeric in certain positions", filter.accept(new Path("/2023/09/01/0-123456")));
        assertTrue("last level may contain non-alphanumeric in certain positions", filter.accept(new Path("/2023/09/01/012345-6")));
        assertFalse("last level may only end with alphanumeric", filter.accept(new Path("/2023/09/01/0123456-")));
        assertFalse("last level may only start with alphanumeric", filter.accept(new Path("/2023/09/01/-0123456")));
        assertFalse("last level too short", filter.accept(new Path("/2023/09/01/0")));
        assertTrue("last level minimum size", filter.accept(new Path("/2023/09/01/01")));
        assertFalse("last level minimum size", filter.accept(new Path("/2023/09/01/0-")));
        assertFalse("last level minimum size", filter.accept(new Path("/2023/09/01/-0")));

        // verify patterns on first level
        assertTrue(filter.accept(new Path("/2023/09/01/0123456")));
        assertFalse(filter.accept(new Path("/3023/09/01/0123456")));
        assertTrue(filter.accept(new Path("/2-/0/0/00")));
        assertFalse(filter.accept(new Path("/-2/0/0/00")));
        assertFalse(filter.accept(new Path("/3/0/0/00")));

        // verify patterns on second and third levels
        assertTrue(filter.accept(new Path("/2023/09/01/0123456")));
        assertTrue(filter.accept(new Path("/2023/09/-/0123456")));
        assertTrue(filter.accept(new Path("/2023/-/01/0123456")));
        assertFalse("cannot be empty", filter.accept(new Path("/2023//01/0123456")));
        assertFalse("cannot be empty", filter.accept(new Path("/2023/09//0123456")));
    }

    @Test
    public void testFlagMakerConfigPatternVariation() throws IOException {
        FullPathGlobFilter filter = new FullPathGlobFilter(Collections.singletonList("/2*/*/*/*/[0-9a-zA-Z]*[0-9a-zA-Z]"));
        assertFalse(filter.accept(new Path("/")));
        assertFalse(filter.accept(new Path("/2023")));
        assertFalse(filter.accept(new Path("/2023/09")));
        assertFalse(filter.accept(new Path("/one/2023/09/")));

        assertFalse(filter.accept(new Path("/")));
        assertFalse(filter.accept(new Path("/2023")));
        assertFalse(filter.accept(new Path("/2023/09")));
        assertFalse(filter.accept(new Path("/one/2023/09/")));

        // baseline cases that are accepted
        assertTrue(filter.accept(new Path("/2/0/0/0/00")));
        assertTrue(filter.accept(new Path("/2023/09/01/01/0123456")));

        // verify patterns on the last level
        assertTrue("last level may contain non-alphanumeric in certain positions", filter.accept(new Path("/2023/09/01/01/0-123456")));
        assertTrue("last level may contain non-alphanumeric in certain positions", filter.accept(new Path("/2023/09/01/01/012345-6")));
        assertFalse("last level may only end with alphanumeric", filter.accept(new Path("/2023/09/01/01/0123456-")));
        assertFalse("last level may only start with alphanumeric", filter.accept(new Path("/2023/09/01/01/-0123456")));
        assertFalse("last level too short", filter.accept(new Path("/2023/09/01/01/0")));
        assertTrue("last level minimum size", filter.accept(new Path("/2023/09/01/01/01")));
        assertFalse("last level minimum size", filter.accept(new Path("/2023/09/01/01/0-")));
        assertFalse("last level minimum size", filter.accept(new Path("/2023/09/01/01/-0")));

        // verify patterns on first level
        assertTrue(filter.accept(new Path("/2023/09/01/01/0123456")));
        assertFalse(filter.accept(new Path("/3023/09/01/01/0123456")));
        assertTrue(filter.accept(new Path("/2-/0/0/01/00")));
        assertFalse(filter.accept(new Path("/-2/0/0/01/00")));
        assertFalse(filter.accept(new Path("/3/0/0/01/00")));

        // verify patterns on second, third, and fourth levels
        assertTrue(filter.accept(new Path("/2023/09/01/01/0123456")));
        assertTrue(filter.accept(new Path("/2023/-/01/01/0123456")));
        assertTrue(filter.accept(new Path("/2023/09/-/01/0123456")));
        assertTrue(filter.accept(new Path("/2023/09/01/-/0123456")));
        assertFalse("cannot be empty", filter.accept(new Path("/2023//01/01/0123456")));
        assertFalse("cannot be empty", filter.accept(new Path("/2023/09//01/0123456")));
        assertFalse("cannot be empty", filter.accept(new Path("/2023/09/01//0123456")));
    }

    @Test(expected = AssertionError.class)
    public void testNullPath() throws IOException {
        FullPathGlobFilter filter = new FullPathGlobFilter(Collections.emptySet());
        filter.accept(null);
    }
}
