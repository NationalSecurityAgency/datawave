package datawave.visibility;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class VisibilityFunctionsTest {

    private final VisibilityFunctions functions = new VisibilityFunctions.Default();

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
        assertSame(VisibilityFunctions.getInstance(), VisibilityFunctions.getInstance());
    }

    @Test
    public void testLoadWithoutProvidersUsesDefault() {
        assertInstanceOf(VisibilityFunctions.Default.class, VisibilityFunctions.Holder.load(null, getClass().getClassLoader()));
    }

    @Test
    public void testLoadWithOverride() {
        VisibilityFunctions loaded = VisibilityFunctions.Holder.load(TestProvider.class.getName(), getClass().getClassLoader());
        assertInstanceOf(TestProvider.class, loaded);
    }

    @Test
    public void testLoadWithBlankOverrideUsesDiscovery() {
        assertInstanceOf(VisibilityFunctions.Default.class, VisibilityFunctions.Holder.load("  ", getClass().getClassLoader()));
    }

    @Test
    public void testLoadWithMissingOverrideClassFails() {
        assertThrows(IllegalStateException.class, () -> VisibilityFunctions.Holder.load("datawave.visibility.DoesNotExist", getClass().getClassLoader()));
    }

    @Test
    public void testLoadWithOverrideOfWrongTypeFails() {
        assertThrows(IllegalStateException.class, () -> VisibilityFunctions.Holder.load(String.class.getName(), getClass().getClassLoader()));
    }

    @Test
    public void testLoadWithSingleProvider(@TempDir Path dir) throws IOException {
        try (URLClassLoader loader = loaderWithProviders(dir, TestProvider.class.getName())) {
            assertInstanceOf(TestProvider.class, VisibilityFunctions.Holder.load(null, loader));
        }
    }

    @Test
    public void testOverrideTakesPrecedenceOverProvider(@TempDir Path dir) throws IOException {
        try (URLClassLoader loader = loaderWithProviders(dir, TestProvider.class.getName())) {
            assertInstanceOf(OtherTestProvider.class, VisibilityFunctions.Holder.load(OtherTestProvider.class.getName(), loader));
        }
    }

    @Test
    public void testLoadWithMultipleProvidersFails(@TempDir Path dir) throws IOException {
        try (URLClassLoader loader = loaderWithProviders(dir, TestProvider.class.getName(), OtherTestProvider.class.getName())) {
            assertThrows(IllegalStateException.class, () -> VisibilityFunctions.Holder.load(null, loader));
        }
    }

    @Test
    public void testLoadWithBrokenProviderFails(@TempDir Path dir) throws IOException {
        try (URLClassLoader loader = loaderWithProviders(dir, "datawave.visibility.DoesNotExist")) {
            assertThrows(IllegalStateException.class, () -> VisibilityFunctions.Holder.load(null, loader));
        }
    }

    private URLClassLoader loaderWithProviders(Path dir, String... providers) throws IOException {
        Path services = dir.resolve("META-INF").resolve("services");
        Files.createDirectories(services);
        Files.write(services.resolve(VisibilityFunctions.class.getName()), String.join("\n", providers).getBytes(UTF_8));
        return new URLClassLoader(new URL[] {dir.toUri().toURL()}, getClass().getClassLoader());
    }

    public static class TestProvider extends VisibilityFunctions.Default {}

    public static class OtherTestProvider extends VisibilityFunctions.Default {}
}
