package datawave.visibility;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.ServiceConfigurationError;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class VisibilityProviderLoaderTest {
    @Test
    void spiAloneHasNoProvider() {
        IllegalStateException error = assertThrows(IllegalStateException.class, VisibilityFunctions::getInstance);
        assertTrue(error.getMessage().contains("No VisibilityFunctions provider registered"));
    }

    @Test
    void discoversSingleRegistration(@TempDir Path root) throws IOException {
        try (URLClassLoader loader = registered(root, TestProvider.class.getName())) {
            assertInstanceOf(TestProvider.class, VisibilityProviderLoader.load(loader));
        }
    }

    @Test
    void rejectsCompetingRegistrations(@TempDir Path root) throws IOException {
        try (URLClassLoader loader = registered(root, TestProvider.class.getName(), OtherProvider.class.getName())) {
            IllegalStateException error = assertThrows(IllegalStateException.class, () -> VisibilityProviderLoader.load(loader));
            assertTrue(error.getMessage().contains(TestProvider.class.getName()));
            assertTrue(error.getMessage().contains(OtherProvider.class.getName()));
        }
    }

    @Test
    void malformedRegistrationIsNotTreatedAsAnAbsentProvider(@TempDir Path root) throws IOException {
        try (URLClassLoader loader = registered(root, "missing.VisibilityPolicy")) {
            IllegalStateException error = assertThrows(IllegalStateException.class, () -> VisibilityProviderLoader.load(loader));
            assertInstanceOf(ServiceConfigurationError.class, error.getCause());
        }
    }

    private URLClassLoader registered(Path root, String... providers) throws IOException {
        Path services = Files.createDirectories(root.resolve("META-INF/services"));
        Files.write(services.resolve(VisibilityFunctions.class.getName()), List.of(providers));
        return new URLClassLoader(new URL[] {root.toUri().toURL()}, VisibilityFunctions.class.getClassLoader());
    }

    public static class TestProvider implements VisibilityFunctions {
        @Override
        public ColumnVisibility combine(Collection<ColumnVisibility> inputs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] flatten(ColumnVisibility input) {
            throw new UnsupportedOperationException();
        }
    }

    public static class OtherProvider extends TestProvider {}
}
