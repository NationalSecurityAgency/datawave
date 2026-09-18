package datawave.microservice.annotation.service.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.FileSystemResource;

import datawave.query.config.annotation.AnnotationConfig;

/**
 * Cross-module parity assertion test.
 * <p>
 * Verifies that {@link AnnotationProperties} (microservice Spring config) carries the same default values as {@link AnnotationConfig} (legacy monolith config)
 * for every shared read-path field. This guards against the microservice silently diverging from the legacy defaults as the codebase evolves.
 * <p>
 * Note: {@code visibilityTransformer} and {@code timestampTransformer} are injected via Spring DI (not part of the properties/config POJO) and are therefore
 * excluded from this parity check.
 */
public class TestAnnotationConfigParity {

    @Test
    public void testPackagedBootstrapConfigurationLoads() throws Exception {
        var propertySources = loadYaml("bootstrap", projectDirectory().resolve("src/main/resources/config/bootstrap.yml"));
        assertEquals(4, propertySources.size(), "bootstrap.yml should contain its base, dev, consul, and nomessaging documents");
    }

    @Test
    public void testExternalConfigurationParity() throws Exception {
        PropertySource<?> serviceConfig = loadYaml("service-config", projectDirectory().resolve("src/main/config/annotation.yml")).get(0);
        PropertySource<?> dockerConfig = loadYaml("docker-config", projectDirectory().resolve("../../../../docker/config/annotation.yml")).get(0);

        assertEquals(serviceConfig.getProperty("spring.cloud.function.definition"), dockerConfig.getProperty("spring.cloud.function.definition"));
        assertEquals(serviceConfig.getProperty("annotation.system-from"), dockerConfig.getProperty("annotation.system-from"));
        assertEquals(serviceConfig.getProperty("annotation.writers.accumulo.enabled"), dockerConfig.getProperty("annotation.writers.accumulo.enabled"));
        assertEquals(serviceConfig.getProperty("annotation.writers.accumulo.health.enabled"),
                        dockerConfig.getProperty("annotation.writers.accumulo.health.enabled"));
        assertNull(dockerConfig.getProperty("annotation.writers.accumulo.annotationTableName"),
                        "the Accumulo writer uses truthmark table properties, not annotation read-table properties");
        assertNull(dockerConfig.getProperty("annotation.writers.accumulo.annotationSourceTableName"),
                        "the Accumulo writer uses truthmark source-table properties, not annotation read-table properties");
    }

    @Test
    public void testAnnotationTableNameDefaultParity() {
        AnnotationConfig legacy = new AnnotationConfig();
        AnnotationProperties microservice = new AnnotationProperties();
        assertEquals(legacy.getAnnotationTableName(), microservice.getAnnotationTableName(),
                        "annotationTableName default must match between legacy and microservice configs");
    }

    @Test
    public void testAnnotationSourceTableNameDefaultParity() {
        AnnotationConfig legacy = new AnnotationConfig();
        AnnotationProperties microservice = new AnnotationProperties();
        assertEquals(legacy.getAnnotationSourceTableName(), microservice.getAnnotationSourceTableName(),
                        "annotationSourceTableName default must match between legacy and microservice configs");
    }

    @Test
    public void testTruthmarkTableNameDefaultParity() {
        AnnotationConfig legacy = new AnnotationConfig();
        AnnotationProperties microservice = new AnnotationProperties();
        assertEquals("truthmark", legacy.getTruthmarkTableName(), "legacy truthmarkTableName should default to 'truthmark'");
        assertEquals("truthmark", microservice.getTruthmarkTableName(), "microservice truthmarkTableName should default to 'truthmark'");
        assertEquals(legacy.getTruthmarkTableName(), microservice.getTruthmarkTableName(),
                        "truthmarkTableName default must match between legacy and microservice configs");
    }

    @Test
    public void testTruthmarkSourceTableNameDefaultParity() {
        AnnotationConfig legacy = new AnnotationConfig();
        AnnotationProperties microservice = new AnnotationProperties();
        assertEquals("truthmarkSource", legacy.getTruthmarkSourceTableName(), "legacy truthmarkSourceTableName should default to 'truthmarkSource'");
        assertEquals("truthmarkSource", microservice.getTruthmarkSourceTableName(),
                        "microservice truthmarkSourceTableName should default to 'truthmarkSource'");
        assertEquals(legacy.getTruthmarkSourceTableName(), microservice.getTruthmarkSourceTableName(),
                        "truthmarkSourceTableName default must match between legacy and microservice configs");
    }

    @Test
    public void testMaskSourceMetadataDefaultParity() {
        AnnotationConfig legacy = new AnnotationConfig();
        AnnotationProperties microservice = new AnnotationProperties();
        assertNotNull(legacy.getMaskSourceMetadata(), "legacy maskSourceMetadata should not be null");
        assertNotNull(microservice.getMaskSourceMetadata(), "microservice maskSourceMetadata should not be null");
        assertEquals(legacy.getMaskSourceMetadata(), microservice.getMaskSourceMetadata(),
                        "maskSourceMetadata default must match between legacy and microservice configs");
        assertEquals(List.of("visibility"), microservice.getMaskSourceMetadata(), "maskSourceMetadata should default to ['visibility']");
    }

    private static Path projectDirectory() {
        return Path.of(System.getProperty("basedir")).toAbsolutePath().normalize();
    }

    private static List<PropertySource<?>> loadYaml(String name, Path path) throws Exception {
        return new YamlPropertySourceLoader().load(name, new FileSystemResource(path));
    }
}
