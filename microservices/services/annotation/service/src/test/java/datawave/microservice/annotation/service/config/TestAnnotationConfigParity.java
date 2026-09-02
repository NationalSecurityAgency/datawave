package datawave.microservice.annotation.service.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;

import org.junit.jupiter.api.Test;

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
}
