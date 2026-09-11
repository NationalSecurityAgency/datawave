package datawave.annotation.util.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.test.v1.AnnotationTestDataUtil;
import datawave.annotation.util.Validator;

/** tests basic annotation validation code, much of the validator functionality is tested in {@link datawave.annotation.util.ValidatorTest}. */

public class AnnotationValidatorsTest {

    @Test
    public void testAnnotationWithoutSource() {
        // demonstrates that an annotation without a source set in the builder will have an empty, non-null source object.
        Annotation a = Annotation.newBuilder().build();
        assertFalse(a.hasSource());
        AnnotationSource source = a.getSource();
        assertNotNull(source);
        assertEquals("", source.getAnalyticSourceHash());
        assertEquals("", source.getAnalyticHash());
        assertTrue(source.getMetadataMap().isEmpty());
    }

    @Test
    public void testAnnotationNullSource() {
        // demonstrates that the builder will throw a NullPointerException if a null source is set, as opposed to the default empty source object.
        // as a result, the validators don't need to explicitly check for null.
        assertThrows(NullPointerException.class, () -> {
            Annotation.newBuilder().setSource((AnnotationSource) null).build();
        });
    }

    @Test
    public void testAnnotationSourceValidators() {
        // demonstrates that the test annotation source is considered valid by the data validation checks, but fails the identity checks until the hashes are
        // injected.
        AnnotationSource testSource = AnnotationTestDataUtil.generateTestAnnotationSource();
        assertValid(AnnotationValidators.checkAnnotationSource(testSource));
        assertInvalid(AnnotationValidators.checkAnnotationSourceIds(testSource));
        AnnotationSource testIdentifiedSource = AnnotationUtils.injectAnnotationSourceHashes(testSource);
        assertValid(AnnotationValidators.checkAnnotationSource(testIdentifiedSource));
        assertValid(AnnotationValidators.checkAnnotationSourceIds(testIdentifiedSource));
    }

    @Test
    public void testAnnotationValidators() {
        // demonstrates that the test annotation is considered valid by the data validation checks, but fails the identity checks until the hashes are injected.
        Annotation testAnnotation = AnnotationTestDataUtil.generateTestAnnotation();
        assertValid(AnnotationValidators.checkAnnotation(testAnnotation));
        assertInvalid(AnnotationValidators.checkAnnotationIds(testAnnotation));
        Annotation testIdentifiedAnnotation = AnnotationUtils.injectAllHashes(testAnnotation);
        assertValid(AnnotationValidators.checkAnnotation(testIdentifiedAnnotation));
        assertValid(AnnotationValidators.checkAnnotationIds(testIdentifiedAnnotation));
    }

    @Test
    public void testSegmentValidators() {
        // demonstrates that the test segment is considered valid by the data validation checks, but fails the identity checks until the hashes are injected.
        Segment testSegment = AnnotationTestDataUtil.generateTestSegment();
        assertValid(AnnotationValidators.checkSegment(testSegment));
        assertInvalid(AnnotationValidators.checkSegmentIds(testSegment));
        Segment testIdentifiedSegment = AnnotationUtils.injectAllHashes(testSegment);
        assertValid(AnnotationValidators.checkSegment(testIdentifiedSegment));
        assertValid(AnnotationValidators.checkSegmentIds(testIdentifiedSegment));
    }

    public static void assertValid(Validator.ValidationState<?> validationState) {
        assertTrue(validationState.isValid(), getErrorMessage(validationState));
    }

    public static void assertInvalid(Validator.ValidationState<?> validationState) {
        assertFalse(validationState.isValid());
    }

    public static String getErrorMessage(Validator.ValidationState<?> validationState) {
        return validationState.getErrors() == null ? "(empty)" : validationState.getErrors().toString();
    }
}
