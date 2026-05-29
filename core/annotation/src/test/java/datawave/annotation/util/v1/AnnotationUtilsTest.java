package datawave.annotation.util.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import datawave.annotation.data.transform.DefaultVisibilityTransformer;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.test.v1.AnnotationTestDataUtil;

public class AnnotationUtilsTest {
    @Test
    public void testInjectAnnotationSourceWithVisibilityInheritanceFeature() throws Exception {
        // annotation is built with PUBLIC visibility only
        Annotation a = AnnotationTestDataUtil.generateTestAnnotation();

        // generating annotation source which two combined visibilities to test the inherit feature
        AnnotationSource as = AnnotationTestDataUtil.generateTestAnnotationSource("(PRIVATE|PUBLIC)");

        // with inherit feature enabled, annotation source should only have the one from annotation
        AnnotationSource asWithInheritedVisibility = AnnotationTestDataUtil.generateTestAnnotationSource("PUBLIC");

        //@formatter:off
        assertEquals(
                AnnotationUtils.injectAnnotationSource(a, as).getSource(),
                as);

        assertEquals(
                AnnotationUtils.injectAnnotationSource(a, as, false, new DefaultVisibilityTransformer()).getSource(),
                as);

        assertEquals(
                AnnotationUtils.injectAnnotationSource(a, as, true, new DefaultVisibilityTransformer()).getSource(),
                asWithInheritedVisibility);
        //@formatter:on
    }
}
