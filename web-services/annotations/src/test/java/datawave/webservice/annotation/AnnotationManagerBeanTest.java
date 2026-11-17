package datawave.webservice.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.query.config.annotation.AnnotationConfig;

public class AnnotationManagerBeanTest {

    @Test
    public void maskSourceMetadataReturnsOriginalWhenMaskListIsNull() throws Exception {
        AnnotationManagerBean bean = buildBeanWithMaskList(null);
        AnnotationSource source = sourceWithMetadata("visibility", "A&B", "source", "demo");

        AnnotationSource masked = bean.maskSourceMetadata(source);

        assertSame(source, masked);
    }

    @Test
    public void maskSourceMetadataReturnsOriginalWhenMaskListIsEmpty() throws Exception {
        AnnotationManagerBean bean = buildBeanWithMaskList(Collections.emptyList());
        AnnotationSource source = sourceWithMetadata("visibility", "A&B", "source", "demo");

        AnnotationSource masked = bean.maskSourceMetadata(source);

        assertSame(source, masked);
    }

    @Test
    public void maskSourceMetadataReturnsOriginalWhenNoMetadataMatches() throws Exception {
        AnnotationManagerBean bean = buildBeanWithMaskList(List.of("visibility"));
        AnnotationSource source = sourceWithMetadata("source", "demo");

        AnnotationSource masked = bean.maskSourceMetadata(source);

        assertSame(source, masked);
    }

    @Test
    public void maskSourceMetadataRemovesConfiguredKeyAndKeepsOtherMetadata() throws Exception {
        AnnotationManagerBean bean = buildBeanWithMaskList(List.of("visibility"));
        AnnotationSource source = sourceWithMetadata("visibility", "A&B", "source", "demo");

        AnnotationSource masked = bean.maskSourceMetadata(source);

        assertFalse(masked.containsMetadata("visibility"));
        assertTrue(masked.containsMetadata("source"));
        assertEquals("demo", masked.getMetadataMap().get("source"));
    }

    @Test
    public void maskSourceMetadataRemovesAllConfiguredKeys() throws Exception {
        AnnotationManagerBean bean = buildBeanWithMaskList(List.of("visibility", "securityMarking"));
        AnnotationSource source = sourceWithMetadata("visibility", "A&B", "securityMarking", "HIGH", "source", "demo");

        AnnotationSource masked = bean.maskSourceMetadata(source);

        assertFalse(masked.containsMetadata("visibility"));
        assertFalse(masked.containsMetadata("securityMarking"));
        assertEquals(1, masked.getMetadataCount());
        assertEquals("demo", masked.getMetadataMap().get("source"));
    }

    private static AnnotationManagerBean buildBeanWithMaskList(List<String> maskList) throws Exception {
        AnnotationConfig annotationConfig = new AnnotationConfig();
        annotationConfig.setMaskSourceMetadata(maskList);

        AnnotationManagerConfig config = new AnnotationManagerConfig();
        config.setAnnotationConfig(annotationConfig);

        AnnotationManagerBean bean = new AnnotationManagerBean();
        setField(bean, "config", config);
        return bean;
    }

    private static AnnotationSource sourceWithMetadata(String... keyValuePairs) {
        AnnotationSource.Builder builder = AnnotationSource.newBuilder();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            builder.putMetadata(keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return builder.build();
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Field findField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
        Class<?> current = clazz;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }
}
