package datawave.microservice.annotation.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.MethodMetadata;
import org.springframework.core.type.classreading.SimpleMetadataReaderFactory;

class ConditionalOnAnnotationRoleTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void requiredRoleArraysArePresentInReflectionAndClassFileMetadata(boolean readClassFile) throws IOException {
        String annotationName = ConditionalOnAnnotationRole.class.getName();
        AnnotationMetadata classMetadata = metadata(ApiOnlyConfiguration.class, readClassFile);
        AnnotationAttributes attributes = AnnotationAttributes.fromMap(classMetadata.getAnnotationAttributes(annotationName));
        assertThat(attributes).isNotNull();
        assertThat((AnnotationRole[]) attributes.get("value")).containsExactly(AnnotationRole.API);

        AnnotationMetadata methodMetadata = metadata(RoleBeans.class, readClassFile);
        assertThat(methodMetadata.getAnnotatedMethods(annotationName)).hasSize(4);
        for (MethodMetadata method : methodMetadata.getAnnotatedMethods(annotationName)) {
            AnnotationAttributes methodAttributes = AnnotationAttributes.fromMap(method.getAnnotationAttributes(annotationName));
            assertThat(methodAttributes).isNotNull();
            AnnotationRole[] roles = (AnnotationRole[]) methodAttributes.get("value");
            assertThat(roles).isNotNull();
            // @formatter:off
            assertThat(method.getAnnotations()
                    .get(ConditionalOnAnnotationRole.class)
                    .getEnumArray("value", AnnotationRole.class))
                    .containsExactly(roles);
            // @formatter:on
            if ("noRolesBean".equals(method.getMethodName())) {
                assertThat(roles).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(AnnotationRole.class)
    void classAndBeanMethodConditionsSelectTheirDeclaredRoles(AnnotationRole role) {
        // @formatter:off
        new ApplicationContextRunner()
                .withUserConfiguration(RoleBeans.class, ApiOnlyConfiguration.class)
                .withPropertyValues("spring.profiles.active=" + role.profile())
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.containsBean("apiBean")).isEqualTo(role == AnnotationRole.API);
                    assertThat(context.containsBean("writerBean")).isEqualTo(role == AnnotationRole.WRITER);
                    assertThat(context.containsBean("combinedBean")).isEqualTo(role == AnnotationRole.COMBINED);
                    assertThat(context.containsBean("apiOnlyBean")).isEqualTo(role == AnnotationRole.API);
                    assertThat(context).doesNotHaveBean("noRolesBean");
                });
        // @formatter:on
    }

    private AnnotationMetadata metadata(Class<?> type, boolean readClassFile) throws IOException {
        return readClassFile ? new SimpleMetadataReaderFactory().getMetadataReader(type.getName()).getAnnotationMetadata()
                        : AnnotationMetadata.introspect(type);
    }

    @Configuration(proxyBeanMethods = false)
    static class RoleBeans {
        @Bean
        @ConditionalOnAnnotationRole(AnnotationRole.API)
        String apiBean() {
            return "api";
        }

        @Bean
        @ConditionalOnAnnotationRole(AnnotationRole.WRITER)
        String writerBean() {
            return "writer";
        }

        @Bean
        @ConditionalOnAnnotationRole(AnnotationRole.COMBINED)
        String combinedBean() {
            return "combined";
        }

        @Bean
        @ConditionalOnAnnotationRole({})
        String noRolesBean() {
            return "none";
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnAnnotationRole(AnnotationRole.API)
    static class ApiOnlyConfiguration {
        @Bean
        String apiOnlyBean() {
            return "api only";
        }
    }
}
