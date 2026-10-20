package datawave.microservice.annotation.config;

import java.util.Arrays;
import java.util.Locale;

import org.springframework.core.env.Environment;

/**
 * Selects the responsibilities of an annotation deployment from exactly one explicit role profile.
 */
public enum AnnotationRole {
    COMBINED, API, WRITER;

    public boolean includesApi() {
        return this != WRITER;
    }

    public boolean includesWriter() {
        return this != API;
    }

    public String profile() {
        return "annotation-" + name().toLowerCase(Locale.ROOT);
    }

    public static AnnotationRole from(Environment environment) {
        AnnotationRole[] selected = Arrays.stream(values()).filter(role -> Arrays.asList(environment.getActiveProfiles()).contains(role.profile()))
                        .toArray(AnnotationRole[]::new);
        if (selected.length != 1) {
            throw new IllegalStateException(
                            "Activate exactly one of annotation-api, annotation-writer, and annotation-combined; selected " + Arrays.toString(selected));
        }
        return selected[0];
    }
}
