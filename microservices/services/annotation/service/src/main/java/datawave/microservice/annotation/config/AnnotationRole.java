package datawave.microservice.annotation.config;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import org.springframework.core.env.Environment;

/**
 * Selects the responsibilities of an annotation deployment from exactly one explicit role profile.
 */
public enum AnnotationRole {
    COMBINED, API, WRITER;

    // @formatter:off
    private static final String ALL_PROFILES = Arrays.stream(values())
            .map(AnnotationRole::profile)
            .collect(Collectors.joining(", "));
    // @formatter:on

    private final String profile;

    AnnotationRole() {
        this.profile = name().toLowerCase(Locale.ROOT);
    }

    public boolean includesApi() {
        return this != WRITER;
    }

    public boolean includesWriter() {
        return this != API;
    }

    public String profile() {
        return profile;
    }

    public static AnnotationRole from(Environment environment) {
        // @formatter:off
        AnnotationRole[] selected = Arrays.stream(values())
                .filter(role -> Arrays.asList(environment.getActiveProfiles()).contains(role.profile()))
                .toArray(AnnotationRole[]::new);
        // @formatter:on
        if (selected.length != 1) {
            throw new IllegalStateException("Activate exactly one of " + ALL_PROFILES + "; selected " + Arrays.toString(selected));
        }
        return selected[0];
    }
}
