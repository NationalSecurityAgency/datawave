package datawave.accumulo.inmemory;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Validators for Accumulo table and namespace names. Replaces non-public class: {@code org.apache.accumulo.core.util.Validators}
 */
public final class AccumuloValidators {

    private AccumuloValidators() {
        throw new UnsupportedOperationException();
    }

    private static final int MAX_NAME_LENGTH = 1024;

    // Pattern for valid namespace/table name characters
    private static final Pattern VALID_NAME_PATTERN = Pattern.compile("\\w+");

    /**
     * Validator for new table names. Checks that the name is non-null, non-empty, does not start with a dot, is not longer than 1024 characters, and contains
     * only valid characters. For qualified names (namespace.table), both parts must be non-blank and match the valid name pattern.
     */
    public static final Validator NEW_TABLE_NAME = new Validator("new table name", name -> {
        if (name == null) {
            return Optional.of("name is null");
        }
        if (name.isEmpty()) {
            return Optional.of("name is empty");
        }
        if (name.startsWith(".")) {
            return Optional.of("name must not start with a dot: " + name);
        }
        if (name.length() > MAX_NAME_LENGTH) {
            return Optional.of("name exceeds maximum length of " + MAX_NAME_LENGTH + " characters: " + name.length());
        }
        if (name.contains(".")) {
            String namespacePart = name.substring(0, name.lastIndexOf('.'));
            String tablePart = name.substring(name.lastIndexOf('.') + 1);
            if (namespacePart.isBlank()) {
                return Optional.of("namespace part must not be blank in qualified name: " + name);
            }
            if (tablePart.isBlank()) {
                return Optional.of("table part must not be blank in qualified name: " + name);
            }
            if (!VALID_NAME_PATTERN.matcher(namespacePart).matches()) {
                return Optional.of("namespace part contains invalid characters: " + namespacePart);
            }
            if (!VALID_NAME_PATTERN.matcher(tablePart).matches()) {
                return Optional.of("table part contains invalid characters: " + tablePart);
            }
        } else if (!VALID_NAME_PATTERN.matcher(name).matches()) {
            return Optional.of("name contains invalid characters: " + name);
        }
        return Optional.empty();
    });

    /**
     * Validator for new namespace names.
     */
    public static final Validator NEW_NAMESPACE_NAME = new Validator("new namespace name", name -> {
        if (name == null) {
            return Optional.of("name is null");
        }
        if (name.isEmpty()) {
            return Optional.of("name is empty");
        }
        if (name.length() > MAX_NAME_LENGTH) {
            return Optional.of("name exceeds maximum length of " + MAX_NAME_LENGTH + " characters: " + name.length());
        }
        if (!VALID_NAME_PATTERN.matcher(name).matches()) {
            return Optional.of("name contains invalid characters: " + name);
        }
        return Optional.empty();
    });

    /**
     * Validator for existing namespace names (allows empty for default namespace).
     */
    public static final Validator EXISTING_NAMESPACE_NAME = new Validator("existing namespace name", name -> {
        if (name == null) {
            return Optional.of("name is null");
        }
        // Empty string is valid (default namespace)
        if (name.isEmpty()) {
            return Optional.empty();
        }
        if (name.length() > MAX_NAME_LENGTH) {
            return Optional.of("name exceeds maximum length of " + MAX_NAME_LENGTH + " characters: " + name.length());
        }
        if (!VALID_NAME_PATTERN.matcher(name).matches()) {
            return Optional.of("name contains invalid characters: " + name);
        }
        return Optional.empty();
    });
}
