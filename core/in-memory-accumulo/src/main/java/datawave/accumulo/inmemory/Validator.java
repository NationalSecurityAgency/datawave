package datawave.accumulo.inmemory;

import java.util.Optional;
import java.util.function.Function;

/**
 * A validator that checks a value and throws IllegalArgumentException with a detailed error description if validation fails. Modeled after Accumulo's internal
 * Validator pattern.
 */
public class Validator {
    private final String description;
    private final Function<String,Optional<String>> validation;

    Validator(String description, Function<String,Optional<String>> validation) {
        this.description = description;
        this.validation = validation;
    }

    /**
     * Validates the given value.
     *
     * @param value
     *            the value to validate
     * @throws IllegalArgumentException
     *             if validation fails, with a detailed error description
     */
    public void validate(String value) {
        Optional<String> error = validation.apply(value);
        if (error.isPresent()) {
            throw new IllegalArgumentException("Invalid " + description + ": " + error.get());
        }
    }
}
