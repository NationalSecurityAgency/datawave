package datawave.annotation.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * A class for defining logic for the object validator pattern, rules are added by passing predicates to {@link #addCheck(Predicate, String)} and object
 * instances are validated using {@link #check(Object)}. Variants of {@code check} are provided that test the checks exhaustively or fail fast as soon as a
 * single validation has failed.
 * <p>
 * Validators should be created once and reused. State is externalized in {@link ValidationState}, so it is safe to use a single instance across multiple
 * threads, but initialization is not thread-safe.
 * </p>
 *
 * @param <T>
 *            the type of object that will be validated.
 */
public class Validator<T> {

    /** the predicates to use for validation */
    final Map<Predicate<T>,String> predicates = new HashMap<>();

    /**
     * create a validator, type is inferred through assignment or cast, e.g.:
     *
     * <pre>{@code
     * Validator<TestObject> validator = Validator.create();
     * }</pre>
     *
     * When chaining, use the method return cast:
     *
     * <pre>{@code
     *      Validator<TestObject> validator = Validator.<TestObject>create().addCheck(o -> o.name != null, "name is not null");
     * }</pre>
     *
     * @return a validator
     * @param <V>
     *            the type of object that will be validated
     */
    public static <V> Validator<V> create() {
        return new Validator<>();
    }

    /** protected constructor use {@link #create()} to create a validator instance. */
    protected Validator() {

    }

    /**
     * add a predicate for validating objects of type T
     *
     * @param predicate
     *            the predicate to execute
     * @param errorMessage
     *            the error message to store if validation fails
     * @return this validator for chaining rule creation.
     */
    public Validator<T> addCheck(Predicate<T> predicate, String errorMessage) {
        predicates.put(predicate, errorMessage);
        return this;
    }

    /**
     * Check all predicates and record all errors
     *
     * @param target
     *            the item to check
     * @return the validation state of the target and all errors that were encountered.
     */
    public ValidationState<T> exhaustiveCheck(T target) {
        return this.check(target, false);
    }

    /**
     * Check the predicates and fail when the first one fails, preserving only the first error.
     *
     * @param target
     *            the item to check
     * @return the validation state of the target includin gthe first error encountered.
     */
    public ValidationState<T> check(T target) {
        return this.check(target, true);
    }

    /**
     * Check the predicates and either fail fast or check each predicate, depending on the argument provided
     *
     * @param target
     *            the target to check
     * @param failFast
     *            wetheer to fail fast.
     * @return the validation state.
     */
    public ValidationState<T> check(T target, boolean failFast) {
        ValidationState<T> state = new ValidationState<>(target);
        for (Map.Entry<Predicate<T>,String> pe : predicates.entrySet()) {
            if (!pe.getKey().test(target)) {
                state.addError(pe.getValue());
                if (failFast) {
                    break;
                }
            }
        }
        return state;
    }

    /**
     * Captures the validation state of a target: a reference ot the target being tested and a list of potential validation errors.
     *
     * @param <T>
     *            the type of object being validated
     */
    public static class ValidationState<T> {
        private final T target;
        private List<String> errors;

        public ValidationState(T target) {
            this.target = target;
        }

        public void addError(String errorMessage) {
            if (errors == null) {
                errors = new ArrayList<>();
            }
            errors.add(errorMessage);
        }

        public boolean isValid() {
            return errors == null || errors.isEmpty();
        }

        public List<String> getErrors() {
            return errors;
        }

        public T getTarget() {
            return target;
        }
    }

    /**
     * Utility method for checking whether that is string is not null and is not empty.
     *
     * @param target
     *            the String to check.
     * @return true if the String is not null or empty.
     */
    public static boolean notNullOrEmpty(String target) {
        return target != null && !target.isEmpty();
    }
}
