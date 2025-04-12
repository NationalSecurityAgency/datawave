package datawave.annotation.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import datawave.data.hash.UID;

/**
 * A class for defining logic the object validator pattern, rules are added by passing predicates to {@link #addCheck(Predicate, String)} and object instances
 * are validated using {@link #check(Object)}.
 * <p>
 * validators should be created once and reused. state is externalized in {@link ValidationState}, so it is safe to use a single instance across multiple
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
     * <pre>
     * Validator<TestObject> validator = Validator.create();
     * </pre>
     *
     * When chaining, use the method return cast:
     *
     * <pre>
     *      Validator<TestObject> validator = Validator.<TestObject>create().addCheck(o -> o.name != null, "name is not null");
     * </pre>
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

    public ValidationState<T> check(T target) {
        ValidationState<T> state = new ValidationState<>(target);
        for (Map.Entry<Predicate<T>,String> pe : predicates.entrySet()) {
            if (!pe.getKey().test(target)) {
                state.addError(pe.getValue());
            }
        }
        return state;
    }

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

    public static boolean notNullOrEmpty(String target) {
        return target != null && !target.isEmpty();
    }

    public static boolean notNullOrEmpty(UID target) {
        return target != null && !target.toString().isEmpty();
    }
}
