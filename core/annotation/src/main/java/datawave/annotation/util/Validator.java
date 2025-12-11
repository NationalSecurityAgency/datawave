package datawave.annotation.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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

    /** the predicates to use for validation, we want to evaluate these in the order in which they were added */
    final Map<Predicate<T>,String> predicates = new LinkedHashMap<>();

    /** more predicates to validate members/fields of T */
    final List<MemberValidation<T,?>> memberValidators = new ArrayList<>();

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
     * adds a validator for one of T's fields / members. The member field is provided by function that supplies the value.
     *
     * @param memberSupplier
     *            the function to call on T to produce one or more U's to validate.
     * @param validator
     *            a validator for U's
     * @return this validator for chaining rule creation.
     * @param <U>
     *            the type of member object we'll be validating
     */
    public <U> Validator<T> addMemberValidator(Function<T,List<U>> memberSupplier, Validator<U> validator) {
        return addMemberValidator(memberSupplier, u -> validator);
    }

    /**
     * adds a 'dynamic' validator for one of T's fields / members. Instead of a concrete validator, we provie a function that can generate a validator pased on
     * a provided instance of U. This allows us to change validation rules based on properties of U. Practically, this is used to validate U differently based
     * on it's type and allows us to work around the need for polymorphism in protobuf.
     *
     * @param memberSupplier
     *            the function to supply the member to validate from T.
     * @param validatorSupplier
     *            the function to supply the validator based on the characteristics of U.
     * @return this validator for chaining rule creation.
     * @param <U>
     *            the type of member object we'll be validating
     */
    public <U> Validator<T> addMemberValidator(Function<T,List<U>> memberSupplier, Function<U,Validator<U>> validatorSupplier) {
        memberValidators.add(new MemberValidation<T,U>(memberSupplier, validatorSupplier));
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
     *            whether to fail fast.
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
        return checkMembers(target, failFast, state);
    }

    /**
     * Check the members of the target using any validators configured and fail fast or check each predicate, depending on the argument provided.
     *
     * @param target
     *            the target to check
     * @param failFast
     *            whether to fail fast
     * @param state
     *            the validation state that will be updated based on these checks.
     * @return the updated validation state.
     */
    protected ValidationState<T> checkMembers(T target, boolean failFast, final ValidationState<T> state) {
        for (MemberValidation<T,?> mv : memberValidators) {
            mv.check(target, failFast, state);
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
     * Captures a member validator for T that validates type U. A single validator for T can check multiple U types because all of the logic for the specific
     * type is contained here.
     *
     * @param <T>
     *            the parent type we're checking
     * @param <U>
     *            the member's type.
     */
    static class MemberValidation<T,U> {
        final Function<T,List<U>> memberSupplier;
        final Function<U,Validator<U>> validatorSupplier;

        /**
         * @param memberSupplier
         *            a function that provides one or more members from part T as a list of the same time.
         * @param validatorSupplier
         *            a function that provides a validator for the member's type U.
         */
        MemberValidation(Function<T,List<U>> memberSupplier, Function<U,Validator<U>> validatorSupplier) {
            this.memberSupplier = memberSupplier;
            this.validatorSupplier = validatorSupplier;
        }

        /**
         *
         * @param target
         *            the target to check
         * @param failFast
         *            whether we should fail on the first error or continue checking
         * @param state
         *            the target's validation state, errors found in the member validators will be copied here.
         */
        void check(T target, boolean failFast, ValidationState<T> state) {
            List<U> members = memberSupplier.apply(target);
            for (U member : members) {
                final Validator<U> validator = validatorSupplier.apply(member);
                ValidationState<U> memberState = validator.check(member, failFast);
                if (memberState.isValid()) {
                    continue;
                }
                for (String error : memberState.getErrors()) {
                    state.addError(error);
                }
            }
        }
    }
}
